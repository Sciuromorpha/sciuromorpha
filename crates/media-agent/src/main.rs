use std::env;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context};
use async_nats::jetstream::consumer::{pull, AckPolicy};
use async_nats::jetstream::{self, stream, AckKind};
use futures::StreamExt;
use sciuromorpha_common::{
    DerivativeReadyEvt, ExtractAudioCmd, LocalFsStagingStore, StagingStore, STREAM_VAULT_CMD,
    STREAM_VAULT_EVT, SUBJECT_CMD_MEDIA_EXTRACT_AUDIO, SUBJECT_CMD_WILDCARD,
    SUBJECT_EVT_MEDIA_DERIVATIVE_READY, SUBJECT_EVT_WILDCARD,
};
use tokio::process::Command;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const CONSUMER_NAME: &str = "worker-media-agent-audio";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,media_agent=debug")),
        )
        .init();

    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let staging_dir =
        PathBuf::from(env::var("STAGING_DIR").unwrap_or_else(|_| "./data/staging".to_string()));
    let ffmpeg_bin = env::var("FFMPEG_BIN").unwrap_or_else(|_| "ffmpeg".to_string());

    tokio::fs::create_dir_all(&staging_dir)
        .await
        .with_context(|| format!("failed to create STAGING_DIR {:?}", staging_dir))?;
    let staging = LocalFsStagingStore::new(staging_dir);

    info!(%nats_url, "Connecting media-agent to NATS...");
    let nats = async_nats::connect(&nats_url)
        .await
        .with_context(|| format!("failed to connect to NATS at {nats_url}"))?;
    let jetstream = jetstream::new(nats);

    ensure_streams(&jetstream).await?;

    let cmd_stream = jetstream
        .get_stream(STREAM_VAULT_CMD)
        .await
        .context("failed to get VAULT_CMD stream")?;

    let consumer = cmd_stream
        .get_or_create_consumer(
            CONSUMER_NAME,
            pull::Config {
                durable_name: Some(CONSUMER_NAME.to_string()),
                filter_subject: SUBJECT_CMD_MEDIA_EXTRACT_AUDIO.to_string(),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .await
        .context("failed to create pull consumer for media-agent")?;

    info!("media-agent ready; pulling from {SUBJECT_CMD_MEDIA_EXTRACT_AUDIO}");

    loop {
        let mut batch = match consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_secs(15))
            .messages()
            .await
        {
            Ok(b) => b,
            Err(err) => {
                warn!(error = %err, "pull fetch error; retrying in 2s");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        while let Some(msg_res) = batch.next().await {
            let msg = match msg_res {
                Ok(m) => m,
                Err(err) => {
                    warn!(error = %err, "failed reading pulled message");
                    continue;
                }
            };

            let cmd: ExtractAudioCmd = match serde_json::from_slice(&msg.payload) {
                Ok(c) => c,
                Err(err) => {
                    error!(error = %err, "malformed ExtractAudioCmd JSON; terminating message");
                    let _ = msg.ack_with(AckKind::Term).await;
                    continue;
                }
            };

            let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
            heartbeat.tick().await;

            let work_fut = process_extract_audio(&jetstream, &staging, &ffmpeg_bin, &cmd);
            tokio::pin!(work_fut);

            let outcome = loop {
                tokio::select! {
                    res = &mut work_fut => break res,
                    _ = heartbeat.tick() => {
                        if let Err(err) = msg.ack_with(AckKind::Progress).await {
                            warn!(error = %err, item_id = %cmd.item_id, "failed sending InProgress heartbeat");
                        }
                    }
                }
            };

            match outcome {
                Ok(()) => {
                    if let Err(err) = msg.ack().await {
                        warn!(error = %err, item_id = %cmd.item_id, "failed to ack ExtractAudioCmd");
                    }
                }
                Err(err) => {
                    error!(error = %err, item_id = %cmd.item_id, sha256 = %cmd.source_blob_sha256, "ffmpeg audio extraction failed");
                    let _ = msg.ack_with(AckKind::Nak(Some(Duration::from_secs(15)))).await;
                }
            }
        }
    }
}

async fn process_extract_audio(
    jetstream: &jetstream::Context,
    staging: &LocalFsStagingStore,
    ffmpeg_bin: &str,
    cmd: &ExtractAudioCmd,
) -> anyhow::Result<()> {
    let source_path = staging
        .local_path(&cmd.source_blob_sha256)
        .context("invalid source_blob_sha256")?
        .context("LocalFsStagingStore must provide local file path")?;

    if !tokio::fs::try_exists(&source_path).await? {
        bail!(
            "source video blob {} does not exist in staging store",
            cmd.source_blob_sha256
        );
    }

    let work_dir = tempfile::tempdir().context("failed to create temporary ffmpeg workspace")?;
    let out_mp3 = work_dir.path().join("derived.mp3");

    let bitrate_arg = format!("{}k", cmd.bitrate_kbps.clamp(64, 320));

    let mut command = Command::new(ffmpeg_bin);
    command
        .arg("-y")
        .arg("-nostdin")
        .arg("-i")
        .arg(&source_path)
        .arg("-vn")
        .arg("-acodec")
        .arg("libmp3lame")
        .arg("-b:a")
        .arg(&bitrate_arg)
        .arg("-id3v2_version")
        .arg("3");

    if let Some(ref title) = cmd.id3_title {
        command.arg("-metadata").arg(format!("title={title}"));
    }
    if let Some(ref artist) = cmd.id3_artist {
        command.arg("-metadata").arg(format!("artist={artist}"));
    }

    command.arg(&out_mp3);

    info!(
        item_id = %cmd.item_id,
        source_sha256 = %cmd.source_blob_sha256,
        bitrate = %bitrate_arg,
        "Running ffmpeg MP3 extraction"
    );

    let output = command
        .output()
        .await
        .with_context(|| format!("failed to spawn `{ffmpeg_bin}`"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("ffmpeg exited with {}: {}", output.status, stderr.trim());
    }

    let staged = staging
        .put_file(&out_mp3)
        .await
        .context("failed writing derived MP3 into staging store")?;

    let evt = DerivativeReadyEvt {
        item_id: cmd.item_id,
        derived_from_sha256: cmd.source_blob_sha256.clone(),
        blob_sha256: staged.sha256.clone(),
        size_bytes: i64::try_from(staged.size_bytes).unwrap_or(i64::MAX),
        mime_type: "audio/mpeg".to_string(),
        role: "derived_audio".to_string(),
        extracted_text: None,
        meta: serde_json::json!({
            "codec": "mp3",
            "bitrate_kbps": cmd.bitrate_kbps,
            "id3_title": cmd.id3_title,
            "id3_artist": cmd.id3_artist,
        }),
    };

    let payload = serde_json::to_vec(&evt).context("failed serializing DerivativeReadyEvt")?;
    jetstream
        .publish(SUBJECT_EVT_MEDIA_DERIVATIVE_READY, payload.into())
        .await
        .context("failed publishing DerivativeReadyEvt")?
        .await
        .context("failed awaiting ack for DerivativeReadyEvt")?;

    info!(
        item_id = %cmd.item_id,
        derived_sha256 = %staged.sha256,
        "Published DerivativeReadyEvt to {SUBJECT_EVT_MEDIA_DERIVATIVE_READY}"
    );
    Ok(())
}

async fn ensure_streams(jetstream: &jetstream::Context) -> anyhow::Result<()> {
    jetstream
        .get_or_create_stream(stream::Config {
            name: STREAM_VAULT_CMD.to_string(),
            subjects: vec![SUBJECT_CMD_WILDCARD.to_string()],
            retention: stream::RetentionPolicy::WorkQueue,
            storage: stream::StorageType::File,
            ..Default::default()
        })
        .await?;

    jetstream
        .get_or_create_stream(stream::Config {
            name: STREAM_VAULT_EVT.to_string(),
            subjects: vec![SUBJECT_EVT_WILDCARD.to_string()],
            retention: stream::RetentionPolicy::Limits,
            storage: stream::StorageType::File,
            ..Default::default()
        })
        .await?;

    Ok(())
}
