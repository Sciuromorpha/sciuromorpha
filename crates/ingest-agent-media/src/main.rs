use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context};
use async_nats::jetstream::consumer::{pull, AckPolicy};
use async_nats::jetstream::{self, stream, AckKind};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use futures::StreamExt;
use sciuromorpha_common::{
    IngestCompletedEvt, IngestUrlCmd, IngestedAuthor, IngestedBlob, LocalFsStagingStore,
    StagingStore, STREAM_VAULT_CMD, STREAM_VAULT_EVT, SUBJECT_CMD_INGEST_URL,
    SUBJECT_CMD_WILDCARD, SUBJECT_EVT_INGEST_COMPLETED, SUBJECT_EVT_WILDCARD,
};
use serde::Deserialize;
use tokio::process::Command;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const CONSUMER_NAME: &str = "worker-ingest-agent-media";

#[derive(Debug, Deserialize, Default)]
struct YtDlpInfoJson {
    id: Option<String>,
    title: Option<String>,
    description: Option<String>,
    uploader: Option<String>,
    uploader_id: Option<String>,
    channel: Option<String>,
    channel_id: Option<String>,
    uploader_url: Option<String>,
    channel_url: Option<String>,
    extractor_key: Option<String>,
    extractor: Option<String>,
    timestamp: Option<i64>,
    upload_date: Option<String>,
    duration: Option<f64>,
    #[serde(default)]
    tags: Vec<String>,
    webpage_url: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,ingest_agent_media=debug")),
        )
        .init();

    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let staging_dir =
        PathBuf::from(env::var("STAGING_DIR").unwrap_or_else(|_| "./data/staging".to_string()));
    let yt_dlp_bin = env::var("YT_DLP_BIN").unwrap_or_else(|_| "yt-dlp".to_string());

    tokio::fs::create_dir_all(&staging_dir)
        .await
        .with_context(|| format!("failed to create STAGING_DIR {:?}", staging_dir))?;
    let staging = LocalFsStagingStore::new(staging_dir);

    info!(%nats_url, "Connecting ingest-agent-media to NATS...");
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
                filter_subject: SUBJECT_CMD_INGEST_URL.to_string(),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .await
        .context("failed to create pull consumer for ingest-agent-media")?;

    info!("ingest-agent-media ready; pulling from {SUBJECT_CMD_INGEST_URL}");

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

            let cmd: IngestUrlCmd = match serde_json::from_slice(&msg.payload) {
                Ok(c) => c,
                Err(err) => {
                    error!(error = %err, "malformed IngestUrlCmd JSON; terminating message");
                    let _ = msg.ack_with(AckKind::Term).await;
                    continue;
                }
            };

            let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
            // Consume immediate first tick
            heartbeat.tick().await;

            let work_fut = process_ingest_command(&jetstream, &staging, &yt_dlp_bin, &cmd);
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
                        warn!(error = %err, item_id = %cmd.item_id, "failed to ack completed ingest message");
                    }
                }
                Err(err) => {
                    error!(error = %err, item_id = %cmd.item_id, url = %cmd.url, "yt-dlp ingest failed");
                    let _ = msg.ack_with(AckKind::Nak(Some(Duration::from_secs(15)))).await;
                }
            }
        }
    }
}

async fn process_ingest_command(
    jetstream: &jetstream::Context,
    staging: &LocalFsStagingStore,
    yt_dlp_bin: &str,
    cmd: &IngestUrlCmd,
) -> anyhow::Result<()> {
    info!(item_id = %cmd.item_id, url = %cmd.url, "Starting yt-dlp capture");

    let work_dir = tempfile::tempdir().context("failed to create temporary yt-dlp workspace")?;
    let output_template = work_dir.path().join("capture.%(ext)s");
    let output_template_str = output_template
        .to_str()
        .context("non-UTF8 temporary directory path")?;

    let output = Command::new(yt_dlp_bin)
        .arg("--no-playlist")
        .arg("--write-info-json")
        .arg("-o")
        .arg(output_template_str)
        .arg("--")
        .arg(&cmd.url)
        .output()
        .await
        .with_context(|| format!("failed to spawn `{yt_dlp_bin}`"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("yt-dlp exited with {}: {}", output.status, stderr.trim());
    }

    let (media_path, info_json_path) = locate_captured_files(work_dir.path()).await?;

    let info: YtDlpInfoJson = if let Some(ref json_path) = info_json_path {
        let bytes = tokio::fs::read(json_path)
            .await
            .context("failed reading yt-dlp .info.json")?;
        serde_json::from_slice(&bytes).context("failed parsing yt-dlp .info.json")?
    } else {
        YtDlpInfoJson::default()
    };

    let staged_media = staging
        .put_file(&media_path)
        .await
        .context("failed staging captured media file")?;

    let mime_type = mime_guess::from_path(&media_path)
        .first_or_octet_stream()
        .essence_str()
        .to_string();

    let is_audio_only_source = mime_type.starts_with("audio/");
    let primary_role = if is_audio_only_source {
        "derived_audio"
    } else if mime_type.starts_with("image/") {
        "primary_image"
    } else {
        "primary_video"
    };

    let item_type = if is_audio_only_source {
        "audio"
    } else if mime_type.starts_with("image/") {
        "photo"
    } else {
        "video"
    };

    let mut blobs = vec![IngestedBlob {
        sha256: staged_media.sha256,
        size_bytes: i64::try_from(staged_media.size_bytes).unwrap_or(i64::MAX),
        mime_type,
        role: primary_role.to_string(),
        display_order: 0,
        meta: serde_json::json!({
            "duration_sec": info.duration,
        }),
    }];

    if let Some(ref json_path) = info_json_path {
        let staged_meta = staging
            .put_file(json_path)
            .await
            .context("failed staging raw .info.json")?;
        blobs.push(IngestedBlob {
            sha256: staged_meta.sha256,
            size_bytes: i64::try_from(staged_meta.size_bytes).unwrap_or(i64::MAX),
            mime_type: "application/json".to_string(),
            role: "raw_meta".to_string(),
            display_order: 99,
            meta: serde_json::json!({"generator": "yt-dlp"}),
        });
    }

    let source_type = normalize_platform(
        info.extractor_key
            .as_deref()
            .or(info.extractor.as_deref()),
        &cmd.url,
    );

    let mut authors = Vec::new();
    let handle = info
        .uploader
        .clone()
        .or_else(|| info.channel.clone());
    let platform_uid = info
        .uploader_id
        .clone()
        .or_else(|| info.channel_id.clone())
        .or_else(|| handle.clone());

    if let (Some(uid), Some(h)) = (platform_uid, handle) {
        authors.push(IngestedAuthor {
            platform: source_type.clone(),
            platform_uid: uid,
            display_name: Some(h.clone()),
            handle: h,
            profile_url: info.uploader_url.clone().or(info.channel_url.clone()),
            role: "creator".to_string(),
        });
    }

    let original_created_at = parse_original_timestamp(info.timestamp, info.upload_date.as_deref());
    let title = info
        .title
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| cmd.url.clone());

    let evt = IngestCompletedEvt {
        item_id: cmd.item_id,
        item_type: item_type.to_string(),
        title,
        description: info.description,
        extracted_text: None,
        source_type,
        source_url: info.webpage_url.unwrap_or_else(|| cmd.url.clone()),
        source_external_id: info.id,
        original_created_at,
        acl_tags: cmd.acl_tags.clone(),
        extra_meta: serde_json::json!({
            "tags": info.tags,
            "duration_sec": info.duration,
            "extractor": info.extractor_key,
        }),
        authors,
        blobs,
        extract_audio: cmd.extract_audio && !is_audio_only_source,
    };

    let payload = serde_json::to_vec(&evt).context("failed to serialize IngestCompletedEvt")?;
    jetstream
        .publish(SUBJECT_EVT_INGEST_COMPLETED, payload.into())
        .await
        .context("failed publishing IngestCompletedEvt")?
        .await
        .context("failed awaiting ack for IngestCompletedEvt")?;

    info!(item_id = %cmd.item_id, "Published IngestCompletedEvt to {SUBJECT_EVT_INGEST_COMPLETED}");
    Ok(())
}

async fn locate_captured_files(dir: &Path) -> anyhow::Result<(PathBuf, Option<PathBuf>)> {
    let mut entries = tokio::fs::read_dir(dir).await?;
    let mut media_candidate: Option<PathBuf> = None;
    let mut info_json: Option<PathBuf> = None;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if name.ends_with(".info.json") {
            info_json = Some(path);
        } else if !name.ends_with(".part") && !name.ends_with(".ytdl") {
            media_candidate = Some(path);
        }
    }

    let media = media_candidate.context("yt-dlp finished without producing a media file")?;
    Ok((media, info_json))
}

fn normalize_platform(extractor: Option<&str>, url: &str) -> String {
    let raw = extractor.unwrap_or_default().to_ascii_lowercase();
    if raw.contains("bilibili") || url.contains("bilibili.com") || url.contains("b23.tv") {
        "bilibili".to_string()
    } else if raw.contains("youtube") || url.contains("youtube.com") || url.contains("youtu.be") {
        "youtube".to_string()
    } else if raw.contains("twitter") || url.contains("twitter.com") || url.contains("x.com") {
        "twitter".to_string()
    } else if raw.contains("pixiv") || url.contains("pixiv.net") {
        "pixiv".to_string()
    } else if !raw.is_empty() {
        raw
    } else {
        "manual".to_string()
    }
}

fn parse_original_timestamp(ts: Option<i64>, upload_date: Option<&str>) -> Option<DateTime<Utc>> {
    if let Some(secs) = ts {
        if let Some(dt) = Utc.timestamp_opt(secs, 0).single() {
            return Some(dt);
        }
    }
    if let Some(ymd) = upload_date {
        if let Ok(date) = NaiveDate::parse_from_str(ymd, "%Y%m%d") {
            if let Some(naive_dt) = date.and_hms_opt(0, 0, 0) {
                return Some(Utc.from_utc_datetime(&naive_dt));
            }
        }
    }
    None
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
