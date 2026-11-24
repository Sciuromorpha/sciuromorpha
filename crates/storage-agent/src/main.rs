use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context};
use async_nats::jetstream::consumer::{pull, AckPolicy};
use async_nats::jetstream::{self, stream, AckKind};
use chrono::Utc;
use futures::StreamExt;
use sciuromorpha_common::{
    sharded_relative_path, validate_sha256, LocalFsStagingStore, ReplicateBlobCmd, StagingStore,
    StorageSyncedEvt, STREAM_VAULT_CMD, STREAM_VAULT_EVT, SUBJECT_CMD_STORAGE_ZFS_HOT_REPLICATE,
    SUBJECT_CMD_WILDCARD, SUBJECT_EVT_STORAGE_SYNCED, SUBJECT_EVT_WILDCARD,
};
use sha2::{Digest, Sha256};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const CONSUMER_NAME: &str = "worker-storage-agent-zfs-hot";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,storage_agent=debug")),
        )
        .init();

    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let staging_dir =
        PathBuf::from(env::var("STAGING_DIR").unwrap_or_else(|_| "./data/staging".to_string()));
    let zfs_archive_dir = PathBuf::from(
        env::var("ZFS_ARCHIVE_DIR").unwrap_or_else(|_| "./data/zfs-hot".to_string()),
    );

    fs::create_dir_all(&staging_dir)
        .await
        .with_context(|| format!("failed to create STAGING_DIR {:?}", staging_dir))?;
    fs::create_dir_all(&zfs_archive_dir)
        .await
        .with_context(|| format!("failed to create ZFS_ARCHIVE_DIR {:?}", zfs_archive_dir))?;

    let staging = LocalFsStagingStore::new(staging_dir);

    info!(%nats_url, ?zfs_archive_dir, "Connecting storage-agent (zfs-hot) to NATS...");
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
                filter_subject: SUBJECT_CMD_STORAGE_ZFS_HOT_REPLICATE.to_string(),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .await
        .context("failed to create pull consumer for storage-agent")?;

    info!("storage-agent ready; pulling from {SUBJECT_CMD_STORAGE_ZFS_HOT_REPLICATE}");

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

            let cmd: ReplicateBlobCmd = match serde_json::from_slice(&msg.payload) {
                Ok(c) => c,
                Err(err) => {
                    error!(error = %err, "malformed ReplicateBlobCmd JSON; terminating message");
                    let _ = msg.ack_with(AckKind::Term).await;
                    continue;
                }
            };

            let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
            heartbeat.tick().await;

            let work_fut = replicate_to_zfs(&jetstream, &staging, &zfs_archive_dir, &cmd);
            tokio::pin!(work_fut);

            let outcome = loop {
                tokio::select! {
                    res = &mut work_fut => break res,
                    _ = heartbeat.tick() => {
                        if let Err(err) = msg.ack_with(AckKind::Progress).await {
                            warn!(error = %err, sha256 = %cmd.blob_sha256, "failed sending InProgress heartbeat");
                        }
                    }
                }
            };

            match outcome {
                Ok(()) => {
                    if let Err(err) = msg.ack().await {
                        warn!(error = %err, sha256 = %cmd.blob_sha256, "failed to ack ReplicateBlobCmd");
                    }
                }
                Err(err) => {
                    error!(error = %err, sha256 = %cmd.blob_sha256, "ZFS archive replication failed");
                    let _ = msg.ack_with(AckKind::Nak(Some(Duration::from_secs(15)))).await;
                }
            }
        }
    }
}

async fn replicate_to_zfs(
    jetstream: &jetstream::Context,
    staging: &LocalFsStagingStore,
    zfs_archive_dir: &Path,
    cmd: &ReplicateBlobCmd,
) -> anyhow::Result<()> {
    let expected_sha = validate_sha256(&cmd.blob_sha256)?;
    let rel_path = sharded_relative_path(&expected_sha)?;
    let dest_path = zfs_archive_dir.join(&rel_path);

    let locator = format!(
        "{}/{}/{}",
        &expected_sha[0..2],
        &expected_sha[2..4],
        expected_sha
    );

    if !fs::try_exists(&dest_path).await? {
        let mut reader = staging
            .open_reader(&expected_sha)
            .await
            .with_context(|| format!("failed opening staged blob {expected_sha}"))?;

        let tmp_dir = zfs_archive_dir.join(".tmp");
        fs::create_dir_all(&tmp_dir).await?;
        let tmp_path = tmp_dir.join(format!("rep-{}.part", Uuid::now_v7()));

        let mut tmp_file = File::create(&tmp_path).await?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 64 * 1024];

        let copy_res: anyhow::Result<String> = async {
            loop {
                let n = reader.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                tmp_file.write_all(&buf[..n]).await?;
            }
            tmp_file.flush().await?;
            tmp_file.sync_all().await?;
            Ok(hex::encode(hasher.finalize()))
        }
        .await;

        drop(tmp_file);

        let actual_sha = match copy_res {
            Ok(s) => s,
            Err(err) => {
                let _ = fs::remove_file(&tmp_path).await;
                return Err(err);
            }
        };

        if actual_sha != expected_sha {
            let _ = fs::remove_file(&tmp_path).await;
            bail!(
                "SHA-256 integrity mismatch during replication: expected {expected_sha}, got {actual_sha}"
            );
        }

        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        if fs::try_exists(&dest_path).await? {
            let _ = fs::remove_file(&tmp_path).await;
        } else if let Err(err) = fs::rename(&tmp_path, &dest_path).await {
            if fs::try_exists(&dest_path).await.unwrap_or(false) {
                let _ = fs::remove_file(&tmp_path).await;
            } else {
                let _ = fs::remove_file(&tmp_path).await;
                return Err(err.into());
            }
        }
    }

    let evt = StorageSyncedEvt {
        blob_sha256: expected_sha.clone(),
        backend_id: cmd.backend_id.clone(),
        locator: locator.clone(),
        is_encrypted: false,
        verified_at: Utc::now(),
    };

    let payload = serde_json::to_vec(&evt).context("failed serializing StorageSyncedEvt")?;
    jetstream
        .publish(SUBJECT_EVT_STORAGE_SYNCED, payload.into())
        .await
        .context("failed publishing StorageSyncedEvt")?
        .await
        .context("failed awaiting ack for StorageSyncedEvt")?;

    info!(
        sha256 = %expected_sha,
        backend = %cmd.backend_id,
        %locator,
        "Archived blob to ZFS and published StorageSyncedEvt"
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
