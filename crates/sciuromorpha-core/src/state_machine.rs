use anyhow::Context;
use async_nats::jetstream::consumer::{pull, AckPolicy};
use futures::StreamExt;
use sciuromorpha_common::{
    subject_storage_replicate, DerivativeReadyEvt, ExtractAudioCmd, IngestCompletedEvt,
    ItemMetaPatchEvt, ReplicateBlobCmd, StorageSyncedEvt, STREAM_VAULT_EVT,
    SUBJECT_CMD_MEDIA_EXTRACT_AUDIO, SUBJECT_EVT_INGEST_COMPLETED, SUBJECT_EVT_ITEM_META_PATCH,
    SUBJECT_EVT_MEDIA_DERIVATIVE_READY, SUBJECT_EVT_STORAGE_SYNCED, SUBJECT_EVT_WILDCARD,
};
use sqlx::Row;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::AppState;

const CONSUMER_CORE_STATE_MACHINE: &str = "core-state-machine";
const DEFAULT_REPLICA_BACKEND: &str = "zfs-hot";

pub fn spawn_event_loop(state: AppState) {
    tokio::spawn(async move {
        loop {
            if let Err(err) = run_event_consumer(&state).await {
                error!(error = %err, "VAULT_EVT consumer loop exited with error; retrying in 3s");
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        }
    });
}

async fn run_event_consumer(state: &AppState) -> anyhow::Result<()> {
    let stream = state
        .jetstream
        .get_stream(STREAM_VAULT_EVT)
        .await
        .context("failed to get VAULT_EVT stream")?;

    let consumer = stream
        .get_or_create_consumer(
            CONSUMER_CORE_STATE_MACHINE,
            pull::Config {
                durable_name: Some(CONSUMER_CORE_STATE_MACHINE.to_string()),
                filter_subject: SUBJECT_EVT_WILDCARD.to_string(),
                ack_policy: AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .context("failed to create durable consumer on VAULT_EVT")?;

    info!("State machine subscribed to VAULT_EVT ({SUBJECT_EVT_WILDCARD})");

    let mut messages = consumer
        .messages()
        .await
        .context("failed to open VAULT_EVT message stream")?;

    while let Some(msg_res) = messages.next().await {
        let msg = match msg_res {
            Ok(m) => m,
            Err(err) => {
                warn!(error = %err, "error reading message from VAULT_EVT");
                continue;
            }
        };

        let subject = msg.subject.to_string();
        let handle_res = match subject.as_str() {
            SUBJECT_EVT_INGEST_COMPLETED => {
                let evt: IngestCompletedEvt = serde_json::from_slice(&msg.payload)
                    .context("invalid IngestCompletedEvt JSON")?;
                handle_ingest_completed(state, evt).await
            }
            SUBJECT_EVT_MEDIA_DERIVATIVE_READY => {
                let evt: DerivativeReadyEvt = serde_json::from_slice(&msg.payload)
                    .context("invalid DerivativeReadyEvt JSON")?;
                handle_derivative_ready(state, evt).await
            }
            SUBJECT_EVT_ITEM_META_PATCH => {
                let evt: ItemMetaPatchEvt = serde_json::from_slice(&msg.payload)
                    .context("invalid ItemMetaPatchEvt JSON")?;
                handle_item_meta_patch(state, evt).await
            }
            SUBJECT_EVT_STORAGE_SYNCED => {
                let evt: StorageSyncedEvt = serde_json::from_slice(&msg.payload)
                    .context("invalid StorageSyncedEvt JSON")?;
                handle_storage_synced(state, evt).await
            }
            other => {
                info!(subject = %other, "Ignoring unhandled event subject in MVP state machine");
                Ok(())
            }
        };

        match handle_res {
            Ok(()) => {
                if let Err(err) = msg.ack().await {
                    warn!(error = %err, %subject, "failed to ack VAULT_EVT message");
                }
            }
            Err(err) => {
                error!(error = %err, %subject, "failed to process VAULT_EVT message");
            }
        }
    }

    Ok(())
}

async fn handle_ingest_completed(state: &AppState, evt: IngestCompletedEvt) -> anyhow::Result<()> {
    info!(item_id = %evt.item_id, title = %evt.title, blobs = evt.blobs.len(), "Processing IngestCompletedEvt");

    let next_status = if evt.extract_audio {
        "processing"
    } else {
        "ready"
    };

    let mut tx = state.db.begin().await?;

    sqlx::query(
        r#"
        INSERT INTO items (
            id, item_type, title, description, extracted_text,
            source_type, source_url, source_external_id,
            original_created_at, acl_tags, extra_meta, status
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (id) DO UPDATE SET
            item_type = EXCLUDED.item_type,
            title = EXCLUDED.title,
            description = COALESCE(EXCLUDED.description, items.description),
            extracted_text = COALESCE(EXCLUDED.extracted_text, items.extracted_text),
            source_type = EXCLUDED.source_type,
            source_url = EXCLUDED.source_url,
            source_external_id = COALESCE(EXCLUDED.source_external_id, items.source_external_id),
            original_created_at = COALESCE(EXCLUDED.original_created_at, items.original_created_at),
            extra_meta = items.extra_meta || EXCLUDED.extra_meta,
            status = EXCLUDED.status
        "#,
    )
    .bind(evt.item_id)
    .bind(&evt.item_type)
    .bind(&evt.title)
    .bind(&evt.description)
    .bind(&evt.extracted_text)
    .bind(&evt.source_type)
    .bind(&evt.source_url)
    .bind(&evt.source_external_id)
    .bind(evt.original_created_at)
    .bind(&evt.acl_tags)
    .bind(&evt.extra_meta)
    .bind(next_status)
    .execute(&mut *tx)
    .await?;

    let mut primary_artist: Option<String> = None;

    for author in &evt.authors {
        let display_name = author
            .display_name
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(&author.handle);

        if primary_artist.is_none() {
            primary_artist = Some(display_name.to_string());
        }

        let existing_ident = sqlx::query(
            r#"
            SELECT id FROM author_identities
            WHERE platform = $1 AND platform_uid = $2
            "#,
        )
        .bind(&author.platform)
        .bind(&author.platform_uid)
        .fetch_optional(&mut *tx)
        .await?;

        let identity_id: Uuid = if let Some(row) = existing_ident {
            let id: Uuid = row.try_get("id")?;
            sqlx::query(
                r#"
                UPDATE author_identities
                SET handle = $1, profile_url = COALESCE($2, profile_url)
                WHERE id = $3
                "#,
            )
            .bind(&author.handle)
            .bind(&author.profile_url)
            .bind(id)
            .execute(&mut *tx)
            .await?;
            id
        } else {
            let new_author_id = Uuid::now_v7();
            sqlx::query(
                r#"
                INSERT INTO authors (id, display_name)
                VALUES ($1, $2)
                "#,
            )
            .bind(new_author_id)
            .bind(display_name)
            .execute(&mut *tx)
            .await?;

            let new_ident_id = Uuid::now_v7();
            sqlx::query(
                r#"
                INSERT INTO author_identities (id, author_id, platform, platform_uid, handle, profile_url)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(new_ident_id)
            .bind(new_author_id)
            .bind(&author.platform)
            .bind(&author.platform_uid)
            .bind(&author.handle)
            .bind(&author.profile_url)
            .execute(&mut *tx)
            .await?;
            new_ident_id
        };

        sqlx::query(
            r#"
            INSERT INTO item_authors (item_id, identity_id, role)
            VALUES ($1, $2, $3)
            ON CONFLICT (item_id, identity_id, role) DO NOTHING
            "#,
        )
        .bind(evt.item_id)
        .bind(identity_id)
        .bind(&author.role)
        .execute(&mut *tx)
        .await?;
    }

    for blob in &evt.blobs {
        sqlx::query(
            r#"
            INSERT INTO blobs (sha256, size_bytes, mime_type, staging_pinned)
            VALUES ($1, $2, $3, TRUE)
            ON CONFLICT (sha256) DO UPDATE SET staging_pinned = TRUE
            "#,
        )
        .bind(&blob.sha256)
        .bind(blob.size_bytes)
        .bind(&blob.mime_type)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO item_blobs (id, item_id, blob_sha256, role, display_order, meta)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(Uuid::now_v7())
        .bind(evt.item_id)
        .bind(&blob.sha256)
        .bind(&blob.role)
        .bind(blob.display_order)
        .bind(&blob.meta)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO blob_replicas (blob_sha256, backend_id, locator, status)
            VALUES ($1, $2, '', 'pending')
            ON CONFLICT (blob_sha256, backend_id) DO NOTHING
            "#,
        )
        .bind(&blob.sha256)
        .bind(DEFAULT_REPLICA_BACKEND)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    // Dispatch storage replication commands for all ingested blobs
    let replicate_subj = subject_storage_replicate(DEFAULT_REPLICA_BACKEND);
    for blob in &evt.blobs {
        let cmd = ReplicateBlobCmd {
            blob_sha256: blob.sha256.clone(),
            backend_id: DEFAULT_REPLICA_BACKEND.to_string(),
            mime_type: Some(blob.mime_type.clone()),
        };
        let payload = serde_json::to_vec(&cmd)?;
        state
            .jetstream
            .publish(replicate_subj.clone(), payload.into())
            .await?
            .await?;
    }

    // Dispatch MP3 extraction if requested on ingest
    if evt.extract_audio {
        if let Some(video_blob) = evt.blobs.iter().find(|b| b.role == "primary_video") {
            let audio_cmd = ExtractAudioCmd {
                item_id: evt.item_id,
                source_blob_sha256: video_blob.sha256.clone(),
                bitrate_kbps: 192,
                id3_title: Some(evt.title.clone()),
                id3_artist: primary_artist,
            };
            let payload = serde_json::to_vec(&audio_cmd)?;
            state
                .jetstream
                .publish(SUBJECT_CMD_MEDIA_EXTRACT_AUDIO, payload.into())
                .await?
                .await?;
            info!(item_id = %evt.item_id, "Dispatched ExtractAudioCmd to VAULT_CMD");
        }
    }

    if let Err(err) = state.search.sync_item_by_id(&state.db, evt.item_id).await {
        warn!(error = %err, item_id = %evt.item_id, "Meilisearch sync failed after ingest");
    }

    Ok(())
}

async fn handle_derivative_ready(
    state: &AppState,
    evt: DerivativeReadyEvt,
) -> anyhow::Result<()> {
    info!(item_id = %evt.item_id, role = %evt.role, sha256 = %evt.blob_sha256, "Processing DerivativeReadyEvt");

    let mut tx = state.db.begin().await?;

    sqlx::query(
        r#"
        INSERT INTO blobs (sha256, size_bytes, mime_type, staging_pinned)
        VALUES ($1, $2, $3, TRUE)
        ON CONFLICT (sha256) DO UPDATE SET staging_pinned = TRUE
        "#,
    )
    .bind(&evt.blob_sha256)
    .bind(evt.size_bytes)
    .bind(&evt.mime_type)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO item_blobs (id, item_id, blob_sha256, role, derived_from_sha256, meta)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(evt.item_id)
    .bind(&evt.blob_sha256)
    .bind(&evt.role)
    .bind(&evt.derived_from_sha256)
    .bind(&evt.meta)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO blob_replicas (blob_sha256, backend_id, locator, status)
        VALUES ($1, $2, '', 'pending')
        ON CONFLICT (blob_sha256, backend_id) DO NOTHING
        "#,
    )
    .bind(&evt.blob_sha256)
    .bind(DEFAULT_REPLICA_BACKEND)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE items
        SET status = 'ready',
            extracted_text = COALESCE($1, extracted_text)
        WHERE id = $2
        "#,
    )
    .bind(&evt.extracted_text)
    .bind(evt.item_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    let replicate_cmd = ReplicateBlobCmd {
        blob_sha256: evt.blob_sha256,
        backend_id: DEFAULT_REPLICA_BACKEND.to_string(),
        mime_type: Some(evt.mime_type),
    };
    let payload = serde_json::to_vec(&replicate_cmd)?;
    state
        .jetstream
        .publish(
            subject_storage_replicate(DEFAULT_REPLICA_BACKEND),
            payload.into(),
        )
        .await?
        .await?;

    if let Err(err) = state.search.sync_item_by_id(&state.db, evt.item_id).await {
        warn!(error = %err, item_id = %evt.item_id, "Meilisearch sync failed after derivative_ready");
    }

    Ok(())
}

/// Section 4.3 Meta Merge: incremental JSONB merge (`extra_meta = extra_meta || $1`)
/// followed by Meilisearch re-indexing.
async fn handle_item_meta_patch(state: &AppState, evt: ItemMetaPatchEvt) -> anyhow::Result<()> {
    info!(
        item_id = %evt.item_id,
        source_agent = ?evt.source_agent,
        "Applying JSONB Meta Merge patch to items.extra_meta"
    );

    sqlx::query(
        r#"
        UPDATE items
        SET extra_meta = extra_meta || $1::jsonb,
            extracted_text = COALESCE($2, extracted_text)
        WHERE id = $3
        "#,
    )
    .bind(&evt.patch)
    .bind(&evt.extracted_text)
    .bind(evt.item_id)
    .execute(&state.db)
    .await?;

    if let Err(err) = state.search.sync_item_by_id(&state.db, evt.item_id).await {
        warn!(error = %err, item_id = %evt.item_id, "Meilisearch sync failed after meta_patch");
    }

    Ok(())
}

async fn handle_storage_synced(state: &AppState, evt: StorageSyncedEvt) -> anyhow::Result<()> {
    info!(
        sha256 = %evt.blob_sha256,
        backend = %evt.backend_id,
        locator = %evt.locator,
        "Recording synced storage replica"
    );

    sqlx::query(
        r#"
        INSERT INTO blob_replicas (blob_sha256, backend_id, locator, status, is_encrypted, last_verified_at, updated_at)
        VALUES ($1, $2, $3, 'synced', $4, $5, NOW())
        ON CONFLICT (blob_sha256, backend_id) DO UPDATE SET
            locator = EXCLUDED.locator,
            status = 'synced',
            is_encrypted = EXCLUDED.is_encrypted,
            last_verified_at = EXCLUDED.last_verified_at,
            updated_at = NOW()
        "#,
    )
    .bind(&evt.blob_sha256)
    .bind(&evt.backend_id)
    .bind(&evt.locator)
    .bind(evt.is_encrypted)
    .bind(evt.verified_at)
    .execute(&state.db)
    .await?;

    // Unpin from staging once at least one online durable replica is synced and no linked item is still processing
    sqlx::query(
        r#"
        UPDATE blobs b
        SET staging_pinned = FALSE
        WHERE b.sha256 = $1
          AND EXISTS (
              SELECT 1
              FROM blob_replicas br
              JOIN storage_backends sb ON sb.id = br.backend_id
              WHERE br.blob_sha256 = b.sha256
                AND br.status = 'synced'
                AND sb.is_online = TRUE
          )
          AND NOT EXISTS (
              SELECT 1
              FROM item_blobs ib
              JOIN items i ON i.id = ib.item_id
              WHERE ib.blob_sha256 = b.sha256
                AND i.status IN ('ingesting', 'processing')
          )
        "#,
    )
    .bind(&evt.blob_sha256)
    .execute(&state.db)
    .await?;

    Ok(())
}
