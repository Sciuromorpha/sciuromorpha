use anyhow::Context;
use axum::extract::State;
use axum::Json;
use sciuromorpha_common::StagingStore;
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, QueryBuilder, Row};
use tracing::info;
use uuid::Uuid;

use crate::auth::Actor;
use crate::routes::AppError;
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct BackfillRequest {
    pub source_type: Option<String>,
    pub item_type: Option<String>,
    /// Role of the existing blob to pass as `source_blob_sha256` (defaults to `"primary_video"`).
    pub source_blob_role: Option<String>,
    /// Only select items that do NOT yet have a blob with this role (e.g. `"derived_audio"`).
    pub missing_role: Option<String>,
    /// Only select items whose `extra_meta` JSONB does NOT contain this top-level key (e.g. `"phash"`).
    pub missing_extra_meta_key: Option<String>,
    /// Target `VAULT_CMD` subject, e.g. `"vault.cmd.media.extract_audio"`.
    pub target_subject: String,
    /// Arbitrary JSON template merged into each dispatched command alongside `item_id` and `source_blob_sha256`.
    #[serde(default = "default_empty_obj")]
    pub params_template: serde_json::Value,
    pub limit: Option<i64>,
}

fn default_empty_obj() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

#[derive(Debug, Serialize)]
pub struct BackfillResponse {
    pub matched_count: usize,
    pub dispatched_count: usize,
    pub target_subject: String,
    pub item_ids: Vec<Uuid>,
}

pub async fn backfill_handler(
    State(state): State<AppState>,
    actor: Actor,
    Json(req): Json<BackfillRequest>,
) -> Result<Json<BackfillResponse>, AppError> {
    actor.require_owner()?;

    if !req.target_subject.starts_with("vault.cmd.") {
        return Err(AppError::BadRequest(
            "target_subject must belong to vault.cmd.> namespace".to_string(),
        ));
    }

    let source_role = req
        .source_blob_role
        .as_deref()
        .unwrap_or("primary_video");
    let limit = req.limit.unwrap_or(100).clamp(1, 1000);

    let mut qb: QueryBuilder<'_, Postgres> = QueryBuilder::new(
        r#"
        SELECT DISTINCT ON (i.id)
            i.id AS item_id,
            i.title,
            ib.blob_sha256
        FROM items i
        JOIN item_blobs ib ON ib.item_id = i.id
        WHERE ib.role = 
        "#,
    );
    qb.push_bind(source_role);

    if let Some(ref source_type) = req.source_type {
        qb.push(" AND i.source_type = ").push_bind(source_type);
    }
    if let Some(ref item_type) = req.item_type {
        qb.push(" AND i.item_type = ").push_bind(item_type);
    }
    if let Some(ref missing_role) = req.missing_role {
        qb.push(
            " AND NOT EXISTS (SELECT 1 FROM item_blobs m WHERE m.item_id = i.id AND m.role = ",
        )
        .push_bind(missing_role)
        .push(")");
    }
    if let Some(ref missing_key) = req.missing_extra_meta_key {
        qb.push(" AND NOT (i.extra_meta ? ")
            .push_bind(missing_key)
            .push(")");
    }

    qb.push(" ORDER BY i.id, ib.display_order ASC LIMIT ")
        .push_bind(limit);

    let rows = qb.build().fetch_all(&state.db).await?;
    let matched_count = rows.len();

    let mut dispatched_ids = Vec::with_capacity(matched_count);

    for row in rows {
        let item_id: Uuid = row.try_get("item_id")?;
        let title: String = row.try_get("title")?;
        let blob_sha256: String = row.try_get("blob_sha256")?;

        // Ensure hot cache has the blob if a local_fs ZFS replica is available on disk
        ensure_staged_if_local(&state, &blob_sha256).await?;

        let mut cmd_obj = match req.params_template.clone() {
            serde_json::Value::Object(map) => map,
            _ => serde_json::Map::new(),
        };
        cmd_obj.insert("item_id".to_string(), serde_json::json!(item_id));
        cmd_obj.insert(
            "source_blob_sha256".to_string(),
            serde_json::json!(blob_sha256),
        );
        cmd_obj
            .entry("id3_title".to_string())
            .or_insert_with(|| serde_json::json!(title));

        let payload = serde_json::to_vec(&serde_json::Value::Object(cmd_obj))
            .context("failed to serialize backfill command payload")?;

        state
            .jetstream
            .publish(req.target_subject.clone(), payload.into())
            .await
            .context("failed publishing backfill task to NATS")?
            .await
            .context("failed awaiting JetStream ack for backfill task")?;

        dispatched_ids.push(item_id);
    }

    info!(
        matched_count,
        dispatched_count = dispatched_ids.len(),
        target_subject = %req.target_subject,
        "Completed admin backfill batch dispatch"
    );

    Ok(Json(BackfillResponse {
        matched_count,
        dispatched_count: dispatched_ids.len(),
        target_subject: req.target_subject,
        item_ids: dispatched_ids,
    }))
}

async fn ensure_staged_if_local(state: &AppState, sha256: &str) -> anyhow::Result<()> {
    if state.staging.exists(sha256).await.unwrap_or(false) {
        return Ok(());
    }

    let row = sqlx::query(
        r#"
        SELECT br.locator
        FROM blob_replicas br
        JOIN storage_backends sb ON sb.id = br.backend_id
        WHERE br.blob_sha256 = $1
          AND br.status = 'synced'
          AND sb.driver = 'local_fs'
          AND sb.is_online = TRUE
        ORDER BY sb.read_priority ASC
        LIMIT 1
        "#,
    )
    .bind(sha256)
    .fetch_optional(&state.db)
    .await?;

    if let Some(row) = row {
        let locator: String = row.try_get("locator")?;
        let zfs_path = state.zfs_archive_dir.join(locator);
        if tokio::fs::try_exists(&zfs_path).await.unwrap_or(false) {
            state.staging.put_file(&zfs_path).await?;
        }
    }

    Ok(())
}
