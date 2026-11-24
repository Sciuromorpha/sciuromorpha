use std::io::SeekFrom;
use std::path::Path;

use anyhow::Context;
use askama::Template;
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use sciuromorpha_common::StagingStore;
use sqlx::Row;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::auth::Actor;
use crate::routes::{AppError, HtmlTemplate};
use crate::AppState;

#[derive(Template)]
#[template(path = "partials/stage_in_status.html")]
pub struct StageInStatusPartial {
    pub blob_sha256: String,
    pub backend_id: String,
}

pub async fn stream_blob_by_role(
    State(state): State<AppState>,
    actor: Actor,
    headers: HeaderMap,
    AxumPath((item_id, role)): AxumPath<(Uuid, String)>,
) -> Result<Response, AppError> {
    verify_item_access(&state, &actor, item_id).await?;

    let row = sqlx::query(
        r#"
        SELECT b.sha256, b.mime_type
        FROM item_blobs ib
        JOIN blobs b ON b.sha256 = ib.blob_sha256
        WHERE ib.item_id = $1 AND ib.role = $2
        ORDER BY ib.display_order ASC, ib.created_at ASC
        LIMIT 1
        "#,
    )
    .bind(item_id)
    .bind(&role)
    .fetch_optional(&state.db)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("No blob with role '{role}' on item {item_id}")))?;

    let sha256: String = row.try_get("sha256")?;
    let mime_type: String = row.try_get("mime_type")?;

    serve_resolved_blob(&state, &headers, &sha256, &mime_type).await
}

pub async fn stream_blob_by_sha256(
    State(state): State<AppState>,
    actor: Actor,
    headers: HeaderMap,
    AxumPath((item_id, sha256)): AxumPath<(Uuid, String)>,
) -> Result<Response, AppError> {
    verify_item_access(&state, &actor, item_id).await?;

    let row = sqlx::query(
        r#"
        SELECT b.sha256, b.mime_type
        FROM item_blobs ib
        JOIN blobs b ON b.sha256 = ib.blob_sha256
        WHERE ib.item_id = $1 AND b.sha256 = $2
        LIMIT 1
        "#,
    )
    .bind(item_id)
    .bind(&sha256)
    .fetch_optional(&state.db)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Blob '{sha256}' not linked to item {item_id}")))?;

    let resolved_sha: String = row.try_get("sha256")?;
    let mime_type: String = row.try_get("mime_type")?;

    serve_resolved_blob(&state, &headers, &resolved_sha, &mime_type).await
}

async fn verify_item_access(
    state: &AppState,
    actor: &Actor,
    item_id: Uuid,
) -> Result<(), AppError> {
    let row = sqlx::query("SELECT acl_tags FROM items WHERE id = $1")
        .bind(item_id)
        .fetch_optional(&state.db)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Item {item_id} not found")))?;

    let acl_tags: Vec<String> = row.try_get("acl_tags")?;
    if !actor.can_read_item(item_id, &acl_tags) {
        return Err(AppError::Forbidden(
            "Insufficient ACL permissions to access blob".to_string(),
        ));
    }
    Ok(())
}

async fn serve_resolved_blob(
    state: &AppState,
    req_headers: &HeaderMap,
    sha256: &str,
    mime_type: &str,
) -> Result<Response, AppError> {
    // 1. Hot Staging Cache hit
    if let Ok(Some(staging_path)) = state.staging.local_path(sha256) {
        if tokio::fs::try_exists(&staging_path).await.unwrap_or(false) {
            return stream_local_file(&staging_path, mime_type, req_headers).await;
        }
    }

    // 2. Query synced replicas ordered by online availability & read_priority
    let replicas = sqlx::query(
        r#"
        SELECT br.backend_id, br.locator, sb.driver, sb.is_online
        FROM blob_replicas br
        JOIN storage_backends sb ON sb.id = br.backend_id
        WHERE br.blob_sha256 = $1
          AND br.status = 'synced'
        ORDER BY sb.is_online DESC, sb.read_priority ASC
        "#,
    )
    .bind(sha256)
    .fetch_all(&state.db)
    .await?;

    for rep in &replicas {
        let driver: String = rep.try_get("driver")?;
        let is_online: bool = rep.try_get("is_online")?;
        let locator: String = rep.try_get("locator")?;

        if is_online && driver == "local_fs" {
            let full_path = state.zfs_archive_dir.join(&locator);
            if tokio::fs::try_exists(&full_path).await.unwrap_or(false) {
                return stream_local_file(&full_path, mime_type, req_headers).await;
            }
        }
    }

    // 3. Offline cold storage / LTO tape fallback -> 202 Accepted + HTMX partial
    if let Some(offline_rep) = replicas.first() {
        let backend_id: String = offline_rep.try_get("backend_id")?;
        let partial = HtmlTemplate(StageInStatusPartial {
            blob_sha256: sha256.to_string(),
            backend_id,
        });
        return Ok((StatusCode::ACCEPTED, partial).into_response());
    }

    Err(AppError::NotFound(format!(
        "Blob {sha256} has no online replica or staging file"
    )))
}

/// Streams a local file with RFC 9110 HTTP `Range` (`bytes=start-end`) support for `<video>` / `<audio>` scrubbing.
async fn stream_local_file(
    path: &Path,
    mime_type: &str,
    req_headers: &HeaderMap,
) -> Result<Response, AppError> {
    let mut file = File::open(path)
        .await
        .with_context(|| format!("failed to open blob file {:?}", path))?;
    let meta = file
        .metadata()
        .await
        .with_context(|| format!("failed to stat blob file {:?}", path))?;
    let total_size = meta.len();

    let content_type = HeaderValue::from_str(mime_type)
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));

    let range_header = req_headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok());

    if let Some(range_str) = range_header {
        if let Some(parsed) = parse_byte_range(range_str, total_size) {
            match parsed {
                ByteRange::Satisfiable { start, end } => {
                    let chunk_len = end - start + 1;
                    file.seek(SeekFrom::Start(start))
                        .await
                        .context("failed to seek blob file for Range request")?;

                    let stream = ReaderStream::new(file.take(chunk_len));
                    let body = Body::from_stream(stream);

                    let content_range =
                        HeaderValue::from_str(&format!("bytes {start}-{end}/{total_size}"))
                            .context("invalid Content-Range header value")?;
                    let content_len = HeaderValue::from_str(&chunk_len.to_string())
                        .context("invalid Content-Length header value")?;

                    let mut headers = HeaderMap::new();
                    headers.insert(header::CONTENT_TYPE, content_type);
                    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
                    headers.insert(header::CONTENT_RANGE, content_range);
                    headers.insert(header::CONTENT_LENGTH, content_len);

                    return Ok((StatusCode::PARTIAL_CONTENT, headers, body).into_response());
                }
                ByteRange::Unsatisfiable => {
                    let content_range =
                        HeaderValue::from_str(&format!("bytes */{total_size}"))
                            .context("invalid 416 Content-Range header value")?;
                    let mut headers = HeaderMap::new();
                    headers.insert(header::CONTENT_RANGE, content_range);
                    return Ok((StatusCode::RANGE_NOT_SATISFIABLE, headers).into_response());
                }
            }
        }
    }

    let content_len = HeaderValue::from_str(&total_size.to_string())
        .context("invalid Content-Length header value")?;

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, content_type);
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(header::CONTENT_LENGTH, content_len);

    let body = Body::from_stream(ReaderStream::new(file));
    Ok((StatusCode::OK, headers, body).into_response())
}

#[derive(Debug, PartialEq, Eq)]
enum ByteRange {
    Satisfiable { start: u64, end: u64 },
    Unsatisfiable,
}

fn parse_byte_range(header_val: &str, total_size: u64) -> Option<ByteRange> {
    let spec = header_val.trim().strip_prefix("bytes=")?;
    // Handle single byte range (if multi-range is requested, fall back to first range or full response)
    let first_range = spec.split(',').next()?.trim();
    let (start_part, end_part) = first_range.split_once('-')?;

    if total_size == 0 {
        return Some(ByteRange::Unsatisfiable);
    }

    if start_part.is_empty() {
        // Suffix byte-range-spec: "-500" means last 500 bytes
        let suffix_len: u64 = end_part.parse().ok()?;
        if suffix_len == 0 {
            return Some(ByteRange::Unsatisfiable);
        }
        let start = total_size.saturating_sub(suffix_len);
        Some(ByteRange::Satisfiable {
            start,
            end: total_size - 1,
        })
    } else {
        let start: u64 = start_part.parse().ok()?;
        if start >= total_size {
            return Some(ByteRange::Unsatisfiable);
        }
        let end = if end_part.is_empty() {
            total_size - 1
        } else {
            let parsed_end: u64 = end_part.parse().ok()?;
            if parsed_end < start {
                return Some(ByteRange::Unsatisfiable);
            }
            parsed_end.min(total_size - 1)
        };
        Some(ByteRange::Satisfiable { start, end })
    }
}
