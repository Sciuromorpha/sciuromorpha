use askama::Template;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::Row;
use uuid::Uuid;

use crate::auth::Actor;
use crate::routes::{AppError, HtmlTemplate};
use crate::search::ItemCardView;
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub audio_only: Option<String>,
}

impl SearchParams {
    pub fn is_audio_only(&self) -> bool {
        matches!(
            self.audio_only.as_deref(),
            Some("true" | "1" | "on" | "yes")
        )
    }
}

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    pub query: String,
    pub audio_only: bool,
    pub is_owner: bool,
    pub items: Vec<ItemCardView>,
}

#[derive(Template)]
#[template(path = "partials/item_list.html")]
pub struct ItemListPartialTemplate {
    pub items: Vec<ItemCardView>,
}

pub async fn index_page(
    State(state): State<AppState>,
    actor: Actor,
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> Result<Response, AppError> {
    let audio_only = params.is_audio_only();
    let items = state
        .search
        .search_items(&state.db, &actor, &params.q, audio_only, 40)
        .await?;

    let is_hx = headers
        .get("HX-Request")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == "true");

    if is_hx {
        Ok(HtmlTemplate(ItemListPartialTemplate { items }).into_response())
    } else {
        Ok(HtmlTemplate(IndexTemplate {
            query: params.q,
            audio_only,
            is_owner: actor.is_owner(),
            items,
        })
        .into_response())
    }
}

pub struct BlobSummaryView {
    pub role: String,
    pub sha256: String,
    pub mime_type: String,
    pub size_bytes: i64,
}

#[derive(Template)]
#[template(path = "item_detail.html")]
pub struct ItemDetailTemplate {
    pub item: ItemCardView,
    pub description: Option<String>,
    pub has_primary_video: bool,
    pub blobs: Vec<BlobSummaryView>,
    pub extra_meta_pretty: String,
}

pub async fn item_detail_page(
    State(state): State<AppState>,
    actor: Actor,
    Path(item_id): Path<Uuid>,
) -> Result<HtmlTemplate<ItemDetailTemplate>, AppError> {
    let row = sqlx::query(
        r#"
        SELECT
            i.id, i.title, i.description, i.item_type, i.source_type,
            i.source_url, i.status, i.acl_tags, i.extra_meta, i.captured_at,
            COALESCE(
                ARRAY(
                    SELECT DISTINCT a.display_name
                    FROM item_authors ia
                    JOIN author_identities ai ON ai.id = ia.identity_id
                    JOIN authors a ON a.id = ai.author_id
                    WHERE ia.item_id = i.id
                ),
                ARRAY[]::TEXT[]
            ) AS author_names
        FROM items i
        WHERE i.id = $1
        "#,
    )
    .bind(item_id)
    .fetch_optional(&state.db)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Item {item_id} not found")))?;

    let acl_tags: Vec<String> = row.try_get("acl_tags")?;
    if !actor.can_read_item(item_id, &acl_tags) {
        return Err(AppError::Forbidden(
            "You do not have permission to view this item".to_string(),
        ));
    }

    let blob_rows = sqlx::query(
        r#"
        SELECT ib.role, ib.blob_sha256, b.mime_type, b.size_bytes
        FROM item_blobs ib
        JOIN blobs b ON b.sha256 = ib.blob_sha256
        WHERE ib.item_id = $1
        ORDER BY ib.display_order ASC, ib.created_at ASC
        "#,
    )
    .bind(item_id)
    .fetch_all(&state.db)
    .await?;

    let mut blobs = Vec::with_capacity(blob_rows.len());
    let mut has_primary_video = false;
    let mut has_derived_audio = false;
    for b in blob_rows {
        let role: String = b.try_get("role")?;
        if role == "primary_video" {
            has_primary_video = true;
        }
        if role == "derived_audio" {
            has_derived_audio = true;
        }
        blobs.push(BlobSummaryView {
            role,
            sha256: b.try_get("blob_sha256")?,
            mime_type: b.try_get("mime_type")?,
            size_bytes: b.try_get("size_bytes")?,
        });
    }

    let captured_at: DateTime<Utc> = row.try_get("captured_at")?;
    let extra_meta: serde_json::Value = row.try_get("extra_meta")?;
    let extra_meta_pretty =
        serde_json::to_string_pretty(&extra_meta).unwrap_or_else(|_| "{}".to_string());

    let item = ItemCardView {
        id: item_id,
        title: row.try_get("title")?,
        item_type: row.try_get("item_type")?,
        source_type: row.try_get("source_type")?,
        source_url: row.try_get("source_url")?,
        status: row.try_get("status")?,
        acl_tags,
        author_names: row.try_get("author_names")?,
        has_derived_audio,
        captured_at_display: captured_at.format("%Y-%m-%d %H:%M UTC").to_string(),
    };

    Ok(HtmlTemplate(ItemDetailTemplate {
        item,
        description: row.try_get("description")?,
        has_primary_video,
        blobs,
        extra_meta_pretty,
    }))
}

pub struct AuthorIdentityRowView {
    pub author_id: Uuid,
    pub author_display_name: String,
    pub identity_id: Uuid,
    pub platform: String,
    pub platform_uid: String,
    pub handle: String,
}

#[derive(Template)]
#[template(path = "authors.html")]
pub struct AuthorsTemplate {
    pub identities: Vec<AuthorIdentityRowView>,
}

pub async fn authors_page(
    State(state): State<AppState>,
    actor: Actor,
) -> Result<HtmlTemplate<AuthorsTemplate>, AppError> {
    actor.require_owner()?;

    let rows = sqlx::query(
        r#"
        SELECT a.id AS author_id, a.display_name,
               ai.id AS identity_id, ai.platform, ai.platform_uid, ai.handle
        FROM author_identities ai
        JOIN authors a ON a.id = ai.author_id
        ORDER BY a.display_name ASC, ai.platform ASC
        LIMIT 200
        "#,
    )
    .fetch_all(&state.db)
    .await?;

    let mut identities = Vec::with_capacity(rows.len());
    for r in rows {
        identities.push(AuthorIdentityRowView {
            author_id: r.try_get("author_id")?,
            author_display_name: r.try_get("display_name")?,
            identity_id: r.try_get("identity_id")?,
            platform: r.try_get("platform")?,
            platform_uid: r.try_get("platform_uid")?,
            handle: r.try_get("handle")?,
        });
    }

    Ok(HtmlTemplate(AuthorsTemplate { identities }))
}
