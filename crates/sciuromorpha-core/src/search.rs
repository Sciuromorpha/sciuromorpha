use anyhow::Context;
use chrono::{DateTime, Utc};
use meilisearch_sdk::client::Client;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use tracing::warn;
use uuid::Uuid;

use crate::auth::Actor;

pub const MEILI_INDEX_VAULT_ITEMS: &str = "vault_items";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultSearchDoc {
    pub id: Uuid,
    pub title: String,
    pub description: Option<String>,
    pub extracted_text: Option<String>,
    pub item_type: String,
    pub source_type: String,
    pub author_ids: Vec<Uuid>,
    pub author_names: Vec<String>,
    pub author_handles: Vec<String>,
    pub available_roles: Vec<String>,
    pub acl_tags: Vec<String>,
    pub tags: Vec<String>,
    pub original_created_at: Option<i64>,
    pub captured_at: i64,
}

#[derive(Debug, Clone)]
pub struct ItemCardView {
    pub id: Uuid,
    pub title: String,
    pub item_type: String,
    pub source_type: String,
    pub source_url: Option<String>,
    pub status: String,
    pub acl_tags: Vec<String>,
    pub author_names: Vec<String>,
    pub has_derived_audio: bool,
    pub captured_at_display: String,
}

#[derive(Clone)]
pub struct SearchService {
    client: Client,
}

impl SearchService {
    pub fn new(meili_url: &str, api_key: Option<&str>) -> anyhow::Result<Self> {
        let client = Client::new(meili_url, api_key)
            .context("failed to construct Meilisearch client")?;
        Ok(Self { client })
    }

    /// Configures `vault_items` index schema according to Section 6.1 of ARCHITECTURE.md.
    pub async fn configure_index(&self) -> anyhow::Result<()> {
        let index = self.client.index(MEILI_INDEX_VAULT_ITEMS);

        index
            .set_searchable_attributes([
                "title",
                "author_names",
                "author_handles",
                "description",
                "extracted_text",
                "tags",
            ])
            .await
            .context("failed to set Meilisearch searchableAttributes")?;

        index
            .set_filterable_attributes([
                "acl_tags",
                "item_type",
                "source_type",
                "author_ids",
                "available_roles",
                "original_created_at",
                "captured_at",
            ])
            .await
            .context("failed to set Meilisearch filterableAttributes")?;

        index
            .set_sortable_attributes(["original_created_at", "captured_at"])
            .await
            .context("failed to set Meilisearch sortableAttributes")?;

        index
            .set_displayed_attributes([
                "id",
                "title",
                "item_type",
                "source_type",
                "author_names",
                "available_roles",
                "acl_tags",
                "original_created_at",
            ])
            .await
            .context("failed to set Meilisearch displayedAttributes")?;

        Ok(())
    }

    /// Rebuilds the search document for `item_id` from PostgreSQL (Single Source of Truth)
    /// and pushes it to Meilisearch `vault_items`.
    pub async fn sync_item_by_id(&self, db: &PgPool, item_id: Uuid) -> anyhow::Result<()> {
        let item_row = sqlx::query(
            r#"
            SELECT id, item_type, title, description, extracted_text, source_type,
                   original_created_at, captured_at, acl_tags, extra_meta
            FROM items
            WHERE id = $1
            "#,
        )
        .bind(item_id)
        .fetch_optional(db)
        .await?;

        let Some(row) = item_row else {
            return Ok(());
        };

        let author_rows = sqlx::query(
            r#"
            SELECT DISTINCT a.id AS author_id, a.display_name, ai.handle
            FROM item_authors ia
            JOIN author_identities ai ON ai.id = ia.identity_id
            JOIN authors a ON a.id = ai.author_id
            WHERE ia.item_id = $1
            "#,
        )
        .bind(item_id)
        .fetch_all(db)
        .await?;

        let role_rows = sqlx::query(
            r#"
            SELECT DISTINCT role
            FROM item_blobs
            WHERE item_id = $1
            "#,
        )
        .bind(item_id)
        .fetch_all(db)
        .await?;

        let mut author_ids = Vec::with_capacity(author_rows.len());
        let mut author_names = Vec::with_capacity(author_rows.len());
        let mut author_handles = Vec::with_capacity(author_rows.len());
        for a in author_rows {
            author_ids.push(a.try_get("author_id")?);
            author_names.push(a.try_get("display_name")?);
            author_handles.push(a.try_get("handle")?);
        }

        let mut available_roles = Vec::with_capacity(role_rows.len());
        for r in role_rows {
            available_roles.push(r.try_get("role")?);
        }

        let acl_tags: Vec<String> = row.try_get("acl_tags")?;
        let extra_meta: serde_json::Value = row.try_get("extra_meta")?;

        let mut tags = Vec::new();
        if let Some(arr) = extra_meta.get("tags").and_then(|v| v.as_array()) {
            for val in arr {
                if let Some(s) = val.as_str() {
                    tags.push(s.to_string());
                }
            }
        }

        let original_created_at: Option<DateTime<Utc>> = row.try_get("original_created_at")?;
        let captured_at: DateTime<Utc> = row.try_get("captured_at")?;

        let doc = VaultSearchDoc {
            id: row.try_get("id")?,
            title: row.try_get("title")?,
            description: row.try_get("description")?,
            extracted_text: row.try_get("extracted_text")?,
            item_type: row.try_get("item_type")?,
            source_type: row.try_get("source_type")?,
            author_ids,
            author_names,
            author_handles,
            available_roles,
            acl_tags,
            tags,
            original_created_at: original_created_at.map(|dt| dt.timestamp()),
            captured_at: captured_at.timestamp(),
        };

        let index = self.client.index(MEILI_INDEX_VAULT_ITEMS);
        index
            .add_or_replace(&[doc], Some("id"))
            .await
            .context("failed to upsert VaultSearchDoc into Meilisearch")?;

        Ok(())
    }

    /// Dual-guarded search (Section 6.2):
    /// 1. Injects `acl_tags` and optional `available_roles = "derived_audio"` into Meilisearch.
    /// 2. Re-verifies every candidate `item_id` against PostgreSQL `items.acl_tags` before rendering.
    pub async fn search_items(
        &self,
        db: &PgPool,
        actor: &Actor,
        query: &str,
        audio_only: bool,
        limit: usize,
    ) -> anyhow::Result<Vec<ItemCardView>> {
        let Some(meili_filter) = build_meili_filter(actor, audio_only) else {
            return Ok(Vec::new());
        };

        let index = self.client.index(MEILI_INDEX_VAULT_ITEMS);
        let sort_criteria = ["captured_at:desc"];
        let mut search_req = index.search();
        search_req
            .with_query(query)
            .with_limit(limit)
            .with_sort(&sort_criteria);

        if !meili_filter.is_empty() {
            search_req.with_filter(&meili_filter);
        }

        let candidate_ids: Vec<Uuid> = match search_req.execute::<VaultSearchDoc>().await {
            Ok(results) => results.hits.into_iter().map(|h| h.result.id).collect(),
            Err(err) => {
                warn!(error = %err, "Meilisearch query failed; falling back to PostgreSQL scan");
                fallback_pg_candidate_ids(db, query, audio_only, limit as i64).await?
            }
        };

        // When browsing the homepage with an empty query while async Meilisearch indexing is catching up,
        // supplement from PostgreSQL so freshly submitted items appear immediately.
        let ordered_ids = if candidate_ids.is_empty() && query.trim().is_empty() {
            fallback_pg_candidate_ids(db, "", audio_only, limit as i64).await?
        } else {
            candidate_ids
        };

        if ordered_ids.is_empty() {
            return Ok(Vec::new());
        }

        hydrate_and_verify_cards(db, actor, &ordered_ids, audio_only).await
    }
}

fn escape_meili_str(val: &str) -> String {
    let escaped = val.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

/// Returns `None` if the actor has zero permitted ACL scope, or `Some(filter_string)` otherwise.
fn build_meili_filter(actor: &Actor, audio_only: bool) -> Option<String> {
    let mut clauses = Vec::new();

    match actor {
        Actor::Owner => {}
        Actor::Peer {
            allowed_acl_tags, ..
        } => {
            let mut tags = allowed_acl_tags.clone();
            if !tags.iter().any(|t| t == "public") {
                tags.push("public".to_string());
            }
            let list = tags
                .iter()
                .map(|t| escape_meili_str(t))
                .collect::<Vec<_>>()
                .join(", ");
            clauses.push(format!("acl_tags IN [{list}]"));
        }
        Actor::Guest {
            target_item_id,
            allowed_acl_tags,
        } => {
            let mut sub = Vec::new();
            if let Some(id) = target_item_id {
                sub.push(format!("id = {}", escape_meili_str(&id.to_string())));
            }
            if let Some(tags) = allowed_acl_tags {
                if !tags.is_empty() {
                    let list = tags
                        .iter()
                        .map(|t| escape_meili_str(t))
                        .collect::<Vec<_>>()
                        .join(", ");
                    sub.push(format!("acl_tags IN [{list}]"));
                }
            }
            if sub.is_empty() {
                return None;
            }
            clauses.push(format!("({})", sub.join(" OR ")));
        }
        Actor::Anonymous => {
            clauses.push("acl_tags = \"public\"".to_string());
        }
    }

    if audio_only {
        clauses.push("available_roles = \"derived_audio\"".to_string());
    }

    Some(clauses.join(" AND "))
}

async fn fallback_pg_candidate_ids(
    db: &PgPool,
    query: &str,
    audio_only: bool,
    limit: i64,
) -> anyhow::Result<Vec<Uuid>> {
    let pattern = format!("%{}%", query.trim());
    let rows = sqlx::query(
        r#"
        SELECT i.id
        FROM items i
        WHERE ($1 = '%%' OR i.title ILIKE $1 OR COALESCE(i.description, '') ILIKE $1)
          AND (
              NOT $2
              OR EXISTS (
                  SELECT 1 FROM item_blobs ib
                  WHERE ib.item_id = i.id AND ib.role = 'derived_audio'
              )
          )
        ORDER BY i.captured_at DESC
        LIMIT $3
        "#,
    )
    .bind(pattern)
    .bind(audio_only)
    .bind(limit)
    .fetch_all(db)
    .await?;

    let mut ids = Vec::with_capacity(rows.len());
    for row in rows {
        ids.push(row.try_get("id")?);
    }
    Ok(ids)
}

async fn hydrate_and_verify_cards(
    db: &PgPool,
    actor: &Actor,
    ordered_ids: &[Uuid],
    audio_only: bool,
) -> anyhow::Result<Vec<ItemCardView>> {
    let rows = sqlx::query(
        r#"
        SELECT
            i.id,
            i.title,
            i.item_type,
            i.source_type,
            i.source_url,
            i.status,
            i.acl_tags,
            i.captured_at,
            COALESCE(
                ARRAY(
                    SELECT DISTINCT a.display_name
                    FROM item_authors ia
                    JOIN author_identities ai ON ai.id = ia.identity_id
                    JOIN authors a ON a.id = ai.author_id
                    WHERE ia.item_id = i.id
                ),
                ARRAY[]::TEXT[]
            ) AS author_names,
            EXISTS (
                SELECT 1 FROM item_blobs ib
                WHERE ib.item_id = i.id AND ib.role = 'derived_audio'
            ) AS has_derived_audio
        FROM items i
        WHERE i.id = ANY($1)
        "#,
    )
    .bind(ordered_ids)
    .fetch_all(db)
    .await?;

    let mut by_id = std::collections::HashMap::with_capacity(rows.len());
    for row in rows {
        let id: Uuid = row.try_get("id")?;
        let acl_tags: Vec<String> = row.try_get("acl_tags")?;
        let has_derived_audio: bool = row.try_get("has_derived_audio")?;

        // Secondary PostgreSQL ACL verification (Section 6.2 dual guard)
        if !actor.can_read_item(id, &acl_tags) {
            continue;
        }
        if audio_only && !has_derived_audio {
            continue;
        }

        let captured_at: DateTime<Utc> = row.try_get("captured_at")?;
        let view = ItemCardView {
            id,
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
        by_id.insert(id, view);
    }

    let mut cards = Vec::with_capacity(by_id.len());
    for id in ordered_ids {
        if let Some(card) = by_id.remove(id) {
            cards.push(card);
        }
    }
    Ok(cards)
}
