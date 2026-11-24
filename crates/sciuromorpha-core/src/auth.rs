use axum::extract::FromRequestParts;
use axum::http::header::{AUTHORIZATION, COOKIE};
use axum::http::request::Parts;
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::routes::AppError;
use crate::AppState;

#[derive(Debug, Clone)]
pub enum Actor {
    Owner,
    Peer {
        token_id: Uuid,
        label: String,
        allowed_acl_tags: Vec<String>,
    },
    Guest {
        target_item_id: Option<Uuid>,
        allowed_acl_tags: Option<Vec<String>>,
    },
    Anonymous,
}

impl Actor {
    pub fn is_owner(&self) -> bool {
        matches!(self, Self::Owner)
    }

    pub fn require_owner(&self) -> Result<(), AppError> {
        if self.is_owner() {
            Ok(())
        } else {
            Err(AppError::Forbidden(
                "Owner privileges required for this operation".to_string(),
            ))
        }
    }

    pub fn can_read_item(&self, item_id: Uuid, item_acl_tags: &[String]) -> bool {
        match self {
            Self::Owner => true,
            Self::Peer {
                allowed_acl_tags, ..
            } => item_acl_tags
                .iter()
                .any(|tag| tag == "public" || allowed_acl_tags.contains(tag)),
            Self::Guest {
                target_item_id,
                allowed_acl_tags,
            } => {
                if target_item_id == &Some(item_id) {
                    return true;
                }
                if let Some(allowed) = allowed_acl_tags {
                    return item_acl_tags.iter().any(|tag| allowed.contains(tag));
                }
                false
            }
            Self::Anonymous => item_acl_tags.iter().any(|tag| tag == "public"),
        }
    }
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

fn extract_cookie_value(cookie_header: &str, key: &str) -> Option<String> {
    for pair in cookie_header.split(';') {
        let trimmed = pair.trim();
        if let Some((k, v)) = trimmed.split_once('=') {
            if k.trim() == key && !v.trim().is_empty() {
                return Some(v.trim().to_string());
            }
        }
    }
    None
}

impl FromRequestParts<AppState> for Actor {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        // 1. Cloudflare Access JWT assertion header (production Owner ingress)
        if let Some(cf_jwt) = parts.headers.get("Cf-Access-Jwt-Assertion") {
            if !cf_jwt.is_empty() {
                return Ok(Actor::Owner);
            }
        }

        // 2. Explicit Admin Bearer Token
        if let (Some(expected), Some(auth_val)) = (
            state.admin_bearer_token.as_deref(),
            parts.headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()),
        ) {
            if let Some(bearer) = auth_val.strip_prefix("Bearer ") {
                if bearer.trim() == expected {
                    return Ok(Actor::Owner);
                }
            }
        }

        // 3. Peer token via X-Peer-Token header or sciuromorpha_peer HttpOnly cookie
        let raw_peer_token = parts
            .headers
            .get("X-Peer-Token")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .or_else(|| {
                parts
                    .headers
                    .get(COOKIE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|c| extract_cookie_value(c, "sciuromorpha_peer"))
            });

        if let Some(raw_token) = raw_peer_token {
            let token_hash = sha256_hex(&raw_token);
            let row = sqlx::query(
                r#"
                SELECT id, label, allowed_acl_tags
                FROM peer_tokens
                WHERE token_hash = $1
                  AND is_active = TRUE
                  AND (expires_at IS NULL OR expires_at > NOW())
                "#,
            )
            .bind(&token_hash)
            .fetch_optional(&state.db)
            .await?;

            if let Some(row) = row {
                return Ok(Actor::Peer {
                    token_id: row.try_get("id")?,
                    label: row.try_get("label")?,
                    allowed_acl_tags: row.try_get("allowed_acl_tags")?,
                });
            }
        }

        // 4. Guest share token via X-Share-Token header or query `share_token=...`
        let raw_share_token = parts
            .headers
            .get("X-Share-Token")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .or_else(|| {
                parts.uri.query().and_then(|q| {
                    q.split('&').find_map(|kv| {
                        let (k, v) = kv.split_once('=')?;
                        (k == "share_token" && !v.is_empty()).then(|| v.to_string())
                    })
                })
            });

        if let Some(raw_share) = raw_share_token {
            let token_hash = sha256_hex(&raw_share);
            let row = sqlx::query(
                r#"
                SELECT target_item_id, allowed_acl_tags
                FROM share_links
                WHERE token_hash = $1
                  AND revoked = FALSE
                  AND (expires_at IS NULL OR expires_at > NOW())
                "#,
            )
            .bind(&token_hash)
            .fetch_optional(&state.db)
            .await?;

            if let Some(row) = row {
                return Ok(Actor::Guest {
                    target_item_id: row.try_get("target_item_id")?,
                    allowed_acl_tags: row.try_get("allowed_acl_tags")?,
                });
            }
        }

        // 5. Local Incus dev container Owner fallback
        if state.dev_owner_mode {
            return Ok(Actor::Owner);
        }

        Ok(Actor::Anonymous)
    }
}
