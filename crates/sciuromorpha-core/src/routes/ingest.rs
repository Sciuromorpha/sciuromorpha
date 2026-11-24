use anyhow::Context;
use askama::Template;
use axum::extract::State;
use axum::Form;
use chrono::Utc;
use sciuromorpha_common::{IngestUrlCmd, SUBJECT_CMD_INGEST_URL};
use serde::Deserialize;
use tracing::info;
use uuid::Uuid;

use crate::auth::Actor;
use crate::routes::{AppError, HtmlTemplate};
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct IngestUrlForm {
    pub url: String,
    #[serde(default)]
    pub extract_audio: Option<String>,
}

impl IngestUrlForm {
    pub fn should_extract_audio(&self) -> bool {
        matches!(
            self.extract_audio.as_deref(),
            Some("true" | "1" | "on" | "yes")
        )
    }
}

#[derive(Template)]
#[template(path = "partials/ingest_status.html")]
pub struct IngestStatusPartial {
    pub item_id: Uuid,
    pub url: String,
    pub extract_audio: bool,
}

fn infer_source_type(url: &str) -> &'static str {
    let lower = url.to_ascii_lowercase();
    if lower.contains("bilibili.com") || lower.contains("b23.tv") {
        "bilibili"
    } else if lower.contains("youtube.com") || lower.contains("youtu.be") {
        "youtube"
    } else if lower.contains("twitter.com") || lower.contains("x.com") {
        "twitter"
    } else if lower.contains("pixiv.net") {
        "pixiv"
    } else {
        "manual"
    }
}

pub async fn ingest_url_handler(
    State(state): State<AppState>,
    actor: Actor,
    Form(form): Form<IngestUrlForm>,
) -> Result<HtmlTemplate<IngestStatusPartial>, AppError> {
    actor.require_owner()?;

    let url = form.url.trim().to_string();
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(AppError::BadRequest(
            "URL must start with http:// or https://".to_string(),
        ));
    }

    let item_id = Uuid::now_v7();
    let extract_audio = form.should_extract_audio();
    let source_type = infer_source_type(&url);
    let acl_tags = vec!["private".to_string()];

    // Pre-register placeholder item in PostgreSQL so UI tracks ingesting status immediately
    sqlx::query(
        r#"
        INSERT INTO items (id, item_type, title, source_type, source_url, acl_tags, status)
        VALUES ($1, 'video', $2, $3, $4, $5, 'ingesting')
        "#,
    )
    .bind(item_id)
    .bind(&url)
    .bind(source_type)
    .bind(&url)
    .bind(&acl_tags)
    .execute(&state.db)
    .await?;

    let cmd = IngestUrlCmd {
        item_id,
        url: url.clone(),
        extract_audio,
        acl_tags,
        requested_at: Utc::now(),
    };

    let payload = serde_json::to_vec(&cmd).context("failed to serialize IngestUrlCmd")?;
    state
        .jetstream
        .publish(SUBJECT_CMD_INGEST_URL, payload.into())
        .await
        .context("failed to publish IngestUrlCmd to NATS")?
        .await
        .context("failed awaiting JetStream publish ack for IngestUrlCmd")?;

    info!(%item_id, %url, extract_audio, "Published IngestUrlCmd to vault.cmd.ingest.url");

    Ok(HtmlTemplate(IngestStatusPartial {
        item_id,
        url,
        extract_audio,
    }))
}
