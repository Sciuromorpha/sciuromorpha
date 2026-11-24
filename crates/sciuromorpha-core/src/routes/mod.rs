pub mod backfill;
pub mod blobs;
pub mod ingest;
pub mod ui;

use askama::Template;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use thiserror::Error;
use tracing::error;

use crate::AppState;

const APP_CSS: &str = include_str!("../../assets/app.css");
const HTMX_FALLBACK_JS: &str = include_str!("../../assets/htmx.min.js");

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(ui::index_page))
        .route("/items/{item_id}", get(ui::item_detail_page))
        .route("/authors", get(ui::authors_page))
        .route("/api/ingest/url", post(ingest::ingest_url_handler))
        .route("/api/admin/backfill", post(backfill::backfill_handler))
        .route("/blobs/{item_id}/role/{role}", get(blobs::stream_blob_by_role))
        .route(
            "/blobs/{item_id}/sha256/{sha256}",
            get(blobs::stream_blob_by_sha256),
        )
        .route("/assets/app.css", get(serve_app_css))
        .route("/assets/htmx.min.js", get(serve_htmx_js))
        .with_state(state)
}

async fn serve_app_css() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], APP_CSS)
}

async fn serve_htmx_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/javascript; charset=utf-8")],
        HTMX_FALLBACK_JS,
    )
}

pub struct HtmlTemplate<T>(pub T);

impl<T: Template> IntoResponse for HtmlTemplate<T> {
    fn into_response(self) -> Response {
        match self.0.render() {
            Ok(html) => Html(html).into_response(),
            Err(err) => {
                error!(error = %err, "Askama template rendering error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to render page template",
                )
                    .into_response()
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match &self {
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()).into_response(),
            Self::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.clone()).into_response(),
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()).into_response(),
            Self::Database(err) => {
                error!(error = %err, "SQLx database error in HTTP handler");
                (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response()
            }
            Self::Internal(err) => {
                error!(error = %err, "Internal server error in HTTP handler");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
            }
        }
    }
}
