mod auth;
mod routes;
mod search;
mod state_machine;

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use async_nats::jetstream::{self, stream};
use sciuromorpha_common::{
    LocalFsStagingStore, STREAM_VAULT_CMD, STREAM_VAULT_EVT, SUBJECT_CMD_WILDCARD,
    SUBJECT_EVT_WILDCARD,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::search::SearchService;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub nats: async_nats::Client,
    pub jetstream: jetstream::Context,
    pub search: SearchService,
    pub staging: Arc<LocalFsStagingStore>,
    pub zfs_archive_dir: PathBuf,
    pub dev_owner_mode: bool,
    pub admin_bearer_token: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,sciuromorpha_core=debug")),
        )
        .init();

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let meili_url = env::var("MEILI_URL").unwrap_or_else(|_| "http://127.0.0.1:7700".to_string());
    let meili_api_key = env::var("MEILI_API_KEY").ok().filter(|s| !s.is_empty());
    let staging_dir =
        PathBuf::from(env::var("STAGING_DIR").unwrap_or_else(|_| "./data/staging".to_string()));
    let zfs_archive_dir = PathBuf::from(
        env::var("ZFS_ARCHIVE_DIR").unwrap_or_else(|_| "./data/zfs-hot".to_string()),
    );
    let bind_addr =
        env::var("CORE_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
    let dev_owner_mode = env::var("DEV_OWNER_MODE")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(true);
    let admin_bearer_token = env::var("ADMIN_BEARER_TOKEN").ok().filter(|s| !s.is_empty());

    tokio::fs::create_dir_all(&staging_dir)
        .await
        .with_context(|| format!("failed to create STAGING_DIR {:?}", staging_dir))?;
    tokio::fs::create_dir_all(&zfs_archive_dir)
        .await
        .with_context(|| format!("failed to create ZFS_ARCHIVE_DIR {:?}", zfs_archive_dir))?;

    info!("Connecting to PostgreSQL and running migrations...");
    let db = PgPoolOptions::new()
        .max_connections(16)
        .connect(&database_url)
        .await
        .context("failed to connect to PostgreSQL")?;

    sqlx::migrate!("../../migrations")
        .run(&db)
        .await
        .context("failed to execute sqlx database migrations")?;
    info!("Database migrations are up to date.");

    info!(%nats_url, "Connecting to NATS and initializing JetStream streams...");
    let nats = async_nats::connect(&nats_url)
        .await
        .with_context(|| format!("failed to connect to NATS at {nats_url}"))?;
    let jetstream = jetstream::new(nats.clone());
    init_jetstream_streams(&jetstream).await?;

    info!(%meili_url, "Initializing Meilisearch vault_items index...");
    let search = SearchService::new(&meili_url, meili_api_key.as_deref())?;
    if let Err(err) = search.configure_index().await {
        warn!(error = %err, "Meilisearch index configuration deferred (verify Meilisearch connectivity)");
    }

    let staging = Arc::new(LocalFsStagingStore::new(staging_dir));

    let state = AppState {
        db,
        nats,
        jetstream,
        search,
        staging,
        zfs_archive_dir,
        dev_owner_mode,
        admin_bearer_token,
    };

    state_machine::spawn_event_loop(state.clone());

    let app = routes::build_router(state);
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed to bind HTTP server on {bind_addr}"))?;
    info!(%bind_addr, "sciuromorpha-core listening");

    axum::serve(listener, app)
        .await
        .context("axum server terminated unexpectedly")?;

    Ok(())
}

async fn init_jetstream_streams(jetstream: &jetstream::Context) -> anyhow::Result<()> {
    let cmd_cfg = stream::Config {
        name: STREAM_VAULT_CMD.to_string(),
        subjects: vec![SUBJECT_CMD_WILDCARD.to_string()],
        retention: stream::RetentionPolicy::WorkQueue,
        storage: stream::StorageType::File,
        ..Default::default()
    };
    jetstream
        .get_or_create_stream(cmd_cfg)
        .await
        .context("failed to create or get JetStream stream VAULT_CMD")?;

    let evt_cfg = stream::Config {
        name: STREAM_VAULT_EVT.to_string(),
        subjects: vec![SUBJECT_EVT_WILDCARD.to_string()],
        retention: stream::RetentionPolicy::Limits,
        storage: stream::StorageType::File,
        ..Default::default()
    };
    jetstream
        .get_or_create_stream(evt_cfg)
        .await
        .context("failed to create or get JetStream stream VAULT_EVT")?;

    info!("JetStream streams VAULT_CMD (WorkQueue) and VAULT_EVT (Limits) ready.");
    Ok(())
}
