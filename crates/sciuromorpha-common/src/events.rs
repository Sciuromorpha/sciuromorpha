use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ==========================================
// JetStream Stream & Subject Constants
// ==========================================

pub const STREAM_VAULT_CMD: &str = "VAULT_CMD";
pub const STREAM_VAULT_EVT: &str = "VAULT_EVT";

pub const SUBJECT_CMD_WILDCARD: &str = "vault.cmd.>";
pub const SUBJECT_EVT_WILDCARD: &str = "vault.evt.>";

pub const SUBJECT_CMD_INGEST_URL: &str = "vault.cmd.ingest.url";
pub const SUBJECT_CMD_MEDIA_EXTRACT_AUDIO: &str = "vault.cmd.media.extract_audio";
pub const SUBJECT_CMD_MEDIA_THUMBNAIL: &str = "vault.cmd.media.thumbnail";
pub const SUBJECT_CMD_STORAGE_ZFS_HOT_REPLICATE: &str = "vault.cmd.storage.zfs-hot.replicate";
pub const SUBJECT_CMD_STORAGE_ZFS_HOT_STAGE_IN: &str = "vault.cmd.storage.zfs-hot.stage_in";

pub const SUBJECT_EVT_INGEST_COMPLETED: &str = "vault.evt.ingest.completed";
pub const SUBJECT_EVT_MEDIA_DERIVATIVE_READY: &str = "vault.evt.media.derivative_ready";
pub const SUBJECT_EVT_ITEM_META_PATCH: &str = "vault.evt.item.meta_patch";
pub const SUBJECT_EVT_STORAGE_SYNCED: &str = "vault.evt.storage.synced";
pub const SUBJECT_EVT_STORAGE_STAGE_READY: &str = "vault.evt.storage.stage_ready";

pub fn subject_storage_replicate(backend_id: &str) -> String {
    format!("vault.cmd.storage.{backend_id}.replicate")
}

pub fn subject_storage_stage_in(backend_id: &str) -> String {
    format!("vault.cmd.storage.{backend_id}.stage_in")
}

// ==========================================
// Command Payloads (VAULT_CMD)
// ==========================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestUrlCmd {
    pub item_id: Uuid,
    pub url: String,
    #[serde(default)]
    pub extract_audio: bool,
    #[serde(default = "default_private_acl")]
    pub acl_tags: Vec<String>,
    pub requested_at: DateTime<Utc>,
}

fn default_private_acl() -> Vec<String> {
    vec!["private".to_string()]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractAudioCmd {
    pub item_id: Uuid,
    pub source_blob_sha256: String,
    #[serde(default = "default_audio_bitrate_kbps")]
    pub bitrate_kbps: u32,
    pub id3_title: Option<String>,
    pub id3_artist: Option<String>,
}

fn default_audio_bitrate_kbps() -> u32 {
    192
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThumbnailCmd {
    pub item_id: Uuid,
    pub source_blob_sha256: String,
    #[serde(default = "default_thumb_max_width")]
    pub max_width: u32,
}

fn default_thumb_max_width() -> u32 {
    640
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateBlobCmd {
    pub blob_sha256: String,
    pub backend_id: String,
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageInBlobCmd {
    pub item_id: Option<Uuid>,
    pub blob_sha256: String,
    pub backend_id: String,
    pub locator: String,
}

// ==========================================
// Event Payloads (VAULT_EVT)
// ==========================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestedAuthor {
    pub platform: String,
    pub platform_uid: String,
    pub handle: String,
    pub display_name: Option<String>,
    pub profile_url: Option<String>,
    #[serde(default = "default_author_role")]
    pub role: String,
}

fn default_author_role() -> String {
    "creator".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestedBlob {
    pub sha256: String,
    pub size_bytes: i64,
    pub mime_type: String,
    pub role: String,
    #[serde(default)]
    pub display_order: i32,
    #[serde(default = "empty_json_object")]
    pub meta: serde_json::Value,
}

fn empty_json_object() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestCompletedEvt {
    pub item_id: Uuid,
    pub item_type: String,
    pub title: String,
    pub description: Option<String>,
    pub extracted_text: Option<String>,
    pub source_type: String,
    pub source_url: String,
    pub source_external_id: Option<String>,
    pub original_created_at: Option<DateTime<Utc>>,
    #[serde(default = "default_private_acl")]
    pub acl_tags: Vec<String>,
    #[serde(default = "empty_json_object")]
    pub extra_meta: serde_json::Value,
    #[serde(default)]
    pub authors: Vec<IngestedAuthor>,
    pub blobs: Vec<IngestedBlob>,
    #[serde(default)]
    pub extract_audio: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivativeReadyEvt {
    pub item_id: Uuid,
    pub derived_from_sha256: String,
    pub blob_sha256: String,
    pub size_bytes: i64,
    pub mime_type: String,
    pub role: String,
    pub extracted_text: Option<String>,
    #[serde(default = "empty_json_object")]
    pub meta: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemMetaPatchEvt {
    pub item_id: Uuid,
    pub patch: serde_json::Value,
    pub extracted_text: Option<String>,
    pub source_agent: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSyncedEvt {
    pub blob_sha256: String,
    pub backend_id: String,
    pub locator: String,
    #[serde(default)]
    pub is_encrypted: bool,
    pub verified_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStageReadyEvt {
    pub item_id: Option<Uuid>,
    pub blob_sha256: String,
    pub backend_id: String,
}
