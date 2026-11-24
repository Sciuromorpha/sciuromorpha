use std::path::{Path, PathBuf};
use std::pin::Pin;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

pub type BoxAsyncRead = Pin<Box<dyn AsyncRead + Send + Unpin>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedObject {
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Debug, Error)]
pub enum StagingError {
    #[error("invalid sha256 digest '{0}': expected 64 hex characters")]
    InvalidSha256(String),

    #[error("staged blob not found for sha256 '{0}'")]
    NotFound(String),

    #[error("io error in staging store: {0}")]
    Io(#[from] std::io::Error),
}

/// Validates and normalizes a 64-character hexadecimal SHA-256 string.
pub fn validate_sha256(sha256: &str) -> Result<String, StagingError> {
    let trimmed = sha256.trim();
    if trimmed.len() != 64 || !trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(StagingError::InvalidSha256(sha256.to_string()));
    }
    Ok(trimmed.to_ascii_lowercase())
}

/// Computes the two-level sharded relative path `<sha[0..2]>/<sha[2..4]>/<sha>`.
pub fn sharded_relative_path(sha256: &str) -> Result<PathBuf, StagingError> {
    let normalized = validate_sha256(sha256)?;
    let prefix_1 = &normalized[0..2];
    let prefix_2 = &normalized[2..4];
    let mut rel = PathBuf::with_capacity(72);
    rel.push(prefix_1);
    rel.push(prefix_2);
    rel.push(&normalized);
    Ok(rel)
}

#[async_trait]
pub trait StagingStore: Send + Sync {
    /// Streams bytes from `reader`, computes SHA-256 on the fly, and atomically commits
    /// the object into the two-level bucket `<ab>/<cd>/<sha256>`.
    async fn put_reader(
        &self,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<StagedObject, StagingError>;

    /// Streams a local file into the staging store.
    async fn put_file(&self, source_path: &Path) -> Result<StagedObject, StagingError> {
        let mut file = File::open(source_path).await?;
        self.put_reader(&mut file).await
    }

    /// Opens an asynchronous reader for the staged blob.
    async fn open_reader(&self, sha256: &str) -> Result<BoxAsyncRead, StagingError>;

    /// Checks whether the blob exists in the staging store.
    async fn exists(&self, sha256: &str) -> Result<bool, StagingError>;

    /// Removes a blob from the staging store (used by GC after unpinning).
    async fn remove(&self, sha256: &str) -> Result<(), StagingError>;

    /// Returns the underlying local filesystem path if backed by a local mount
    /// (enables zero-copy `ffmpeg` input and HTTP `Range` `File::seek`).
    fn local_path(&self, sha256: &str) -> Result<Option<PathBuf>, StagingError>;
}

#[derive(Debug, Clone)]
pub struct LocalFsStagingStore {
    root: PathBuf,
}

impl LocalFsStagingStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn resolve_path(&self, sha256: &str) -> Result<PathBuf, StagingError> {
        let rel = sharded_relative_path(sha256)?;
        Ok(self.root.join(rel))
    }
}

#[async_trait]
impl StagingStore for LocalFsStagingStore {
    async fn put_reader(
        &self,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<StagedObject, StagingError> {
        let tmp_dir = self.root.join(".tmp");
        fs::create_dir_all(&tmp_dir).await?;

        let tmp_path = tmp_dir.join(format!("upload-{}.part", Uuid::now_v7()));
        let mut tmp_file = File::create(&tmp_path).await?;

        let mut hasher = Sha256::new();
        let mut size_bytes: u64 = 0;
        let mut buf = vec![0u8; 64 * 1024];

        let write_result: Result<(), std::io::Error> = async {
            loop {
                let n = reader.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                tmp_file.write_all(&buf[..n]).await?;
                size_bytes += n as u64;
            }
            tmp_file.flush().await?;
            tmp_file.sync_all().await?;
            Ok(())
        }
        .await;

        drop(tmp_file);

        if let Err(err) = write_result {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(StagingError::Io(err));
        }

        let sha256 = hex::encode(hasher.finalize());
        let final_path = self.resolve_path(&sha256)?;

        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Content-addressed idempotency: if destination already exists, discard temp file.
        if fs::try_exists(&final_path).await? {
            let _ = fs::remove_file(&tmp_path).await;
        } else if let Err(err) = fs::rename(&tmp_path, &final_path).await {
            if fs::try_exists(&final_path).await.unwrap_or(false) {
                let _ = fs::remove_file(&tmp_path).await;
            } else {
                let _ = fs::remove_file(&tmp_path).await;
                return Err(StagingError::Io(err));
            }
        }

        Ok(StagedObject { sha256, size_bytes })
    }

    async fn open_reader(&self, sha256: &str) -> Result<BoxAsyncRead, StagingError> {
        let normalized = validate_sha256(sha256)?;
        let path = self.resolve_path(&normalized)?;
        match File::open(&path).await {
            Ok(file) => Ok(Box::pin(file)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Err(StagingError::NotFound(normalized))
            }
            Err(err) => Err(StagingError::Io(err)),
        }
    }

    async fn exists(&self, sha256: &str) -> Result<bool, StagingError> {
        let path = self.resolve_path(sha256)?;
        Ok(fs::try_exists(path).await?)
    }

    async fn remove(&self, sha256: &str) -> Result<(), StagingError> {
        let path = self.resolve_path(sha256)?;
        match fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(StagingError::Io(err)),
        }
    }

    fn local_path(&self, sha256: &str) -> Result<Option<PathBuf>, StagingError> {
        Ok(Some(self.resolve_path(sha256)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sharded_layout_and_streaming_put() -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("sciuromorpha-test-{}", Uuid::now_v7()));
        let store = LocalFsStagingStore::new(&root);

        let payload = b"sciuromorpha staging cas test payload";
        let mut cursor = std::io::Cursor::new(payload);
        let staged = store.put_reader(&mut cursor).await?;

        assert_eq!(staged.size_bytes, payload.len() as u64);
        assert!(store.exists(&staged.sha256).await?);

        let expected_path = root
            .join(&staged.sha256[0..2])
            .join(&staged.sha256[2..4])
            .join(&staged.sha256);
        assert_eq!(store.local_path(&staged.sha256)?, Some(expected_path));

        let mut reader = store.open_reader(&staged.sha256).await?;
        let mut read_back = Vec::new();
        reader.read_to_end(&mut read_back).await?;
        assert_eq!(read_back, payload);

        store.remove(&staged.sha256).await?;
        let _ = fs::remove_dir_all(&root).await;
        Ok(())
    }
}
