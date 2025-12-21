use async_trait::async_trait;

use crate::error::Result;

/// Information about a stored version of a file
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct VersionInfo {
    /// Version identifier (e.g., generation number or timestamp)
    pub version_id: String,
}

/// Trait for cloud storage backends
#[async_trait]
#[allow(dead_code)]
pub trait CloudStorage: Send + Sync {
    /// Upload a file to cloud storage
    ///
    /// Key format: `{sha256[0:2]}/{sha256[0:4]}/{sha256}.{filename}`
    /// Returns the version ID (the full key path)
    async fn upload(&self, inode: u64, data: &[u8], sha256: [u8; 32], filename: &str) -> Result<String>;

    /// Download a file by its SHA256 hash and filename
    ///
    /// If version_id is None, uses the provided sha256/filename to construct the key
    async fn download(&self, inode: u64, version_id: Option<&str>) -> Result<Vec<u8>>;

    /// Download a file directly by SHA256 and filename
    async fn download_by_hash(&self, sha256: [u8; 32], filename: &str) -> Result<Vec<u8>>;

    /// List all versions of a file
    async fn list_versions(&self, inode: u64) -> Result<Vec<VersionInfo>>;

    /// Delete old versions, keeping only the most recent `keep_count`
    async fn delete_old_versions(&self, inode: u64, keep_count: u32) -> Result<()>;

    /// Delete a specific version by its key
    async fn delete_by_key(&self, key: &str) -> Result<()>;

    /// Upload the filesystem checkpoint to cloud storage
    ///
    /// Returns the version ID of the uploaded checkpoint
    async fn upload_checkpoint(&self, data: &[u8]) -> Result<String>;

    /// Download a specific version of the checkpoint
    ///
    /// If version_id is None, downloads the latest version
    async fn download_checkpoint(&self, version_id: Option<&str>) -> Result<Vec<u8>>;

    /// List all versions of the checkpoint
    async fn list_checkpoint_versions(&self) -> Result<Vec<VersionInfo>>;
}

/// Mock implementation for testing
#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::SystemTime;

    #[derive(Default)]
    pub struct MockCloudStorage {
        /// Storage by key (SHA256-based key -> data)
        storage: Mutex<HashMap<String, (Vec<u8>, [u8; 32], SystemTime)>>,
        checkpoint_storage: Mutex<Vec<(String, Vec<u8>, SystemTime)>>,
        next_version: Mutex<u64>,
    }

    impl MockCloudStorage {
        pub fn new() -> Self {
            Self::default()
        }

        /// Generate object key from SHA256 and filename (same format as GcsStorage)
        fn object_key(sha256: [u8; 32], filename: &str) -> String {
            let hex = hex::encode(sha256);
            format!("{}/{}/{}.{}", &hex[0..2], &hex[0..4], hex, filename)
        }
    }

    #[async_trait]
    impl CloudStorage for MockCloudStorage {
        async fn upload(&self, _inode: u64, data: &[u8], sha256: [u8; 32], filename: &str) -> Result<String> {
            let key = Self::object_key(sha256, filename);

            let mut storage = self.storage.lock().unwrap();
            storage.insert(key.clone(), (data.to_vec(), sha256, SystemTime::now()));

            Ok(key)
        }

        async fn download(&self, _inode: u64, version_id: Option<&str>) -> Result<Vec<u8>> {
            let key = version_id
                .ok_or_else(|| crate::error::BsfsError::FileNotFound("version_id required".to_string()))?;

            let storage = self.storage.lock().unwrap();
            storage
                .get(key)
                .map(|(data, _, _)| data.clone())
                .ok_or_else(|| crate::error::BsfsError::FileNotFound(format!("key {}", key)))
        }

        async fn download_by_hash(&self, sha256: [u8; 32], filename: &str) -> Result<Vec<u8>> {
            let key = Self::object_key(sha256, filename);

            let storage = self.storage.lock().unwrap();
            storage
                .get(&key)
                .map(|(data, _, _)| data.clone())
                .ok_or_else(|| crate::error::BsfsError::FileNotFound(format!("key {}", key)))
        }

        async fn list_versions(&self, _inode: u64) -> Result<Vec<VersionInfo>> {
            // With SHA256-based keys, versions must be tracked in metadata
            Ok(Vec::new())
        }

        async fn delete_old_versions(&self, _inode: u64, _keep_count: u32) -> Result<()> {
            // With SHA256-based keys, version cleanup must be done via metadata
            Ok(())
        }

        async fn delete_by_key(&self, key: &str) -> Result<()> {
            let mut storage = self.storage.lock().unwrap();
            storage.remove(key);
            Ok(())
        }

        async fn upload_checkpoint(&self, data: &[u8]) -> Result<String> {
            let mut version = self.next_version.lock().unwrap();
            let version_id = format!("cp_v{}", *version);
            *version += 1;

            let mut storage = self.checkpoint_storage.lock().unwrap();
            storage.push((version_id.clone(), data.to_vec(), SystemTime::now()));

            Ok(version_id)
        }

        async fn download_checkpoint(&self, version_id: Option<&str>) -> Result<Vec<u8>> {
            let storage = self.checkpoint_storage.lock().unwrap();
            if storage.is_empty() {
                return Err(crate::error::BsfsError::FileNotFound("checkpoint".to_string()));
            }

            let data = match version_id {
                Some(vid) => storage
                    .iter()
                    .find(|(v, _, _)| v == vid)
                    .map(|(_, d, _)| d.clone()),
                None => storage.last().map(|(_, d, _)| d.clone()),
            };

            data.ok_or_else(|| {
                crate::error::BsfsError::FileNotFound(format!("checkpoint version {:?}", version_id))
            })
        }

        async fn list_checkpoint_versions(&self) -> Result<Vec<VersionInfo>> {
            let storage = self.checkpoint_storage.lock().unwrap();
            Ok(storage
                .iter()
                .map(|(vid, _, _)| VersionInfo {
                    version_id: vid.clone(),
                })
                .collect())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use sha2::{Sha256, Digest};

        #[tokio::test]
        async fn test_mock_upload_download() {
            let storage = MockCloudStorage::new();
            let data = b"hello world";
            let sha: [u8; 32] = Sha256::digest(data).into();

            let key = storage.upload(1, data, sha, "test.txt").await.unwrap();

            // Download by key
            let downloaded = storage.download(1, Some(&key)).await.unwrap();
            assert_eq!(downloaded, data);

            // Download by hash
            let downloaded2 = storage.download_by_hash(sha, "test.txt").await.unwrap();
            assert_eq!(downloaded2, data);
        }

        #[tokio::test]
        async fn test_mock_key_format() {
            let storage = MockCloudStorage::new();
            let data = b"test content";
            let sha: [u8; 32] = Sha256::digest(data).into();
            let hex = hex::encode(sha);

            let key = storage.upload(1, data, sha, "myfile.dat").await.unwrap();

            // Verify key format: {sha[0:2]}/{sha[0:4]}/{sha}.{filename}
            let expected = format!("{}/{}/{}.myfile.dat", &hex[0..2], &hex[0..4], hex);
            assert_eq!(key, expected);
        }

        #[tokio::test]
        async fn test_mock_delete_by_key() {
            let storage = MockCloudStorage::new();
            let data = b"delete me";
            let sha: [u8; 32] = Sha256::digest(data).into();

            let key = storage.upload(1, data, sha, "delete.txt").await.unwrap();

            // File exists
            assert!(storage.download(1, Some(&key)).await.is_ok());

            // Delete it
            storage.delete_by_key(&key).await.unwrap();

            // File no longer exists
            assert!(storage.download(1, Some(&key)).await.is_err());
        }
    }
}
