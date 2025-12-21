use async_trait::async_trait;

use crate::error::Result;

/// Information about a stored version of a file
#[derive(Debug, Clone)]
pub struct VersionInfo {
    /// Version identifier (e.g., generation number or timestamp)
    pub version_id: String,
}

/// Trait for cloud storage backends
#[async_trait]
pub trait CloudStorage: Send + Sync {
    /// Upload a file to cloud storage
    ///
    /// Returns the version ID of the uploaded file
    async fn upload(&self, inode: u64, data: &[u8], sha256: [u8; 32]) -> Result<String>;

    /// Download a specific version of a file
    ///
    /// If version_id is None, downloads the latest version
    async fn download(&self, inode: u64, version_id: Option<&str>) -> Result<Vec<u8>>;

    /// List all versions of a file
    async fn list_versions(&self, inode: u64) -> Result<Vec<VersionInfo>>;

    /// Delete old versions, keeping only the most recent `keep_count`
    async fn delete_old_versions(&self, inode: u64, keep_count: u32) -> Result<()>;
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
        storage: Mutex<HashMap<u64, Vec<(String, Vec<u8>, [u8; 32], SystemTime)>>>,
        next_version: Mutex<u64>,
    }

    impl MockCloudStorage {
        pub fn new() -> Self {
            Self::default()
        }
    }

    #[async_trait]
    impl CloudStorage for MockCloudStorage {
        async fn upload(&self, inode: u64, data: &[u8], sha256: [u8; 32]) -> Result<String> {
            let mut version = self.next_version.lock().unwrap();
            let version_id = format!("v{}", *version);
            *version += 1;

            let mut storage = self.storage.lock().unwrap();
            let versions = storage.entry(inode).or_insert_with(Vec::new);
            versions.push((version_id.clone(), data.to_vec(), sha256, SystemTime::now()));

            Ok(version_id)
        }

        async fn download(&self, inode: u64, version_id: Option<&str>) -> Result<Vec<u8>> {
            let storage = self.storage.lock().unwrap();
            let versions = storage
                .get(&inode)
                .ok_or_else(|| crate::error::BsfsError::FileNotFound(format!("inode {}", inode)))?;

            let data = match version_id {
                Some(vid) => versions
                    .iter()
                    .find(|(v, _, _, _)| v == vid)
                    .map(|(_, d, _, _)| d.clone()),
                None => versions.last().map(|(_, d, _, _)| d.clone()),
            };

            data.ok_or_else(|| {
                crate::error::BsfsError::FileNotFound(format!("inode {} version {:?}", inode, version_id))
            })
        }

        async fn list_versions(&self, inode: u64) -> Result<Vec<VersionInfo>> {
            let storage = self.storage.lock().unwrap();
            let versions = storage.get(&inode).map(|v| {
                v.iter()
                    .map(|(vid, _, _, _)| VersionInfo {
                        version_id: vid.clone(),
                    })
                    .collect()
            });
            Ok(versions.unwrap_or_default())
        }

        async fn delete_old_versions(&self, inode: u64, keep_count: u32) -> Result<()> {
            let mut storage = self.storage.lock().unwrap();
            if let Some(versions) = storage.get_mut(&inode) {
                let keep = keep_count as usize;
                if versions.len() > keep {
                    let drain_count = versions.len() - keep;
                    versions.drain(0..drain_count);
                }
            }
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn test_mock_upload_download() {
            let storage = MockCloudStorage::new();
            let data = b"hello world";
            let sha = [0u8; 32];

            let version = storage.upload(1, data, sha).await.unwrap();
            let downloaded = storage.download(1, Some(&version)).await.unwrap();
            assert_eq!(downloaded, data);
        }

        #[tokio::test]
        async fn test_mock_versions() {
            let storage = MockCloudStorage::new();
            let sha = [0u8; 32];

            storage.upload(1, b"v1", sha).await.unwrap();
            storage.upload(1, b"v2", sha).await.unwrap();
            storage.upload(1, b"v3", sha).await.unwrap();

            let versions = storage.list_versions(1).await.unwrap();
            assert_eq!(versions.len(), 3);

            storage.delete_old_versions(1, 2).await.unwrap();
            let versions = storage.list_versions(1).await.unwrap();
            assert_eq!(versions.len(), 2);
        }
    }
}
