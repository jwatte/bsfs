use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, Semaphore};

use crate::clock::SharedClock;
use crate::cloud::CloudStorage;
use crate::config::Config;
use crate::error::Result;
use crate::fs::operations;
use crate::metadata::{Checkpoint, FileType, InodeMetadata, LogRecord, MetadataLog};

/// Request to upload a file to cloud storage
#[derive(Debug)]
pub struct UploadRequest {
    pub inode: u64,
}

/// Sender half for queuing upload requests
pub type UploadSender = mpsc::UnboundedSender<UploadRequest>;

/// Background task that processes upload requests with limited parallelism
pub struct UploadManager<C: CloudStorage> {
    config: Config,
    clock: SharedClock,
    checkpoint: Arc<RwLock<Checkpoint>>,
    log: Arc<RwLock<MetadataLog>>,
    cloud: Arc<C>,
    receiver: mpsc::UnboundedReceiver<UploadRequest>,
    semaphore: Arc<Semaphore>,
}

impl<C: CloudStorage + 'static> UploadManager<C> {
    /// Create a new upload manager and return the sender for queuing requests
    pub fn new(
        config: Config,
        clock: SharedClock,
        checkpoint: Arc<RwLock<Checkpoint>>,
        log: Arc<RwLock<MetadataLog>>,
        cloud: Arc<C>,
    ) -> (Self, UploadSender) {
        let (tx, rx) = mpsc::unbounded_channel();
        let semaphore = Arc::new(Semaphore::new(config.parallel_upload as usize));

        let manager = Self {
            config,
            clock,
            checkpoint,
            log,
            cloud,
            receiver: rx,
            semaphore,
        };

        (manager, tx)
    }

    /// Run the upload manager loop
    pub async fn run(mut self) {
        tracing::info!(
            "Upload manager started (max {} parallel uploads)",
            self.config.parallel_upload
        );

        while let Some(request) = self.receiver.recv().await {
            let permit = self.semaphore.clone().acquire_owned().await.unwrap();
            let data_root = self.config.data_root();
            let checkpoint = self.checkpoint.clone();
            let log = self.log.clone();
            let cloud = self.cloud.clone();
            let clock = self.clock.clone();
            let version_count = self.config.version_count;
            let inode = request.inode;

            tokio::spawn(async move {
                match Self::upload_file(
                    &data_root,
                    inode,
                    &checkpoint,
                    &log,
                    cloud.as_ref(),
                    clock.as_ref(),
                    version_count,
                )
                .await
                {
                    Ok(uploaded) => {
                        if uploaded {
                            tracing::debug!("Uploaded inode {} to cloud storage", inode);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to upload inode {}: {}", inode, e);
                    }
                }
                drop(permit);
            });
        }

        tracing::info!("Upload manager stopped");
    }

    /// Upload a single file to cloud storage
    async fn upload_file(
        data_root: &PathBuf,
        inode: u64,
        checkpoint: &Arc<RwLock<Checkpoint>>,
        log: &Arc<RwLock<MetadataLog>>,
        cloud: &C,
        clock: &dyn crate::clock::Clock,
        version_count: u32,
    ) -> Result<bool> {
        // Get filename, current checkpointed SHA256, and sha_history from metadata
        let (filename, current_sha256, sha_history, is_regular_file, local_data_present) = {
            let cp = checkpoint.read().unwrap();
            let meta = cp.get(inode);
            match meta {
                Some(m) => (
                    m.filename.clone(),
                    m.checkpointed_sha256,
                    m.sha_history.clone(),
                    m.file_type == FileType::RegularFile,
                    m.local_data_present,
                ),
                None => {
                    tracing::warn!("Upload requested for unknown inode {}", inode);
                    return Ok(false);
                }
            }
        };

        // Skip directories and files without local data
        if !is_regular_file || !local_data_present {
            return Ok(false);
        }

        // Compute SHA256 of current file content
        let new_sha256 = match operations::compute_sha256(data_root, inode) {
            Ok(sha) => sha,
            Err(e) => {
                tracing::warn!("Failed to compute SHA256 for inode {}: {}", inode, e);
                return Ok(false);
            }
        };

        // Skip upload if content hasn't changed (same SHA256 = same cloud key)
        if Some(new_sha256) == current_sha256 {
            tracing::debug!(
                "Skipping upload for inode {} - content unchanged",
                inode
            );
            // Update checkpointed_time to prevent sweeper from re-uploading
            let mut cp = checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(inode) {
                meta.checkpointed_time = Some(clock.now());
            }
            return Ok(false);
        }

        // Check if this SHA256 already exists in history (deduplication)
        if sha_history.contains(&new_sha256) {
            tracing::debug!(
                "Skipping upload for inode {} - SHA256 already in history (deduplicated)",
                inode
            );
            // Update metadata but don't upload
            let mut cp = checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(inode) {
                meta.checkpointed_time = Some(clock.now());
                meta.checkpointed_sha256 = Some(new_sha256);
                // Move this SHA to the front of history (most recent)
                meta.sha_history.retain(|&sha| sha != new_sha256);
                meta.sha_history.insert(0, new_sha256);
            }
            return Ok(false);
        }

        tracing::info!("Uploading inode {} ({}) to cloud storage", inode, filename);

        // Upload to cloud
        let (_version_id, sha256) =
            operations::upload_to_cloud(cloud, data_root, inode, &filename).await?;

        // Update metadata and log purge entries for old versions
        let now = clock.now();
        {
            let mut cp = checkpoint.write().unwrap();

            if let Some(meta) = cp.get_mut(inode) {
                meta.checkpointed_time = Some(now);
                meta.checkpointed_sha256 = Some(sha256);

                // Add new SHA to history (at the front, most recent first)
                meta.sha_history.insert(0, sha256);

                // If history exceeds version_count, log purge entries for old versions
                let version_count = version_count as usize;
                while meta.sha_history.len() > version_count {
                    if let Some(old_sha) = meta.sha_history.pop() {
                        let key = InodeMetadata::cloud_key_for_sha(old_sha, &meta.filename);
                        tracing::info!(
                            "Logging purge for old version of inode {} ({}): {}",
                            inode,
                            meta.filename,
                            hex::encode(old_sha)
                        );

                        // Log the purge entry - sweeper will execute it
                        if let Ok(mut log_guard) = log.write() {
                            let _ = log_guard.append(&LogRecord::Purge {
                                key,
                                inode,
                                timestamp: now,
                            });
                        }
                    }
                }
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Clock, MockClock};
    use crate::cloud::traits::mock::MockCloudStorage;
    use crate::fs::operations;
    use std::time::Duration;
    use tempfile::tempdir;

    fn create_test_config(dir: &std::path::Path) -> Config {
        Config {
            local_root: dir.to_path_buf(),
            gcs_bucket: "test-bucket".into(),
            gcs_credentials: crate::config::GcsCredentials::Ambient,
            target_free_space: 1_000_000,
            sweep_interval_secs: 60,
            version_count: 3,
            inconsistent_start: false,
            max_storage: 0,
            parallel_upload: 8,
        }
    }

    fn create_test_upload_manager(
        dir: &std::path::Path,
        clock: &Arc<MockClock>,
    ) -> (UploadManager<MockCloudStorage>, UploadSender, Arc<MockCloudStorage>) {
        let config = create_test_config(dir);
        let checkpoint = Arc::new(RwLock::new(Checkpoint::new(clock.as_ref(), 1000, 1000)));
        let log_path = config.log_path();
        let log = Arc::new(RwLock::new(MetadataLog::open(&log_path).unwrap()));
        let cloud = Arc::new(MockCloudStorage::new());

        let (manager, tx) = UploadManager::new(
            config,
            clock.clone() as SharedClock,
            checkpoint,
            log,
            cloud.clone(),
        );

        (manager, tx, cloud)
    }

    #[tokio::test]
    async fn test_upload_file_basic() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let (manager, _tx, cloud) = create_test_upload_manager(dir.path(), &clock);

        // Create a local file
        let data_root = manager.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();
        operations::write_local(&data_root, 2, 0, b"test content").unwrap();

        // Add file metadata
        {
            let mut cp = manager.checkpoint.write().unwrap();
            let file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            cp.insert(file);
        }

        // Upload the file
        let uploaded = UploadManager::upload_file(
            &data_root,
            2,
            &manager.checkpoint,
            &manager.log,
            cloud.as_ref(),
            clock.as_ref(),
            manager.config.version_count,
        )
        .await
        .unwrap();

        assert!(uploaded);

        // Verify metadata was updated
        let sha256 = {
            let cp = manager.checkpoint.read().unwrap();
            let meta = cp.get(2).unwrap();
            assert!(meta.checkpointed_time.is_some());
            assert!(meta.checkpointed_sha256.is_some());
            assert_eq!(meta.sha_history.len(), 1);
            meta.checkpointed_sha256.unwrap()
        };

        // Verify file is in cloud storage
        let cloud_data = cloud.download_by_hash(sha256, "test.txt").await.unwrap();
        assert_eq!(cloud_data, b"test content");
    }

    #[tokio::test]
    async fn test_upload_file_skips_unchanged_content() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let (manager, _tx, cloud) = create_test_upload_manager(dir.path(), &clock);

        // Create a local file
        let data_root = manager.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();
        operations::write_local(&data_root, 2, 0, b"test content").unwrap();

        let sha256 = operations::compute_sha256(&data_root, 2).unwrap();

        // Add file metadata with same SHA already checkpointed
        {
            let mut cp = manager.checkpoint.write().unwrap();
            let mut file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            file.checkpointed_sha256 = Some(sha256);
            file.checkpointed_time = Some(clock.now());
            cp.insert(file);
        }

        // Try to upload - should skip
        let uploaded = UploadManager::upload_file(
            &data_root,
            2,
            &manager.checkpoint,
            &manager.log,
            cloud.as_ref(),
            clock.as_ref(),
            manager.config.version_count,
        )
        .await
        .unwrap();

        assert!(!uploaded);
    }

    #[tokio::test]
    async fn test_upload_file_deduplication() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let (manager, _tx, cloud) = create_test_upload_manager(dir.path(), &clock);

        // Create a local file
        let data_root = manager.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();
        operations::write_local(&data_root, 2, 0, b"test content").unwrap();

        let sha256 = operations::compute_sha256(&data_root, 2).unwrap();

        // Add file metadata with SHA in history but different current checkpointed_sha256
        {
            let mut cp = manager.checkpoint.write().unwrap();
            let mut file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            file.checkpointed_sha256 = Some([0u8; 32]); // Different SHA
            file.sha_history = vec![[0u8; 32], sha256]; // But target SHA is in history
            cp.insert(file);
        }

        // Try to upload - should skip due to deduplication
        let uploaded = UploadManager::upload_file(
            &data_root,
            2,
            &manager.checkpoint,
            &manager.log,
            cloud.as_ref(),
            clock.as_ref(),
            manager.config.version_count,
        )
        .await
        .unwrap();

        assert!(!uploaded);

        // Verify SHA was moved to front of history
        let cp = manager.checkpoint.read().unwrap();
        let meta = cp.get(2).unwrap();
        assert_eq!(meta.checkpointed_sha256, Some(sha256));
        assert_eq!(meta.sha_history[0], sha256);
    }

    #[tokio::test]
    async fn test_upload_file_logs_purge_for_old_versions() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let (manager, _tx, cloud) = create_test_upload_manager(dir.path(), &clock);

        let data_root = manager.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();

        // Add file with version_count (3) existing versions in history
        let existing_shas: Vec<[u8; 32]> = (0..3).map(|i| [i as u8; 32]).collect();
        {
            let mut cp = manager.checkpoint.write().unwrap();
            let mut file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            file.sha_history = existing_shas.clone();
            cp.insert(file);
        }

        // Write new content and upload
        operations::write_local(&data_root, 2, 0, b"new content").unwrap();

        let uploaded = UploadManager::upload_file(
            &data_root,
            2,
            &manager.checkpoint,
            &manager.log,
            cloud.as_ref(),
            clock.as_ref(),
            manager.config.version_count,
        )
        .await
        .unwrap();

        assert!(uploaded);

        // Verify sha_history has version_count entries
        {
            let cp = manager.checkpoint.read().unwrap();
            let meta = cp.get(2).unwrap();
            assert_eq!(meta.sha_history.len(), 3);
            // Oldest SHA should have been removed
            assert!(!meta.sha_history.contains(&existing_shas[2]));
        }

        // Verify a Purge log record was created
        let log_path = manager.config.log_path();
        let records = MetadataLog::read_all(&log_path).unwrap();
        let purge_records: Vec<_> = records
            .iter()
            .filter(|r| matches!(r, LogRecord::Purge { .. }))
            .collect();
        assert_eq!(purge_records.len(), 1);
    }

    #[tokio::test]
    async fn test_upload_file_skips_directories() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let (manager, _tx, cloud) = create_test_upload_manager(dir.path(), &clock);

        // Add a directory
        {
            let mut cp = manager.checkpoint.write().unwrap();
            let subdir = InodeMetadata::new_directory(clock.as_ref(), 2, "subdir".into(), 1, 1000, 1000, 0o755);
            cp.insert(subdir);
        }

        let data_root = manager.config.data_root();
        let uploaded = UploadManager::upload_file(
            &data_root,
            2,
            &manager.checkpoint,
            &manager.log,
            cloud.as_ref(),
            clock.as_ref(),
            manager.config.version_count,
        )
        .await
        .unwrap();

        assert!(!uploaded);
    }

    #[tokio::test]
    async fn test_upload_file_skips_evicted_files() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let (manager, _tx, cloud) = create_test_upload_manager(dir.path(), &clock);

        // Add a file with local_data_present = false (evicted)
        {
            let mut cp = manager.checkpoint.write().unwrap();
            let mut file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            file.local_data_present = false;
            cp.insert(file);
        }

        let data_root = manager.config.data_root();
        let uploaded = UploadManager::upload_file(
            &data_root,
            2,
            &manager.checkpoint,
            &manager.log,
            cloud.as_ref(),
            clock.as_ref(),
            manager.config.version_count,
        )
        .await
        .unwrap();

        assert!(!uploaded);
    }

    #[tokio::test]
    async fn test_upload_file_handles_unknown_inode() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let (manager, _tx, cloud) = create_test_upload_manager(dir.path(), &clock);

        let data_root = manager.config.data_root();

        // Try to upload non-existent inode
        let uploaded = UploadManager::upload_file(
            &data_root,
            999,
            &manager.checkpoint,
            &manager.log,
            cloud.as_ref(),
            clock.as_ref(),
            manager.config.version_count,
        )
        .await
        .unwrap();

        assert!(!uploaded);
    }

    #[tokio::test]
    async fn test_upload_manager_processes_requests() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let (manager, tx, cloud) = create_test_upload_manager(dir.path(), &clock);

        // Create a local file
        let data_root = manager.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();
        operations::write_local(&data_root, 2, 0, b"test content").unwrap();

        // Add file metadata
        {
            let mut cp = manager.checkpoint.write().unwrap();
            let file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            cp.insert(file);
        }

        let checkpoint = manager.checkpoint.clone();

        // Start the upload manager in a background task
        let manager_handle = tokio::spawn(manager.run());

        // Send an upload request
        tx.send(UploadRequest { inode: 2 }).unwrap();

        // Drop the sender to signal shutdown
        drop(tx);

        // Wait for manager to finish
        let _ = tokio::time::timeout(Duration::from_secs(5), manager_handle).await;

        // Verify file was uploaded
        let sha256 = {
            let cp = checkpoint.read().unwrap();
            let meta = cp.get(2).unwrap();
            assert!(meta.checkpointed_sha256.is_some());
            meta.checkpointed_sha256.unwrap()
        };

        let cloud_data = cloud.download_by_hash(sha256, "test.txt").await.unwrap();
        assert_eq!(cloud_data, b"test content");
    }
}
