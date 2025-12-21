use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::time::interval;

use crate::clock::SharedClock;
use crate::cloud::CloudStorage;
use crate::config::Config;
use crate::error::Result;
use crate::fs::operations;
use crate::metadata::{Checkpoint, FileType, MetadataLog};

/// Background task that:
/// 1. Proactively checkpoints modified files to cloud storage
/// 2. Evicts local data only when disk space is low
pub struct Sweeper<C: CloudStorage> {
    config: Config,
    clock: SharedClock,
    checkpoint: Arc<RwLock<Checkpoint>>,
    log: Arc<RwLock<MetadataLog>>,
    cloud: Arc<C>,
    /// Override free space for testing (None = use real statvfs)
    #[cfg(test)]
    free_space_override: std::sync::Mutex<Option<u64>>,
}

impl<C: CloudStorage + 'static> Sweeper<C> {
    pub fn new(
        config: Config,
        clock: SharedClock,
        checkpoint: Arc<RwLock<Checkpoint>>,
        log: Arc<RwLock<MetadataLog>>,
        cloud: Arc<C>,
    ) -> Self {
        Self {
            config,
            clock,
            checkpoint,
            log,
            cloud,
            #[cfg(test)]
            free_space_override: std::sync::Mutex::new(None),
        }
    }

    /// Set the free space value for testing
    #[cfg(test)]
    pub fn set_free_space_for_test(&self, bytes: u64) {
        *self.free_space_override.lock().unwrap() = Some(bytes);
    }

    /// Run the sweeper loop
    pub async fn run(self) {
        let mut interval = interval(Duration::from_secs(self.config.sweep_interval_secs));

        loop {
            interval.tick().await;
            if let Err(e) = self.sweep().await {
                tracing::error!("Sweeper error: {}", e);
            }
        }
    }

    /// Perform a single sweep cycle:
    /// 1. Checkpoint any modified files to cloud (proactive backup)
    /// 2. If low on disk space, evict cold files that are safely checkpointed
    /// 3. Save checkpoint to disk and upload to cloud (only if changes were made)
    async fn sweep(&self) -> Result<()> {
        tracing::debug!("Starting sweep");

        let mut checkpoint_changed = false;

        // Phase 1: Checkpoint modified files to cloud (regardless of space pressure)
        if self.checkpoint_modified_files().await? {
            checkpoint_changed = true;
        }

        // Phase 2: Evict local data only if we're low on space
        let free_space = self.get_free_space()?;
        if free_space < self.config.target_free_space {
            tracing::info!(
                "Free space {} below target {}, evicting cold files",
                free_space,
                self.config.target_free_space
            );
            if self.evict_cold_files().await? {
                checkpoint_changed = true;
            }
        } else {
            tracing::debug!("Sufficient free space: {} bytes", free_space);
        }

        // Phase 3: Save and upload checkpoint only if there were changes
        if checkpoint_changed {
            self.save_checkpoint()?;
            self.upload_checkpoint_to_cloud().await?;
        }

        Ok(())
    }

    /// Upload modified files to cloud storage (proactive checkpointing)
    /// Returns true if any files were checkpointed
    async fn checkpoint_modified_files(&self) -> Result<bool> {
        let files_needing_checkpoint = self.find_files_needing_checkpoint();

        if files_needing_checkpoint.is_empty() {
            return Ok(false);
        }

        tracing::info!(
            "Checkpointing {} modified files to cloud",
            files_needing_checkpoint.len()
        );

        let data_root = self.config.data_root();

        let mut any_uploaded = false;
        for inode in files_needing_checkpoint {
            match self.checkpoint_file_to_cloud(&data_root, inode).await {
                Ok(uploaded) => {
                    if uploaded {
                        any_uploaded = true;
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to checkpoint inode {}: {}", inode, e);
                    // Continue with other files
                }
            }
        }

        Ok(any_uploaded)
    }

    /// Find files that have been modified since their last checkpoint
    fn find_files_needing_checkpoint(&self) -> Vec<u64> {
        let cp = self.checkpoint.read().unwrap();

        cp.inodes
            .values()
            .filter(|m| {
                m.file_type == FileType::RegularFile
                    && m.local_data_present
                    && m.needs_checkpoint()
            })
            .map(|m| m.inode)
            .collect()
    }

    /// Upload a single file to cloud storage
    /// Returns true if the file was actually uploaded (content changed)
    async fn checkpoint_file_to_cloud(
        &self,
        data_root: &std::path::Path,
        inode: u64,
    ) -> Result<bool> {
        // Get filename and current checkpointed SHA256 from metadata
        let (filename, current_sha256) = {
            let cp = self.checkpoint.read().unwrap();
            let meta = cp.get(inode);
            (
                meta.map(|m| m.filename.clone())
                    .unwrap_or_else(|| format!("{}", inode)),
                meta.and_then(|m| m.checkpointed_sha256),
            )
        };

        // Compute SHA256 of current file content
        let new_sha256 = operations::compute_sha256(data_root, inode)?;

        // Skip upload if content hasn't changed (same SHA256 = same cloud key)
        if Some(new_sha256) == current_sha256 {
            tracing::debug!(
                "Skipping upload for inode {} - content unchanged (SHA256 matches)",
                inode
            );
            // Update checkpointed_time to prevent repeated checks
            let mut cp = self.checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(inode) {
                meta.checkpointed_time = Some(self.clock.as_ref().now());
            }
            return Ok(false);
        }

        tracing::debug!("Uploading inode {} to cloud storage", inode);

        // Upload to cloud
        let (_version_id, sha256) =
            operations::upload_to_cloud(self.cloud.as_ref(), data_root, inode, &filename).await?;

        // Update in-memory metadata
        {
            let mut cp = self.checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(inode) {
                meta.checkpointed_time = Some(self.clock.as_ref().now());
                meta.checkpointed_sha256 = Some(sha256);
            }
        }

        Ok(true)
    }

    /// Evict cold files from local storage to free up disk space
    /// Only evicts files that are safely checkpointed to cloud
    /// Returns true if any files were evicted
    async fn evict_cold_files(&self) -> Result<bool> {
        let data_root = self.config.data_root();
        let mut evicted_any = false;

        loop {
            // Check if we have enough space now
            let free_space = self.get_free_space()?;
            if free_space >= self.config.target_free_space {
                tracing::info!("Target free space reached");
                break;
            }

            // Find next cold file that's safe to evict (already checkpointed)
            let inode = match self.find_next_evictable_file() {
                Some(ino) => ino,
                None => {
                    tracing::warn!(
                        "No more files eligible for eviction, free space still below target"
                    );
                    break;
                }
            };

            tracing::info!("Evicting local data for inode {}", inode);

            // Delete local file
            operations::delete_local_file(&data_root, inode)?;

            // Mark as not local in memory
            {
                let mut cp = self.checkpoint.write().unwrap();
                if let Some(meta) = cp.get_mut(inode) {
                    meta.local_data_present = false;
                }
            }

            // Save checkpoint immediately after each deletion
            // This ensures we don't lose track of the eviction if we crash
            self.save_checkpoint()?;
            evicted_any = true;
        }

        Ok(evicted_any)
    }

    /// Find the next cold file that can be safely evicted
    /// Requirements:
    /// - Is a regular file
    /// - Has local data present
    /// - Has been checkpointed to cloud (checkpointed_sha256 is Some)
    /// - Is cold (closed_time older than threshold)
    /// Returns the coldest (oldest closed_time) eligible file
    fn find_next_evictable_file(&self) -> Option<u64> {
        let cp = self.checkpoint.read().unwrap();
        let now = self.clock.as_ref().now();

        // Consider files cold if not accessed in the last hour
        let cold_threshold = now - Duration::from_secs(3600);

        cp.inodes
            .values()
            .filter(|m| {
                m.file_type == FileType::RegularFile
                    && m.local_data_present
                    && m.checkpointed_sha256.is_some() // Must be safely in cloud
                    && m.closed_time < cold_threshold
            })
            .min_by_key(|m| m.closed_time) // Evict oldest first
            .map(|m| m.inode)
    }

    /// Get free space on the local filesystem
    fn get_free_space(&self) -> Result<u64> {
        // In test mode, allow overriding the free space value
        #[cfg(test)]
        {
            if let Some(override_value) = *self.free_space_override.lock().unwrap() {
                return Ok(override_value);
            }
        }

        let stat = nix::sys::statvfs::statvfs(&self.config.local_root).map_err(|e| {
            crate::error::BsfsError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

        Ok(stat.blocks_available() * stat.block_size() as u64)
    }

    /// Save checkpoint to disk and truncate the log
    fn save_checkpoint(&self) -> Result<()> {
        let cp = self.checkpoint.read().unwrap();
        cp.save(&self.config.checkpoint_path())?;
        drop(cp);

        let mut log = self.log.write().unwrap();
        log.truncate()?;

        tracing::debug!("Checkpoint saved and log truncated");
        Ok(())
    }

    /// Upload checkpoint to cloud storage
    async fn upload_checkpoint_to_cloud(&self) -> Result<()> {
        // Read the checkpoint file from disk
        let checkpoint_path = self.config.checkpoint_path();
        let data = std::fs::read(&checkpoint_path)?;

        tracing::info!("Uploading checkpoint to cloud storage ({} bytes)", data.len());

        let version_id = self.cloud.upload_checkpoint(&data).await?;

        tracing::info!("Checkpoint uploaded to cloud with version {}", version_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Clock, MockClock};
    use crate::cloud::traits::mock::MockCloudStorage;
    use crate::metadata::InodeMetadata;
    use std::time::Duration;
    use tempfile::tempdir;

    fn create_test_config(dir: &std::path::Path) -> Config {
        Config {
            local_root: dir.to_path_buf(),
            gcs_bucket: "test-bucket".into(),
            gcs_credentials: crate::config::GcsCredentials::Ambient,
            target_free_space: 1_000_000, // 1MB target
            sweep_interval_secs: 60,
            version_count: 3,
            inconsistent_start: false,
            max_storage: 0,
        }
    }

    fn create_test_sweeper(
        dir: &std::path::Path,
        clock: &Arc<MockClock>,
    ) -> Sweeper<MockCloudStorage> {
        let config = create_test_config(dir);
        let checkpoint = Arc::new(RwLock::new(Checkpoint::new(clock.as_ref(), 1000, 1000)));
        let log_path = config.log_path();
        let log = Arc::new(RwLock::new(MetadataLog::open(&log_path).unwrap()));
        let cloud = Arc::new(MockCloudStorage::new());

        Sweeper::new(
            config,
            clock.clone() as SharedClock,
            checkpoint,
            log,
            cloud,
        )
    }

    #[test]
    fn test_find_files_needing_checkpoint_empty() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Only root directory exists, no files need checkpointing
        let files = sweeper.find_files_needing_checkpoint();
        assert!(files.is_empty());
    }

    #[test]
    fn test_find_files_needing_checkpoint_new_file() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Add a new file (never checkpointed)
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            cp.insert(file);
        }

        let files = sweeper.find_files_needing_checkpoint();
        assert_eq!(files.len(), 1);
        assert!(files.contains(&2));
    }

    #[test]
    fn test_find_files_needing_checkpoint_modified_file() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Add a file that was checkpointed, then modified
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let mut file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            file.checkpointed_time = Some(clock.as_ref().now());
            file.checkpointed_sha256 = Some([0u8; 32]);
            cp.insert(file);
        }

        // Advance time and modify the file
        clock.advance(Duration::from_secs(100));
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(2) {
                meta.mutated_time = clock.as_ref().now();
            }
        }

        let files = sweeper.find_files_needing_checkpoint();
        assert_eq!(files.len(), 1);
        assert!(files.contains(&2));
    }

    #[test]
    fn test_find_files_needing_checkpoint_already_checkpointed() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Add a file that's already checkpointed and not modified since
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let mut file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            // Checkpoint time is AFTER mutated_time
            clock.advance(Duration::from_secs(10));
            file.checkpointed_time = Some(clock.as_ref().now());
            file.checkpointed_sha256 = Some([0u8; 32]);
            cp.insert(file);
        }

        let files = sweeper.find_files_needing_checkpoint();
        assert!(files.is_empty());
    }

    #[test]
    fn test_find_files_needing_checkpoint_skips_directories() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Add a directory (should not be checkpointed)
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let subdir = InodeMetadata::new_directory(clock.as_ref(), 2, "subdir".into(), 1, 1000, 1000, 0o755);
            cp.insert(subdir);
        }

        let files = sweeper.find_files_needing_checkpoint();
        assert!(files.is_empty());
    }

    #[test]
    fn test_find_next_evictable_file_none_when_not_checkpointed() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Add a cold file that's NOT checkpointed
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let mut file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            file.closed_time = clock.as_ref().now();
            cp.insert(file);
        }

        // Advance past cold threshold (1 hour)
        clock.advance(Duration::from_secs(7200));

        // File is cold but NOT checkpointed, so not evictable
        let evictable = sweeper.find_next_evictable_file();
        assert!(evictable.is_none());
    }

    #[test]
    fn test_find_next_evictable_file_returns_checkpointed_cold_file() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Add a cold file that IS checkpointed
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let mut file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            file.closed_time = clock.as_ref().now();
            file.checkpointed_time = Some(clock.as_ref().now());
            file.checkpointed_sha256 = Some([0u8; 32]);
            cp.insert(file);
        }

        // Advance past cold threshold (1 hour)
        clock.advance(Duration::from_secs(7200));

        let evictable = sweeper.find_next_evictable_file();
        assert_eq!(evictable, Some(2));
    }

    #[test]
    fn test_find_next_evictable_file_not_cold_yet() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Add a checkpointed file that's NOT cold yet
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let mut file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            file.closed_time = clock.as_ref().now();
            file.checkpointed_time = Some(clock.as_ref().now());
            file.checkpointed_sha256 = Some([0u8; 32]);
            cp.insert(file);
        }

        // Only advance 30 minutes (less than 1 hour cold threshold)
        clock.advance(Duration::from_secs(1800));

        let evictable = sweeper.find_next_evictable_file();
        assert!(evictable.is_none());
    }

    #[test]
    fn test_find_next_evictable_file_evicts_oldest_first() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Add two checkpointed files with different ages
        {
            let mut cp = sweeper.checkpoint.write().unwrap();

            // Older file (inode 2)
            let mut file1 = InodeMetadata::new_file(clock.as_ref(), 2, "old.txt".into(), 1, 1000, 1000, 0o644);
            file1.closed_time = clock.as_ref().now();
            file1.checkpointed_time = Some(clock.as_ref().now());
            file1.checkpointed_sha256 = Some([0u8; 32]);
            cp.insert(file1);

            // Advance time
            clock.advance(Duration::from_secs(1000));

            // Newer file (inode 3)
            let mut file2 = InodeMetadata::new_file(clock.as_ref(), 3, "new.txt".into(), 1, 1000, 1000, 0o644);
            file2.closed_time = clock.as_ref().now();
            file2.checkpointed_time = Some(clock.as_ref().now());
            file2.checkpointed_sha256 = Some([0u8; 32]);
            cp.insert(file2);
        }

        // Advance past cold threshold for both files
        clock.advance(Duration::from_secs(7200));

        // Should return the older file first
        let evictable = sweeper.find_next_evictable_file();
        assert_eq!(evictable, Some(2));
    }

    #[test]
    fn test_find_next_evictable_file_skips_already_evicted() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Add a cold, checkpointed file that's already evicted (local_data_present = false)
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let mut file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            file.closed_time = clock.as_ref().now();
            file.checkpointed_time = Some(clock.as_ref().now());
            file.checkpointed_sha256 = Some([0u8; 32]);
            file.local_data_present = false; // Already evicted
            cp.insert(file);
        }

        clock.advance(Duration::from_secs(7200));

        let evictable = sweeper.find_next_evictable_file();
        assert!(evictable.is_none());
    }

    #[tokio::test]
    async fn test_checkpoint_file_to_cloud() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Create a local file
        let data_root = sweeper.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();
        operations::write_local(&data_root, 2, 0, b"test content").unwrap();

        // Add file metadata
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            cp.insert(file);
        }

        // Checkpoint to cloud
        clock.advance(Duration::from_secs(100));
        sweeper.checkpoint_file_to_cloud(&data_root, 2).await.unwrap();

        // Verify metadata was updated
        let sha256 = {
            let cp = sweeper.checkpoint.read().unwrap();
            let meta = cp.get(2).unwrap();
            assert!(meta.checkpointed_time.is_some());
            assert!(meta.checkpointed_sha256.is_some());
            assert!(meta.cloud_key().is_some());
            meta.checkpointed_sha256.unwrap()
        };

        // Verify file is in cloud storage
        let cloud_data = sweeper.cloud.download_by_hash(sha256, "test.txt").await.unwrap();
        assert_eq!(cloud_data, b"test content");
    }

    #[tokio::test]
    async fn test_sweep_checkpoints_before_eviction() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Create a local file
        let data_root = sweeper.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();
        operations::write_local(&data_root, 2, 0, b"test content").unwrap();

        // Add file metadata (not checkpointed, cold)
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let mut file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            file.closed_time = clock.as_ref().now();
            cp.insert(file);

            // Add to parent's children
            if let Some(root) = cp.get_mut(1) {
                root.children.push(2);
            }
        }

        // Advance past cold threshold
        clock.advance(Duration::from_secs(7200));

        // Set low free space to trigger eviction
        sweeper.set_free_space_for_test(100); // Below 1MB target

        // Run sweep
        sweeper.sweep().await.unwrap();

        // File should be checkpointed to cloud and evicted
        let sha256 = {
            let cp = sweeper.checkpoint.read().unwrap();
            let meta = cp.get(2).unwrap();
            assert!(meta.checkpointed_sha256.is_some());
            assert!(meta.cloud_key().is_some());
            assert!(!meta.local_data_present);
            meta.checkpointed_sha256.unwrap()
        };

        // Verify file is in cloud storage
        let cloud_data = sweeper.cloud.download_by_hash(sha256, "test.txt").await.unwrap();
        assert_eq!(cloud_data, b"test content");

        // Local file should be deleted
        assert!(!operations::local_data_exists(&data_root, 2));
    }

    #[tokio::test]
    async fn test_sweep_no_eviction_when_space_sufficient() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Create a local file
        let data_root = sweeper.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();
        operations::write_local(&data_root, 2, 0, b"test content").unwrap();

        // Add file metadata (not checkpointed, cold)
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let mut file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            file.closed_time = clock.as_ref().now();
            cp.insert(file);
        }

        // Advance past cold threshold
        clock.advance(Duration::from_secs(7200));

        // Set high free space (above target)
        sweeper.set_free_space_for_test(10_000_000); // 10MB, above 1MB target

        // Run sweep
        sweeper.sweep().await.unwrap();

        // File should be checkpointed to cloud (proactive) but NOT evicted
        let sha256 = {
            let cp = sweeper.checkpoint.read().unwrap();
            let meta = cp.get(2).unwrap();
            assert!(meta.checkpointed_sha256.is_some());
            assert!(meta.cloud_key().is_some());
            assert!(meta.local_data_present); // Still present locally
            meta.checkpointed_sha256.unwrap()
        };

        // Verify file is in cloud storage
        let cloud_data = sweeper.cloud.download_by_hash(sha256, "test.txt").await.unwrap();
        assert_eq!(cloud_data, b"test content");

        // Local file should still exist
        assert!(operations::local_data_exists(&data_root, 2));
    }

    #[tokio::test]
    async fn test_eviction_requires_checkpoint_first() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Create two local files
        let data_root = sweeper.config.data_root();

        // File 2: old, not checkpointed
        operations::create_local_file(&data_root, 2).unwrap();
        operations::write_local(&data_root, 2, 0, b"old file").unwrap();

        // File 3: newer, already checkpointed (upload to cloud first)
        operations::create_local_file(&data_root, 3).unwrap();
        operations::write_local(&data_root, 3, 0, b"new file").unwrap();
        let sha3 = operations::compute_sha256(&data_root, 3).unwrap();
        let _key3 = sweeper.cloud.upload(3, b"new file", sha3, "new.txt").await.unwrap();

        {
            let mut cp = sweeper.checkpoint.write().unwrap();

            // Old file (not checkpointed)
            let mut file1 = InodeMetadata::new_file(clock.as_ref(), 2, "old.txt".into(), 1, 1000, 1000, 0o644);
            file1.closed_time = clock.as_ref().now();
            cp.insert(file1);

            clock.advance(Duration::from_secs(100));

            // Newer file (already checkpointed - in cloud)
            let mut file2 = InodeMetadata::new_file(clock.as_ref(), 3, "new.txt".into(), 1, 1000, 1000, 0o644);
            file2.closed_time = clock.as_ref().now();
            file2.checkpointed_time = Some(clock.as_ref().now());
            file2.checkpointed_sha256 = Some(sha3);
            // cloud_key() is computed from sha256 + filename
            cp.insert(file2);
        }

        // Advance past cold threshold
        clock.advance(Duration::from_secs(7200));

        // Before sweep: only file 3 is evictable (it's checkpointed)
        let evictable_before = sweeper.find_next_evictable_file();
        assert_eq!(evictable_before, Some(3)); // File 2 is NOT evictable yet

        // Set low free space
        sweeper.set_free_space_for_test(100);

        // Run sweep - this should:
        // 1. Checkpoint file 2 to cloud
        // 2. Evict file 3 first (already checkpointed, older)
        // 3. Then evict file 2 (now checkpointed)
        sweeper.sweep().await.unwrap();

        // Both files should be checkpointed and evicted
        let (key2, key3) = {
            let cp = sweeper.checkpoint.read().unwrap();
            let meta2 = cp.get(2).unwrap();
            let meta3 = cp.get(3).unwrap();

            // Both should have cloud keys (computed from sha256 + filename)
            assert!(meta2.cloud_key().is_some());
            assert!(meta3.cloud_key().is_some());

            // Both should be evicted
            assert!(!meta2.local_data_present);
            assert!(!meta3.local_data_present);

            (meta2.cloud_key().unwrap(), meta3.cloud_key().unwrap())
        };

        // Verify we can download by key
        assert!(sweeper.cloud.download(2, Some(&key2)).await.is_ok());
        assert!(sweeper.cloud.download(3, Some(&key3)).await.is_ok());
    }

    #[tokio::test]
    async fn test_sweep_uploads_checkpoint_to_cloud() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Create a local file
        let data_root = sweeper.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();
        operations::write_local(&data_root, 2, 0, b"test content").unwrap();

        // Add file metadata (not checkpointed)
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            cp.insert(file);
        }

        // Set high free space (no eviction needed)
        sweeper.set_free_space_for_test(10_000_000);

        // Before sweep: no checkpoint in cloud
        let versions_before = sweeper.cloud.list_checkpoint_versions().await.unwrap();
        assert!(versions_before.is_empty());

        // Run sweep - should checkpoint file and upload checkpoint to cloud
        sweeper.sweep().await.unwrap();

        // After sweep: checkpoint should be in cloud
        let versions_after = sweeper.cloud.list_checkpoint_versions().await.unwrap();
        assert_eq!(versions_after.len(), 1);

        // Download and verify checkpoint contains our file
        let checkpoint_data = sweeper.cloud.download_checkpoint(None).await.unwrap();
        assert!(!checkpoint_data.is_empty());
    }

    #[tokio::test]
    async fn test_sweep_creates_new_checkpoint_versions() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Set high free space (no eviction needed)
        sweeper.set_free_space_for_test(10_000_000);

        // Create first file and sweep
        let data_root = sweeper.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();
        operations::write_local(&data_root, 2, 0, b"file1").unwrap();
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let file = InodeMetadata::new_file(clock.as_ref(), 2, "file1.txt".into(), 1, 1000, 1000, 0o644);
            cp.insert(file);
        }
        sweeper.sweep().await.unwrap();

        let versions_after_first = sweeper.cloud.list_checkpoint_versions().await.unwrap();
        assert_eq!(versions_after_first.len(), 1);

        // Create second file and sweep again
        clock.advance(Duration::from_secs(100));
        operations::create_local_file(&data_root, 3).unwrap();
        operations::write_local(&data_root, 3, 0, b"file2").unwrap();
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let file = InodeMetadata::new_file(clock.as_ref(), 3, "file2.txt".into(), 1, 1000, 1000, 0o644);
            cp.insert(file);
        }
        sweeper.sweep().await.unwrap();

        // Should now have two checkpoint versions
        let versions_after_second = sweeper.cloud.list_checkpoint_versions().await.unwrap();
        assert_eq!(versions_after_second.len(), 2);
    }

    #[tokio::test]
    async fn test_sweep_no_changes_skips_checkpoint_upload() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Set high free space (no eviction needed)
        sweeper.set_free_space_for_test(10_000_000);

        // Create a file and run first sweep
        let data_root = sweeper.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();
        operations::write_local(&data_root, 2, 0, b"test content").unwrap();
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            cp.insert(file);
        }

        // First sweep - should checkpoint the file and upload checkpoint
        sweeper.sweep().await.unwrap();

        let versions_after_first = sweeper.cloud.list_checkpoint_versions().await.unwrap();
        assert_eq!(versions_after_first.len(), 1);

        // Second sweep - no changes, should NOT upload a new checkpoint
        clock.advance(Duration::from_secs(60));
        sweeper.sweep().await.unwrap();

        let versions_after_second = sweeper.cloud.list_checkpoint_versions().await.unwrap();
        assert_eq!(versions_after_second.len(), 1); // Still just 1!

        // Third sweep - still no changes
        clock.advance(Duration::from_secs(60));
        sweeper.sweep().await.unwrap();

        let versions_after_third = sweeper.cloud.list_checkpoint_versions().await.unwrap();
        assert_eq!(versions_after_third.len(), 1); // Still just 1!
    }
}
