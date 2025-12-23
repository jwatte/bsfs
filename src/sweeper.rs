use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::interval;

use crate::clock::SharedClock;
use crate::cloud::CloudStorage;
use crate::config::Config;
use crate::error::Result;
use crate::fs::operations;
use crate::metadata::{Checkpoint, FileType, InodeMetadata, MetadataLog};

/// Request to trigger an immediate sweep with a specific free space target
#[derive(Debug)]
pub struct SweepRequest {
    /// Target free space to achieve (may be higher than config default)
    pub target_free_space: u64,
    /// Channel to send response when sweep completes
    pub response_tx: oneshot::Sender<SweepResponse>,
}

/// Response from a sweep request
#[derive(Debug)]
pub struct SweepResponse {
    /// Current free space after the sweep
    pub free_space: u64,
    /// Whether the target was achieved
    #[allow(dead_code)]
    pub target_achieved: bool,
}

/// Sender for triggering on-demand sweeps
pub type SweepSender = mpsc::UnboundedSender<SweepRequest>;

/// Entry indicating a SHA should be purged from cold storage
#[derive(Debug, Clone)]
struct PurgeEntry {
    /// The cloud storage key to delete
    key: String,
    /// Human-readable description for logging
    description: String,
}

/// Background task that:
/// 1. Proactively checkpoints modified files to cloud storage
/// 2. Evicts local data only when disk space is low
pub struct Sweeper<C: CloudStorage> {
    config: Config,
    clock: SharedClock,
    checkpoint: Arc<RwLock<Checkpoint>>,
    log: Arc<RwLock<MetadataLog>>,
    cloud: Arc<C>,
    /// Receiver for on-demand sweep requests
    sweep_rx: mpsc::UnboundedReceiver<SweepRequest>,
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
    ) -> (Self, SweepSender) {
        let (sweep_tx, sweep_rx) = mpsc::unbounded_channel();

        let sweeper = Self {
            config,
            clock,
            checkpoint,
            log,
            cloud,
            sweep_rx,
            #[cfg(test)]
            free_space_override: std::sync::Mutex::new(None),
        };

        (sweeper, sweep_tx)
    }

    /// Set the free space value for testing
    #[cfg(test)]
    pub fn set_free_space_for_test(&self, bytes: u64) {
        *self.free_space_override.lock().unwrap() = Some(bytes);
    }

    /// Run the sweeper loop
    pub async fn run(mut self) {
        let mut interval = interval(Duration::from_secs(self.config.sweep_interval_secs));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Regular periodic sweep
                    if let Err(e) = self.sweep_with_target(self.config.target_free_space).await {
                        tracing::error!("Sweeper error: {}", e);
                    }
                }
                Some(request) = self.sweep_rx.recv() => {
                    // On-demand sweep request (e.g., from create() when space is low)
                    tracing::info!(
                        "On-demand sweep requested with target {} bytes",
                        request.target_free_space
                    );

                    let result = self.sweep_with_target(request.target_free_space).await;
                    let free_space = self.get_free_space().unwrap_or(0);
                    let target_achieved = free_space >= request.target_free_space;

                    // Send response (ignore error if receiver dropped)
                    let _ = request.response_tx.send(SweepResponse {
                        free_space,
                        target_achieved,
                    });

                    if let Err(e) = result {
                        tracing::error!("On-demand sweep error: {}", e);
                    }
                }
            }
        }
    }

    /// Perform a single sweep cycle with a specific free space target:
    /// 1. Checkpoint any modified files to cloud (proactive backup)
    /// 2. Purge old versions from cold storage
    /// 3. If low on disk space, evict cold files that are safely checkpointed
    /// 4. Save checkpoint to disk and upload to cloud (only if changes were made)
    async fn sweep_with_target(&self, target_free_space: u64) -> Result<()> {
        tracing::debug!("Starting sweep with target {} bytes", target_free_space);

        let mut checkpoint_changed = false;
        let mut purge_entries: Vec<PurgeEntry> = Vec::new();

        // Phase 1: Checkpoint modified files to cloud (regardless of space pressure)
        let (checkpointed, new_purge_entries) = self.checkpoint_modified_files().await?;
        if checkpointed {
            checkpoint_changed = true;
        }
        purge_entries.extend(new_purge_entries);

        // Phase 2: Purge old versions from cold storage before saving checkpoint
        // This ensures we don't lose track of what needs to be purged if we crash
        if !purge_entries.is_empty() {
            self.purge_old_versions(&purge_entries).await;
        }

        // Phase 2b: Process any pending purges from the log (from upload manager)
        self.process_pending_purges().await?;

        // Phase 3: Evict local data only if we're low on space
        let free_space = self.get_free_space()?;
        if free_space < target_free_space {
            tracing::info!(
                "Free space {} below target {}, evicting cold files",
                free_space,
                target_free_space
            );
            if self.evict_cold_files_to_target(target_free_space).await? {
                checkpoint_changed = true;
            }
        } else {
            tracing::debug!("Sufficient free space: {} bytes", free_space);
        }

        // Phase 4: Save and upload checkpoint only if there were changes
        if checkpoint_changed {
            self.save_checkpoint()?;
            self.upload_checkpoint_to_cloud().await?;
        }

        Ok(())
    }

    /// Perform a sweep with the default target from config
    #[cfg(test)]
    async fn sweep(&self) -> Result<()> {
        self.sweep_with_target(self.config.target_free_space).await
    }

    /// Purge old versions from cold storage
    /// Logs clear messages about what's being purged and any failures
    async fn purge_old_versions(&self, entries: &[PurgeEntry]) {
        tracing::info!(
            "Purging {} old version(s) from cold storage",
            entries.len()
        );

        for entry in entries {
            tracing::info!("Purging from cold storage: {}", entry.description);
            match self.cloud.delete_by_key(&entry.key).await {
                Ok(()) => {
                    tracing::debug!("Successfully purged: {}", entry.key);
                }
                Err(e) => {
                    // Log error but continue with other purges
                    tracing::error!(
                        "Failed to purge {} from cold storage: {}",
                        entry.key,
                        e
                    );
                }
            }
        }
    }

    /// Upload modified files to cloud storage (proactive checkpointing)
    /// Returns (uploaded_any, purge_entries) - whether any files were uploaded and list of old versions to purge
    ///
    /// Uploads run in parallel, limited by `parallel_upload` config setting.
    async fn checkpoint_modified_files(&self) -> Result<(bool, Vec<PurgeEntry>)> {
        let files_needing_checkpoint = self.find_files_needing_checkpoint();

        if files_needing_checkpoint.is_empty() {
            return Ok((false, Vec::new()));
        }

        tracing::info!(
            "Checkpointing {} modified files to cloud (max {} parallel)",
            files_needing_checkpoint.len(),
            self.config.parallel_upload
        );

        let data_root = self.config.data_root();
        let semaphore = Arc::new(Semaphore::new(self.config.parallel_upload as usize));

        // Spawn upload tasks for each file
        let mut handles = Vec::new();
        for inode in files_needing_checkpoint {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let data_root = data_root.clone();
            let checkpoint = self.checkpoint.clone();
            let cloud = self.cloud.clone();
            let clock = self.clock.clone();
            let version_count = self.config.version_count;

            let handle = tokio::spawn(async move {
                let result = Self::checkpoint_file_to_cloud_static(
                    &data_root,
                    inode,
                    &checkpoint,
                    cloud.as_ref(),
                    clock.as_ref(),
                    version_count,
                )
                .await;
                drop(permit); // Release semaphore permit
                (inode, result)
            });
            handles.push(handle);
        }

        // Collect results
        let mut any_uploaded = false;
        let mut purge_entries = Vec::new();

        for handle in handles {
            match handle.await {
                Ok((_inode, Ok((uploaded, new_purge_entries)))) => {
                    if uploaded {
                        any_uploaded = true;
                    }
                    purge_entries.extend(new_purge_entries);
                }
                Ok((inode, Err(e))) => {
                    tracing::error!("Failed to checkpoint inode {}: {}", inode, e);
                    // Continue with other files
                }
                Err(e) => {
                    tracing::error!("Upload task panicked: {}", e);
                }
            }
        }

        Ok((any_uploaded, purge_entries))
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

    /// Upload a single file to cloud storage (instance method for tests)
    /// Returns (uploaded, purge_entries) - whether file was uploaded and any old versions to purge
    #[cfg(test)]
    async fn checkpoint_file_to_cloud(
        &self,
        data_root: &std::path::Path,
        inode: u64,
    ) -> Result<(bool, Vec<PurgeEntry>)> {
        Self::checkpoint_file_to_cloud_static(
            &data_root.to_path_buf(),
            inode,
            &self.checkpoint,
            self.cloud.as_ref(),
            self.clock.as_ref(),
            self.config.version_count,
        )
        .await
    }

    /// Upload a single file to cloud storage (static version for parallel execution)
    /// Returns (uploaded, purge_entries) - whether file was uploaded and any old versions to purge
    async fn checkpoint_file_to_cloud_static(
        data_root: &PathBuf,
        inode: u64,
        checkpoint: &Arc<RwLock<Checkpoint>>,
        cloud: &C,
        clock: &dyn crate::clock::Clock,
        version_count: u32,
    ) -> Result<(bool, Vec<PurgeEntry>)> {
        // Get filename, current checkpointed SHA256, and sha_history from metadata
        let (filename, current_sha256, sha_history) = {
            let cp = checkpoint.read().unwrap();
            let meta = cp.get(inode);
            (
                meta.map(|m| m.filename.clone())
                    .unwrap_or_else(|| format!("{}", inode)),
                meta.and_then(|m| m.checkpointed_sha256),
                meta.map(|m| m.sha_history.clone()).unwrap_or_default(),
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
            let mut cp = checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(inode) {
                meta.checkpointed_time = Some(clock.now());
            }
            return Ok((false, Vec::new()));
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
            return Ok((false, Vec::new()));
        }

        tracing::debug!("Uploading inode {} to cloud storage", inode);

        // Upload to cloud
        let (_version_id, sha256) =
            operations::upload_to_cloud(cloud, data_root, inode, &filename).await?;

        // Update metadata and determine which old versions to purge
        let purge_entries = {
            let mut cp = checkpoint.write().unwrap();
            let mut purge_entries = Vec::new();

            if let Some(meta) = cp.get_mut(inode) {
                meta.checkpointed_time = Some(clock.now());
                meta.checkpointed_sha256 = Some(sha256);

                // Add new SHA to history (at the front, most recent first)
                meta.sha_history.insert(0, sha256);

                // If history exceeds version_count, remove oldest entries and schedule for purging
                let version_count = version_count as usize;
                while meta.sha_history.len() > version_count {
                    if let Some(old_sha) = meta.sha_history.pop() {
                        let key = InodeMetadata::cloud_key_for_sha(old_sha, &meta.filename);
                        let description = format!(
                            "inode {} ({}) - old version {}",
                            inode,
                            meta.filename,
                            hex::encode(old_sha)
                        );
                        tracing::info!(
                            "Scheduling purge of old version for inode {} ({}): keeping {} versions, removing SHA {}",
                            inode,
                            meta.filename,
                            version_count,
                            hex::encode(old_sha)
                        );
                        purge_entries.push(PurgeEntry { key, description });
                    }
                }
            }

            purge_entries
        };

        Ok((true, purge_entries))
    }

    /// Evict cold files from local storage to free up disk space
    /// Only evicts files that are safely checkpointed to cloud
    /// Returns true if any files were evicted
    async fn evict_cold_files_to_target(&self, target_free_space: u64) -> Result<bool> {
        let data_root = self.config.data_root();
        let mut evicted_any = false;

        loop {
            // Check if we have enough space now
            let free_space = self.get_free_space()?;
            if free_space >= target_free_space {
                tracing::info!("Target free space {} reached (have {})", target_free_space, free_space);
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

    /// Evict cold files using the default target from config (for tests)
    #[cfg(test)]
    async fn evict_cold_files(&self) -> Result<bool> {
        self.evict_cold_files_to_target(self.config.target_free_space).await
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

    /// Process pending purge entries from the log
    async fn process_pending_purges(&self) -> Result<()> {
        // Read all log records and collect Purge entries
        let purge_keys: Vec<(String, u64)> = {
            let _log = self.log.read().unwrap();
            let log_path = self.config.log_path();

            match MetadataLog::read_all(&log_path) {
                Ok(records) => records
                    .into_iter()
                    .filter_map(|r| {
                        if let crate::metadata::LogRecord::Purge { key, inode, .. } = r {
                            Some((key, inode))
                        } else {
                            None
                        }
                    })
                    .collect(),
                Err(e) => {
                    tracing::warn!("Failed to read log for pending purges: {}", e);
                    Vec::new()
                }
            }
        };

        if purge_keys.is_empty() {
            return Ok(());
        }

        tracing::info!("Processing {} pending purge(s) from log", purge_keys.len());

        for (key, inode) in purge_keys {
            match self.cloud.delete_by_key(&key).await {
                Ok(()) => {
                    tracing::debug!("Purged old version for inode {}: {}", inode, key);
                }
                Err(e) => {
                    tracing::error!("Failed to purge {} for inode {}: {}", key, inode, e);
                }
            }
        }

        Ok(())
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
            parallel_upload: 8,
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

        let (sweeper, _sweep_tx) = Sweeper::new(
            config,
            clock.clone() as SharedClock,
            checkpoint,
            log,
            cloud,
        );
        sweeper
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

    #[tokio::test]
    async fn test_sha_history_tracking() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Set high free space (no eviction needed)
        sweeper.set_free_space_for_test(10_000_000);

        let data_root = sweeper.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();

        // Add file metadata
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            cp.insert(file);
        }

        // Write first version and sweep
        operations::write_local(&data_root, 2, 0, b"version 1").unwrap();
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(2) {
                meta.mutated_time = clock.as_ref().now();
            }
        }
        sweeper.sweep().await.unwrap();

        // Verify sha_history has one entry
        let sha1 = {
            let cp = sweeper.checkpoint.read().unwrap();
            let meta = cp.get(2).unwrap();
            assert_eq!(meta.sha_history.len(), 1);
            assert_eq!(meta.checkpointed_sha256, Some(meta.sha_history[0]));
            meta.sha_history[0]
        };

        // Write second version and sweep
        clock.advance(Duration::from_secs(100));
        operations::write_local(&data_root, 2, 0, b"version 2").unwrap();
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(2) {
                meta.mutated_time = clock.as_ref().now();
            }
        }
        sweeper.sweep().await.unwrap();

        // Verify sha_history has two entries, newest first
        let sha2 = {
            let cp = sweeper.checkpoint.read().unwrap();
            let meta = cp.get(2).unwrap();
            assert_eq!(meta.sha_history.len(), 2);
            assert_eq!(meta.checkpointed_sha256, Some(meta.sha_history[0]));
            assert_ne!(meta.sha_history[0], sha1); // New SHA is different
            assert_eq!(meta.sha_history[1], sha1); // Old SHA is second
            meta.sha_history[0]
        };

        // Verify both versions are in cloud storage
        let v1_data = sweeper.cloud.download_by_hash(sha1, "test.txt").await.unwrap();
        assert_eq!(v1_data, b"version 1");
        let v2_data = sweeper.cloud.download_by_hash(sha2, "test.txt").await.unwrap();
        assert_eq!(v2_data, b"version 2");
    }

    #[tokio::test]
    async fn test_sha_history_purges_old_versions() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Set high free space (no eviction needed)
        sweeper.set_free_space_for_test(10_000_000);

        // Config has version_count = 3, so we'll create 4 versions
        // and verify the oldest gets purged
        let data_root = sweeper.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();

        // Add file metadata
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            cp.insert(file);
        }

        let mut shas = Vec::new();

        // Create 4 versions (version_count = 3)
        for i in 0..4 {
            clock.advance(Duration::from_secs(100));
            let content = format!("version {}", i);
            operations::write_local(&data_root, 2, 0, content.as_bytes()).unwrap();
            {
                let mut cp = sweeper.checkpoint.write().unwrap();
                if let Some(meta) = cp.get_mut(2) {
                    meta.mutated_time = clock.as_ref().now();
                }
            }
            sweeper.sweep().await.unwrap();

            // Record the SHA
            let sha = {
                let cp = sweeper.checkpoint.read().unwrap();
                let meta = cp.get(2).unwrap();
                meta.checkpointed_sha256.unwrap()
            };
            shas.push(sha);
        }

        // Verify sha_history has exactly version_count entries (3)
        {
            let cp = sweeper.checkpoint.read().unwrap();
            let meta = cp.get(2).unwrap();
            assert_eq!(meta.sha_history.len(), 3, "sha_history should have exactly version_count entries");
            // Most recent first
            assert_eq!(meta.sha_history[0], shas[3]);
            assert_eq!(meta.sha_history[1], shas[2]);
            assert_eq!(meta.sha_history[2], shas[1]);
            // shas[0] (the oldest) should have been purged from history
        }

        // Verify newest 3 versions are still in cloud
        for i in 1..4 {
            let data = sweeper.cloud.download_by_hash(shas[i], "test.txt").await;
            assert!(data.is_ok(), "Version {} should still be in cloud", i);
        }

        // Verify oldest version was purged from cloud
        let oldest_data = sweeper.cloud.download_by_hash(shas[0], "test.txt").await;
        assert!(oldest_data.is_err(), "Version 0 should have been purged from cloud");
    }

    #[tokio::test]
    async fn test_sha_history_deduplication() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let clock = Arc::new(MockClock::default());
        let sweeper = create_test_sweeper(dir.path(), &clock);

        // Set high free space (no eviction needed)
        sweeper.set_free_space_for_test(10_000_000);

        let data_root = sweeper.config.data_root();
        operations::create_local_file(&data_root, 2).unwrap();

        // Add file metadata
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            let file = InodeMetadata::new_file(clock.as_ref(), 2, "test.txt".into(), 1, 1000, 1000, 0o644);
            cp.insert(file);
        }

        // Write version A and sweep
        operations::write_local(&data_root, 2, 0, b"content A").unwrap();
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(2) {
                meta.mutated_time = clock.as_ref().now();
            }
        }
        sweeper.sweep().await.unwrap();

        let sha_a = {
            let cp = sweeper.checkpoint.read().unwrap();
            cp.get(2).unwrap().checkpointed_sha256.unwrap()
        };

        // Write version B
        clock.advance(Duration::from_secs(100));
        operations::write_local(&data_root, 2, 0, b"content B").unwrap();
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(2) {
                meta.mutated_time = clock.as_ref().now();
            }
        }
        sweeper.sweep().await.unwrap();

        let sha_b = {
            let cp = sweeper.checkpoint.read().unwrap();
            cp.get(2).unwrap().checkpointed_sha256.unwrap()
        };

        // Revert to content A (same content as before)
        clock.advance(Duration::from_secs(100));
        operations::write_local(&data_root, 2, 0, b"content A").unwrap();
        {
            let mut cp = sweeper.checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(2) {
                meta.mutated_time = clock.as_ref().now();
            }
        }
        sweeper.sweep().await.unwrap();

        // Verify sha_history doesn't have duplicate sha_a
        // It should be: [sha_a, sha_b] (sha_a moved to front, no duplicate)
        {
            let cp = sweeper.checkpoint.read().unwrap();
            let meta = cp.get(2).unwrap();
            assert_eq!(meta.sha_history.len(), 2, "sha_history should have 2 unique entries");
            assert_eq!(meta.checkpointed_sha256, Some(sha_a));
            assert_eq!(meta.sha_history[0], sha_a); // sha_a moved to front
            assert_eq!(meta.sha_history[1], sha_b); // sha_b is second
        }
    }
}
