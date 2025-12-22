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
