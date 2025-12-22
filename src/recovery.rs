use std::time::{Duration, SystemTime};

use crate::clock::Clock;
use crate::config::Config;
use crate::error::{BsfsError, Result};
use crate::fs::operations;
use crate::metadata::{Checkpoint, FileType, MetadataLog};

/// Recovered filesystem state
pub struct RecoveredState {
    pub checkpoint: Checkpoint,
    pub log: MetadataLog,
}

/// Recover the filesystem state from checkpoint and log
pub fn recover(clock: &dyn Clock, config: &Config) -> Result<RecoveredState> {
    let checkpoint_path = config.checkpoint_path();
    let log_path = config.log_path();

    // Load or create checkpoint
    let mut checkpoint = if checkpoint_path.exists() {
        tracing::info!("Loading checkpoint from {:?}", checkpoint_path);
        Checkpoint::load(&checkpoint_path)?
    } else {
        tracing::info!("Creating new checkpoint");
        // Get uid/gid from process
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        Checkpoint::new(clock, uid, gid)
    };

    // Replay log
    if log_path.exists() {
        tracing::info!("Replaying log from {:?}", log_path);
        let records = MetadataLog::read_all(&log_path)?;
        tracing::info!("Replaying {} log records", records.len());
        MetadataLog::replay(clock, &records, &mut checkpoint)?;
    }

    // Validate timestamps
    let now = clock.now();
    let future_timestamps = find_future_timestamps(&checkpoint, now);

    if !future_timestamps.is_empty() {
        if config.inconsistent_start {
            tracing::warn!(
                "Found {} records with future timestamps, adjusting",
                future_timestamps.len()
            );
            adjust_timestamps(&mut checkpoint, now)?;
        } else {
            let mut msg = String::from("Records with future timestamps:\n");
            for (inode, field, time) in &future_timestamps {
                msg.push_str(&format!("  inode {}: {} = {:?}\n", inode, field, time));
            }
            return Err(BsfsError::Recovery(msg));
        }
    }

    // Check for files that were open when daemon crashed (accessedtime > closedtime)
    check_open_files(clock, config, &mut checkpoint)?;

    // Open log for appending
    let log = MetadataLog::open(&log_path)?;

    Ok(RecoveredState { checkpoint, log })
}

/// Find all records with timestamps in the future
fn find_future_timestamps(
    checkpoint: &Checkpoint,
    now: SystemTime,
) -> Vec<(u64, &'static str, SystemTime)> {
    let mut results = Vec::new();

    for meta in checkpoint.inodes.values() {
        // Check internal bookkeeping timestamps
        if meta.created_time > now {
            results.push((meta.inode, "created_time", meta.created_time));
        }
        if meta.mutated_time > now {
            results.push((meta.inode, "mutated_time", meta.mutated_time));
        }
        if meta.accessed_time > now {
            results.push((meta.inode, "accessed_time", meta.accessed_time));
        }
        if meta.closed_time > now {
            results.push((meta.inode, "closed_time", meta.closed_time));
        }
        // Check Unix-level timestamps
        if meta.unix_atime > now {
            results.push((meta.inode, "unix_atime", meta.unix_atime));
        }
        if meta.unix_mtime > now {
            results.push((meta.inode, "unix_mtime", meta.unix_mtime));
        }
        if meta.unix_ctime > now {
            results.push((meta.inode, "unix_ctime", meta.unix_ctime));
        }
        if let Some(cp_time) = meta.checkpointed_time {
            if cp_time > now {
                results.push((meta.inode, "checkpointed_time", cp_time));
            }
        }
    }

    results
}

/// Adjust timestamps so the latest is `now` and others are moved back proportionally
fn adjust_timestamps(checkpoint: &mut Checkpoint, now: SystemTime) -> Result<()> {
    for meta in checkpoint.inodes.values_mut() {
        // Find the latest timestamp (check both internal and Unix timestamps)
        let mut latest = meta.created_time;
        if meta.mutated_time > latest {
            latest = meta.mutated_time;
        }
        if meta.accessed_time > latest {
            latest = meta.accessed_time;
        }
        if meta.closed_time > latest {
            latest = meta.closed_time;
        }
        if meta.unix_atime > latest {
            latest = meta.unix_atime;
        }
        if meta.unix_mtime > latest {
            latest = meta.unix_mtime;
        }
        if meta.unix_ctime > latest {
            latest = meta.unix_ctime;
        }
        if let Some(cp_time) = meta.checkpointed_time {
            if cp_time > latest {
                latest = cp_time;
            }
        }

        // If latest is in the future, adjust all timestamps
        if latest > now {
            let delta = latest.duration_since(now).unwrap_or(Duration::ZERO);

            // Adjust internal bookkeeping timestamps
            meta.created_time = meta.created_time.checked_sub(delta).unwrap_or(now);
            meta.mutated_time = meta.mutated_time.checked_sub(delta).unwrap_or(now);
            meta.accessed_time = meta.accessed_time.checked_sub(delta).unwrap_or(now);
            meta.closed_time = meta.closed_time.checked_sub(delta).unwrap_or(now);

            // Adjust Unix-level timestamps
            meta.unix_atime = meta.unix_atime.checked_sub(delta).unwrap_or(now);
            meta.unix_mtime = meta.unix_mtime.checked_sub(delta).unwrap_or(now);
            meta.unix_ctime = meta.unix_ctime.checked_sub(delta).unwrap_or(now);

            if let Some(cp_time) = meta.checkpointed_time {
                meta.checkpointed_time = Some(cp_time.checked_sub(delta).unwrap_or(now));
            }

            tracing::debug!(
                "Adjusted timestamps for inode {} by {:?}",
                meta.inode,
                delta
            );
        }
    }

    Ok(())
}

/// Check for files that were open when the daemon crashed
fn check_open_files(clock: &dyn Clock, config: &Config, checkpoint: &mut Checkpoint) -> Result<()> {
    let data_root = config.data_root();
    let now = clock.now();

    for meta in checkpoint.inodes.values_mut() {
        // Skip non-files
        if meta.file_type != FileType::RegularFile {
            continue;
        }

        // Skip files that weren't open (accessedtime <= closedtime)
        if meta.accessed_time <= meta.closed_time {
            continue;
        }

        tracing::info!(
            "Found file with accessedtime > closedtime: inode {}",
            meta.inode
        );

        // File was open during crash - check if it was modified
        if meta.local_data_present && operations::local_data_exists(&data_root, meta.inode) {
            match operations::compute_sha256(&data_root, meta.inode) {
                Ok(current_sha) => {
                    let was_modified = meta
                        .checkpointed_sha256
                        .map(|old_sha| old_sha != current_sha)
                        .unwrap_or(true);

                    if was_modified {
                        tracing::info!(
                            "File inode {} was modified, updating mutated_time",
                            meta.inode
                        );
                        meta.mutated_time = now;
                    }

                    // Update closed_time to now
                    meta.closed_time = now;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to compute SHA256 for inode {}: {}",
                        meta.inode,
                        e
                    );
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use crate::metadata::InodeMetadata;

    #[test]
    fn test_find_future_timestamps() {
        let clock = MockClock::default();
        let mut cp = Checkpoint::new(&clock, 0, 0);

        // Advance clock to create a "future" timestamp
        clock.advance(Duration::from_secs(3600));
        let future_time = clock.now();

        // Reset clock to "now"
        clock.set(clock.now() - Duration::from_secs(3600));
        let now = clock.now();

        // Add a file with future timestamp
        let mut meta = InodeMetadata::new_file(&clock, 2, "test.txt".into(), 1, 0, 0, 0o644);
        meta.mutated_time = future_time;
        cp.insert(meta);

        let futures = find_future_timestamps(&cp, now);

        assert!(!futures.is_empty());
        assert!(futures.iter().any(|(ino, field, _)| *ino == 2 && *field == "mutated_time"));
    }

    #[test]
    fn test_adjust_timestamps() {
        let clock = MockClock::default();
        let now = clock.now();

        let mut cp = Checkpoint::new(&clock, 0, 0);

        // Create a future timestamp
        let future = now + Duration::from_secs(3600);

        // Add a file with future timestamp
        let mut meta = InodeMetadata::new_file(&clock, 2, "test.txt".into(), 1, 0, 0, 0o644);
        meta.mutated_time = future;
        meta.created_time = future - Duration::from_secs(100);
        cp.insert(meta);

        adjust_timestamps(&mut cp, now).unwrap();

        let meta = cp.get(2).unwrap();
        assert!(meta.mutated_time <= now);
        assert!(meta.created_time <= now);
        // Relative ordering should be preserved
        assert!(meta.created_time < meta.mutated_time);
    }
}
