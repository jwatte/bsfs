use clap::Parser;
use fuser::MountOption;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;

mod clock;
mod cloud;
mod config;
mod error;
mod fs;
mod metadata;
mod recovery;
mod sweeper;
mod upload_manager;

use clock::system_clock;
use config::Config;
use metadata::{Checkpoint, LogRecord, MetadataLog, INODE_FIXED_SIZE};

#[derive(Parser, Debug)]
#[command(name = "bsfs")]
#[command(about = "Backup Storage File System - a userspace FS with cloud archival")]
#[command(after_help = r#"EXAMPLE CONFIG.JSON:
{
    "local_root": "/data/bsfs",
    "gcs_bucket": "my-backup-bucket",
    "gcs_credentials": {
        "type": "service_account_file",
        "path": "/path/to/service-account.json"
    },
    "target_free_space": 10737418240,
    "sweep_interval_secs": 300,
    "version_count": 5,
    "inconsistent_start": false
}

CONFIG FIELDS:
  local_root           (required) Local directory for file storage
  gcs_bucket           (required) GCS bucket name for cloud archival
  gcs_credentials      (optional) Authentication method:
                         - {"type": "ambient"} (default, uses VM metadata)
                         - {"type": "service_account_file", "path": "..."}
                         - {"type": "service_account_key", "key": "..."}
  target_free_space    (optional) Bytes to keep free locally (default: 10GB)
  sweep_interval_secs  (optional) Archival check frequency (default: 300)
  version_count        (optional) Cloud backup versions to keep (default: 5)
  inconsistent_start   (optional) Allow future timestamps on startup (default: false)
  max_storage          (optional) Max storage capacity in bytes (default: 0 = use physical storage)
                         - When 0, statfs returns underlying filesystem stats
                         - When > 0, statfs returns max_storage minus locally stored data
                         - target_free_space must be smaller than max_storage

LIST MODE:
  Use --list <file> to inspect a checkpoint or log file without starting the daemon.
  Prints file header info and contents."#)]
struct Args {
    /// Mount point (required unless --list is used)
    #[arg(short, long, required_unless_present = "list")]
    mount: Option<PathBuf>,

    /// Configuration file path (required unless --list is used)
    #[arg(short, long, required_unless_present = "list")]
    config: Option<PathBuf>,

    /// Run in foreground (don't daemonize)
    #[arg(short, long)]
    foreground: bool,

    /// Enable debug output
    #[arg(short, long)]
    debug: bool,

    /// List contents of a checkpoint or log file (doesn't start daemon)
    #[arg(short, long)]
    list: Option<PathBuf>,
}

/// List contents of a checkpoint or log file
fn list_file(path: &PathBuf) -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::Read;

    // Read first 8 bytes to determine file type
    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;

    if &magic == b"BSFS_CP\0" {
        list_checkpoint(path)
    } else if &magic == b"BSFS_LG\0" {
        list_log(path)
    } else {
        anyhow::bail!(
            "Unknown file format. Magic bytes: {:?} (expected BSFS_CP or BSFS_LG)",
            String::from_utf8_lossy(&magic)
        );
    }
}

/// List contents of a checkpoint file
fn list_checkpoint(path: &PathBuf) -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::Read;

    // Read header manually to display info
    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;

    let mut version_bytes = [0u8; 4];
    file.read_exact(&mut version_bytes)?;
    let version = u32::from_le_bytes(version_bytes);

    let mut inode_size_bytes = [0u8; 4];
    file.read_exact(&mut inode_size_bytes)?;
    let inode_fixed_size = u32::from_le_bytes(inode_size_bytes);

    // Load checkpoint properly
    let checkpoint = Checkpoint::load(path)?;

    // Print header as JSON
    println!(
        "={{\"type\": \"checkpoint\", \"version\": {}, \"inode_fixed_size\": {}, \"current_inode_fixed_size\": {}, \"next_inode\": {}, \"total_inodes\": {}}}",
        version, inode_fixed_size, INODE_FIXED_SIZE, checkpoint.next_inode, checkpoint.inodes.len()
    );

    // Build path map: inode -> full path
    let mut paths: HashMap<u64, String> = HashMap::new();
    paths.insert(1, "/".to_string());

    // Sort inodes to process parents before children
    let mut sorted_inodes: Vec<_> = checkpoint.inodes.values().collect();
    sorted_inodes.sort_by_key(|m| m.inode);

    // Build paths (may need multiple passes for deep trees)
    for _ in 0..100 {
        // max depth protection
        let mut made_progress = false;
        for meta in &sorted_inodes {
            if paths.contains_key(&meta.inode) {
                continue;
            }
            if let Some(parent_path) = paths.get(&meta.parent) {
                let full_path = if parent_path == "/" {
                    format!("/{}", meta.filename)
                } else {
                    format!("{}/{}", parent_path, meta.filename)
                };
                paths.insert(meta.inode, full_path);
                made_progress = true;
            }
        }
        if !made_progress {
            break;
        }
    }

    // Sort by path for nice output
    let mut entries: Vec<_> = checkpoint.inodes.values().collect();
    entries.sort_by(|a, b| {
        let path_a = paths.get(&a.inode).map(|s| s.as_str()).unwrap_or("?");
        let path_b = paths.get(&b.inode).map(|s| s.as_str()).unwrap_or("?");
        path_a.cmp(path_b)
    });

    // Print directory tree: path: storage-key
    for meta in entries {
        let entry_path = paths.get(&meta.inode).map(|s| s.as_str()).unwrap_or("?");
        let storage_key = meta.cloud_key().unwrap_or_else(|| "-".to_string());
        println!("{}: {}", entry_path, storage_key);
    }

    Ok(())
}

/// Format a SystemTime as Unix timestamp (seconds since epoch)
fn format_timestamp(t: &std::time::SystemTime) -> u64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// List contents of a log file
fn list_log(path: &PathBuf) -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::Read;

    // Read header manually to display info
    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;

    let mut version_bytes = [0u8; 4];
    file.read_exact(&mut version_bytes)?;
    let version = u32::from_le_bytes(version_bytes);

    let mut inode_size_bytes = [0u8; 4];
    file.read_exact(&mut inode_size_bytes)?;
    let inode_fixed_size = u32::from_le_bytes(inode_size_bytes);

    // Read all records
    let records = MetadataLog::read_all(path)?;

    // Print header as JSON
    println!(
        "={{\"type\": \"log\", \"version\": {}, \"inode_fixed_size\": {}, \"current_inode_fixed_size\": {}, \"total_records\": {}}}",
        version, inode_fixed_size, INODE_FIXED_SIZE, records.len()
    );

    // Print each record as type: JSON
    for record in &records {
        match record {
            LogRecord::Open { inode, timestamp } => {
                println!(
                    "Open: {{\"inode\": {}, \"timestamp\": {}}}",
                    inode, format_timestamp(timestamp)
                );
            }
            LogRecord::Close {
                inode,
                timestamp,
                was_written,
                new_sha256,
                new_size,
            } => {
                let sha_str = new_sha256
                    .map(|sha| format!("\"{}\"", hex::encode(sha)))
                    .unwrap_or_else(|| "null".to_string());
                println!(
                    "Close: {{\"inode\": {}, \"was_written\": {}, \"new_size\": {}, \"new_sha256\": {}, \"timestamp\": {}}}",
                    inode, was_written, new_size, sha_str, format_timestamp(timestamp)
                );
            }
            LogRecord::Create {
                inode,
                parent,
                filename,
                uid,
                gid,
                mode,
                is_directory,
                timestamp,
            } => {
                println!(
                    "Create: {{\"inode\": {}, \"parent\": {}, \"filename\": \"{}\", \"uid\": {}, \"gid\": {}, \"mode\": {}, \"is_directory\": {}, \"timestamp\": {}}}",
                    inode, parent, filename, uid, gid, mode, is_directory, format_timestamp(timestamp)
                );
            }
            LogRecord::Delete {
                inode,
                parent,
                timestamp,
            } => {
                println!(
                    "Delete: {{\"inode\": {}, \"parent\": {}, \"timestamp\": {}}}",
                    inode, parent, format_timestamp(timestamp)
                );
            }
            LogRecord::Rename {
                inode,
                old_parent,
                new_parent,
                new_name,
                timestamp,
            } => {
                println!(
                    "Rename: {{\"inode\": {}, \"old_parent\": {}, \"new_parent\": {}, \"new_name\": \"{}\", \"timestamp\": {}}}",
                    inode, old_parent, new_parent, new_name, format_timestamp(timestamp)
                );
            }
            LogRecord::SetAttr {
                inode,
                uid,
                gid,
                mode,
                size,
                timestamp,
            } => {
                let uid_str = uid.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string());
                let gid_str = gid.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string());
                let mode_str = mode.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string());
                let size_str = size.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string());
                println!(
                    "SetAttr: {{\"inode\": {}, \"uid\": {}, \"gid\": {}, \"mode\": {}, \"size\": {}, \"timestamp\": {}}}",
                    inode, uid_str, gid_str, mode_str, size_str, format_timestamp(timestamp)
                );
            }
            LogRecord::Purge {
                key,
                inode,
                timestamp,
            } => {
                println!(
                    "Purge: {{\"key\": \"{}\", \"inode\": {}, \"timestamp\": {}}}",
                    key, inode, format_timestamp(timestamp)
                );
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Handle --list mode (no daemon)
    if let Some(list_path) = &args.list {
        return list_file(list_path);
    }

    // Normal daemon mode - mount and config are required
    let mount = args.mount.expect("mount required when not using --list");
    let config_path = args.config.expect("config required when not using --list");

    // Initialize logging
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(if args.debug {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    tracing::info!("Starting BSFS");

    // Load configuration
    let config = Config::from_file(&config_path)?;
    tracing::info!("Loaded configuration from {:?}", config_path);

    // Ensure directories exist
    std::fs::create_dir_all(&config.local_root)?;
    std::fs::create_dir_all(config.data_root())?;
    std::fs::create_dir_all(&mount)?;

    // Create clock
    let clock = system_clock();

    // Recover state
    tracing::info!("Recovering filesystem state...");
    let state = recovery::recover(clock.as_ref(), &config)?;
    tracing::info!(
        "Recovery complete: {} inodes",
        state.checkpoint.inodes.len()
    );

    // Initialize cloud storage
    tracing::info!("Initializing cloud storage...");
    let cloud = cloud::GcsStorage::from_config(&config, clock.clone()).await?;
    let cloud = Arc::new(cloud);

    // Create filesystem
    let runtime = tokio::runtime::Handle::current();
    let checkpoint = Arc::new(std::sync::RwLock::new(state.checkpoint));
    let log = Arc::new(std::sync::RwLock::new(state.log));

    // Create upload manager for immediate background uploads
    let (upload_manager, upload_tx) = upload_manager::UploadManager::new(
        config.clone(),
        clock.clone(),
        checkpoint.clone(),
        log.clone(),
        cloud.clone(),
    );
    let upload_handle = tokio::spawn(upload_manager.run());

    // Create sweeper (need sweep_tx for filesystem)
    let (sweeper, sweep_tx) = sweeper::Sweeper::new(
        config.clone(),
        clock.clone(),
        checkpoint.clone(),
        log.clone(),
        cloud.clone(),
    );
    let sweeper_handle = tokio::spawn(sweeper.run());

    let filesystem = fs::BsfsFilesystem::new(
        config.clone(),
        clock,
        checkpoint,
        log,
        cloud,
        runtime.clone(),
        upload_tx,
        sweep_tx,
    );

    // Mount options
    let options = vec![
        MountOption::FSName("bsfs".to_string()),
        MountOption::AutoUnmount,
        MountOption::AllowOther,
    ];

    if !args.foreground {
        // Note: actual daemonization would require more work
        // For now we just run in foreground
        tracing::warn!("Daemon mode not fully implemented, running in foreground");
    }

    tracing::info!("Mounting filesystem at {:?}", mount);

    // Set up signal handler for clean shutdown
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
        tracing::info!("Received shutdown signal");
        // Filesystem will be unmounted when dropped
    });

    // Mount the filesystem (blocks until unmounted)
    fuser::mount2(filesystem, &mount, &options)?;

    tracing::info!("Filesystem unmounted");

    // Cancel background tasks
    upload_handle.abort();
    sweeper_handle.abort();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Clock, MockClock};
    use crate::metadata::InodeMetadata;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_list_checkpoint() {
        let dir = tempdir().unwrap();
        let cp_path = dir.path().join("test_checkpoint");

        // Create a checkpoint with some structure
        let clock = MockClock::default();
        let mut cp = Checkpoint::new(&clock, 1000, 1000);

        // Add a directory
        let docs = InodeMetadata::new_directory(&clock, 2, "docs".into(), 1, 1000, 1000, 0o755);
        cp.insert(docs);
        if let Some(root) = cp.get_mut(1) {
            root.children.push(2);
        }

        // Add a file in root
        let mut readme = InodeMetadata::new_file(&clock, 3, "readme.txt".into(), 1, 1000, 1000, 0o644);
        readme.size = 1024;
        readme.checkpointed_sha256 = Some([0xab; 32]);
        cp.insert(readme);
        if let Some(root) = cp.get_mut(1) {
            root.children.push(3);
        }

        // Add a file in docs/
        let notes = InodeMetadata::new_file(&clock, 4, "notes.md".into(), 2, 1000, 1000, 0o644);
        cp.insert(notes);
        if let Some(docs) = cp.get_mut(2) {
            docs.children.push(4);
        }

        cp.next_inode = 5;
        cp.save(&cp_path).unwrap();

        // Test listing
        let result = list_checkpoint(&cp_path.to_path_buf());
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_log() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test_log");

        let clock = MockClock::default();

        // Create a log with some records
        {
            let mut log = MetadataLog::open(&log_path).unwrap();

            log.append(&LogRecord::Create {
                inode: 2,
                parent: 1,
                filename: "test.txt".into(),
                uid: 1000,
                gid: 1000,
                mode: 0o644,
                is_directory: false,
                timestamp: clock.now(),
            })
            .unwrap();

            clock.advance(Duration::from_secs(10));

            log.append(&LogRecord::Open {
                inode: 2,
                timestamp: clock.now(),
            })
            .unwrap();

            clock.advance(Duration::from_secs(5));

            log.append(&LogRecord::Close {
                inode: 2,
                timestamp: clock.now(),
                was_written: true,
                new_sha256: Some([0xcd; 32]),
                new_size: 256,
            })
            .unwrap();
        }

        // Test listing
        let result = list_log(&log_path.to_path_buf());
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_file_detection() {
        let dir = tempdir().unwrap();
        let clock = MockClock::default();

        // Test checkpoint detection
        let cp_path = dir.path().join("checkpoint");
        let cp = Checkpoint::new(&clock, 1000, 1000);
        cp.save(&cp_path).unwrap();
        assert!(list_file(&cp_path.to_path_buf()).is_ok());

        // Test log detection
        let log_path = dir.path().join("log");
        let _log = MetadataLog::open(&log_path).unwrap();
        assert!(list_file(&log_path.to_path_buf()).is_ok());

        // Test unknown file
        let bad_path = dir.path().join("bad");
        std::fs::write(&bad_path, b"not a bsfs file").unwrap();
        assert!(list_file(&bad_path.to_path_buf()).is_err());
    }
}
