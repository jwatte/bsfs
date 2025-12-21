use clap::Parser;
use fuser::MountOption;
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

use clock::system_clock;
use config::Config;

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
                         - target_free_space must be smaller than max_storage"#)]
struct Args {
    /// Mount point
    #[arg(short, long)]
    mount: PathBuf,

    /// Configuration file path
    #[arg(short, long)]
    config: PathBuf,

    /// Run in foreground (don't daemonize)
    #[arg(short, long)]
    foreground: bool,

    /// Enable debug output
    #[arg(short, long)]
    debug: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

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
    let config = Config::from_file(&args.config)?;
    tracing::info!("Loaded configuration from {:?}", args.config);

    // Ensure directories exist
    std::fs::create_dir_all(&config.local_root)?;
    std::fs::create_dir_all(config.data_root())?;
    std::fs::create_dir_all(&args.mount)?;

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

    let filesystem = fs::BsfsFilesystem::new(
        config.clone(),
        clock.clone(),
        checkpoint.clone(),
        log.clone(),
        cloud.clone(),
        runtime.clone(),
    );

    // Start sweeper task
    let sweeper = sweeper::Sweeper::new(
        config.clone(),
        clock,
        checkpoint,
        log,
        cloud,
    );
    let sweeper_handle = tokio::spawn(sweeper.run());

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

    tracing::info!("Mounting filesystem at {:?}", args.mount);

    // Set up signal handler for clean shutdown
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
        tracing::info!("Received shutdown signal");
        // Filesystem will be unmounted when dropped
    });

    // Mount the filesystem (blocks until unmounted)
    fuser::mount2(filesystem, &args.mount, &options)?;

    tracing::info!("Filesystem unmounted");

    // Cancel sweeper
    sweeper_handle.abort();

    Ok(())
}
