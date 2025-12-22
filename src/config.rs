use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::error::{BsfsError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Root directory for local file storage
    pub local_root: PathBuf,

    /// GCS bucket name for cloud storage
    pub gcs_bucket: String,

    /// GCS credentials configuration
    #[serde(default)]
    pub gcs_credentials: GcsCredentials,

    /// Target free space in bytes to maintain on local storage
    #[serde(default = "default_target_free_space")]
    pub target_free_space: u64,

    /// Frequency of sweeping files for archival, in seconds
    #[serde(default = "default_sweep_interval")]
    pub sweep_interval_secs: u64,

    /// Number of backup versions to keep in cloud storage
    #[serde(default = "default_version_count")]
    pub version_count: u32,

    /// If true, adjust timestamps that are in the future on startup
    /// instead of refusing to start
    #[serde(default)]
    pub inconsistent_start: bool,

    /// Maximum storage capacity in bytes (0 = use underlying physical storage)
    #[serde(default)]
    pub max_storage: u64,

    /// Maximum number of parallel uploads to cloud storage
    #[serde(default = "default_parallel_upload")]
    pub parallel_upload: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GcsCredentials {
    /// Use ambient credentials (e.g., from GCP VM metadata)
    #[default]
    Ambient,
    /// Use a service account JSON key file
    ServiceAccountFile { path: PathBuf },
    /// Use inline service account JSON key
    ServiceAccountKey { key: String },
}

fn default_target_free_space() -> u64 {
    10 * 1024 * 1024 * 1024 // 10 GB
}

fn default_sweep_interval() -> u64 {
    300 // 5 minutes
}

fn default_version_count() -> u32 {
    5
}

fn default_parallel_upload() -> u32 {
    8
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::from_json(&content)
    }

    pub fn from_json(json: &str) -> Result<Self> {
        let config: Config = serde_json::from_str(json)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.gcs_bucket.is_empty() {
            return Err(BsfsError::Config("gcs_bucket cannot be empty".into()));
        }
        if self.version_count == 0 {
            return Err(BsfsError::Config("version_count must be at least 1".into()));
        }
        if self.sweep_interval_secs == 0 {
            return Err(BsfsError::Config(
                "sweep_interval_secs must be at least 1".into(),
            ));
        }
        if self.parallel_upload == 0 {
            return Err(BsfsError::Config(
                "parallel_upload must be at least 1".into(),
            ));
        }

        // Validate credentials path if specified
        if let GcsCredentials::ServiceAccountFile { path } = &self.gcs_credentials {
            if !path.exists() {
                return Err(BsfsError::Config(format!(
                    "Service account file not found: {}",
                    path.display()
                )));
            }
        }

        // Validate target_free_space is smaller than max_storage when max_storage > 0
        if self.max_storage > 0 && self.target_free_space >= self.max_storage {
            return Err(BsfsError::Config(
                "target_free_space must be smaller than max_storage".into(),
            ));
        }

        Ok(())
    }

    /// Get the path to the checkpoint file
    pub fn checkpoint_path(&self) -> PathBuf {
        self.local_root.join("bsfs_checkpoint")
    }

    /// Get the path to the metadata log file
    pub fn log_path(&self) -> PathBuf {
        self.local_root.join("bsfs_log")
    }

    /// Get the path to the data directory (where inode data files are stored)
    pub fn data_root(&self) -> PathBuf {
        self.local_root.join("data")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let json = r#"{
            "local_root": "/tmp/bsfs",
            "gcs_bucket": "my-bucket"
        }"#;
        let config = Config::from_json(json).unwrap();
        assert_eq!(config.local_root, PathBuf::from("/tmp/bsfs"));
        assert_eq!(config.gcs_bucket, "my-bucket");
        assert_eq!(config.target_free_space, 10 * 1024 * 1024 * 1024);
        assert_eq!(config.sweep_interval_secs, 300);
        assert_eq!(config.version_count, 5);
        assert!(!config.inconsistent_start);
        assert_eq!(config.max_storage, 0);
        assert_eq!(config.parallel_upload, 8);
    }

    #[test]
    fn test_parse_full_config() {
        let json = r#"{
            "local_root": "/data/bsfs",
            "gcs_bucket": "backup-bucket",
            "gcs_credentials": {
                "type": "service_account_file",
                "path": "/etc/gcs-key.json"
            },
            "target_free_space": 5368709120,
            "sweep_interval_secs": 600,
            "version_count": 10,
            "inconsistent_start": true
        }"#;
        // This will fail validation because the key file doesn't exist,
        // but we can test the parsing separately
        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.local_root, PathBuf::from("/data/bsfs"));
        assert_eq!(config.gcs_bucket, "backup-bucket");
        assert_eq!(config.target_free_space, 5368709120);
        assert_eq!(config.sweep_interval_secs, 600);
        assert_eq!(config.version_count, 10);
        assert!(config.inconsistent_start);
    }

    #[test]
    fn test_empty_bucket_fails() {
        let json = r#"{
            "local_root": "/tmp/bsfs",
            "gcs_bucket": ""
        }"#;
        let result = Config::from_json(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_version_count_fails() {
        let json = r#"{
            "local_root": "/tmp/bsfs",
            "gcs_bucket": "bucket",
            "version_count": 0
        }"#;
        let result = Config::from_json(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_parallel_upload_fails() {
        let json = r#"{
            "local_root": "/tmp/bsfs",
            "gcs_bucket": "bucket",
            "parallel_upload": 0
        }"#;
        let result = Config::from_json(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_custom_parallel_upload() {
        let json = r#"{
            "local_root": "/tmp/bsfs",
            "gcs_bucket": "bucket",
            "parallel_upload": 16
        }"#;
        let config = Config::from_json(json).unwrap();
        assert_eq!(config.parallel_upload, 16);
    }

    #[test]
    fn test_max_storage_with_valid_target_free_space() {
        let json = r#"{
            "local_root": "/tmp/bsfs",
            "gcs_bucket": "bucket",
            "max_storage": 107374182400,
            "target_free_space": 10737418240
        }"#;
        let config = Config::from_json(json).unwrap();
        assert_eq!(config.max_storage, 107374182400);
        assert_eq!(config.target_free_space, 10737418240);
    }

    #[test]
    fn test_target_free_space_equals_max_storage_fails() {
        let json = r#"{
            "local_root": "/tmp/bsfs",
            "gcs_bucket": "bucket",
            "max_storage": 10737418240,
            "target_free_space": 10737418240
        }"#;
        let result = Config::from_json(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_target_free_space_exceeds_max_storage_fails() {
        let json = r#"{
            "local_root": "/tmp/bsfs",
            "gcs_bucket": "bucket",
            "max_storage": 5368709120,
            "target_free_space": 10737418240
        }"#;
        let result = Config::from_json(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_max_storage_zero_allows_any_target_free_space() {
        let json = r#"{
            "local_root": "/tmp/bsfs",
            "gcs_bucket": "bucket",
            "max_storage": 0,
            "target_free_space": 10737418240
        }"#;
        // When max_storage is 0, target_free_space can be anything
        let config = Config::from_json(json).unwrap();
        assert_eq!(config.max_storage, 0);
        assert_eq!(config.target_free_space, 10737418240);
    }
}
