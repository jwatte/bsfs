use async_trait::async_trait;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::delete::DeleteObjectRequest;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::list::ListObjectsRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use std::time::SystemTime;

use crate::clock::SharedClock;
use crate::cloud::traits::{CloudStorage, VersionInfo};
use crate::config::{Config, GcsCredentials};
use crate::error::{BsfsError, Result};

/// Google Cloud Storage implementation
pub struct GcsStorage {
    client: Client,
    bucket: String,
    clock: SharedClock,
}

impl GcsStorage {
    /// Create a new GCS storage client from config
    pub async fn from_config(config: &Config, clock: SharedClock) -> Result<Self> {
        let client_config = match &config.gcs_credentials {
            GcsCredentials::Ambient => {
                ClientConfig::default()
                    .with_auth()
                    .await
                    .map_err(|e| BsfsError::CloudStorage(e.to_string()))?
            }
            GcsCredentials::ServiceAccountFile { path } => {
                let key_json = std::fs::read_to_string(path)?;
                ClientConfig::default()
                    .with_credentials(
                        google_cloud_storage::client::google_cloud_auth::credentials::CredentialsFile::new_from_str(&key_json)
                            .await
                            .map_err(|e| BsfsError::CloudStorage(e.to_string()))?,
                    )
                    .await
                    .map_err(|e| BsfsError::CloudStorage(e.to_string()))?
            }
            GcsCredentials::ServiceAccountKey { key } => {
                ClientConfig::default()
                    .with_credentials(
                        google_cloud_storage::client::google_cloud_auth::credentials::CredentialsFile::new_from_str(key)
                            .await
                            .map_err(|e| BsfsError::CloudStorage(e.to_string()))?,
                    )
                    .await
                    .map_err(|e| BsfsError::CloudStorage(e.to_string()))?
            }
        };

        let client = Client::new(client_config);

        Ok(Self {
            client,
            bucket: config.gcs_bucket.clone(),
            clock,
        })
    }

    /// Generate object key from SHA256 and filename
    /// Format: `{sha256[0:2]}/{sha256[0:4]}/{sha256}.{filename}`
    fn object_key(sha256: [u8; 32], filename: &str) -> String {
        let hex = hex::encode(sha256);
        format!("{}/{}/{}.{}", &hex[0..2], &hex[0..4], hex, filename)
    }

    /// Parse version ID from object name (returns the full key)
    #[allow(dead_code)]
    fn parse_version(object_name: &str) -> Option<String> {
        Some(object_name.to_string())
    }

    /// Get the object name for a checkpoint version
    fn checkpoint_object_name(version: &str) -> String {
        format!("checkpoints/{}", version)
    }

    /// Get the prefix for all checkpoint versions
    #[allow(dead_code)]
    fn checkpoint_prefix() -> &'static str {
        "checkpoints/"
    }
}

#[async_trait]
impl CloudStorage for GcsStorage {
    async fn upload(&self, _inode: u64, data: &[u8], sha256: [u8; 32], filename: &str) -> Result<String> {
        let object_name = Self::object_key(sha256, filename);

        let upload_type = UploadType::Simple(Media::new(object_name.clone()));
        let req = UploadObjectRequest {
            bucket: self.bucket.clone(),
            ..Default::default()
        };

        self.client
            .upload_object(&req, data.to_vec(), &upload_type)
            .await
            .map_err(|e| BsfsError::CloudStorage(e.to_string()))?;

        Ok(object_name)
    }

    async fn download(&self, _inode: u64, version_id: Option<&str>) -> Result<Vec<u8>> {
        let object_name = version_id
            .ok_or_else(|| BsfsError::FileNotFound("version_id required for download".to_string()))?;

        let data = self
            .client
            .download_object(
                &GetObjectRequest {
                    bucket: self.bucket.clone(),
                    object: object_name.to_string(),
                    ..Default::default()
                },
                &Range::default(),
            )
            .await
            .map_err(|e| BsfsError::CloudStorage(e.to_string()))?;

        Ok(data)
    }

    async fn download_by_hash(&self, sha256: [u8; 32], filename: &str) -> Result<Vec<u8>> {
        let object_name = Self::object_key(sha256, filename);

        let data = self
            .client
            .download_object(
                &GetObjectRequest {
                    bucket: self.bucket.clone(),
                    object: object_name,
                    ..Default::default()
                },
                &Range::default(),
            )
            .await
            .map_err(|e| BsfsError::CloudStorage(e.to_string()))?;

        Ok(data)
    }

    async fn list_versions(&self, _inode: u64) -> Result<Vec<VersionInfo>> {
        // With SHA256-based keys, versions must be tracked in metadata
        // This method is deprecated - use metadata to track version history
        Ok(Vec::new())
    }

    async fn delete_old_versions(&self, _inode: u64, _keep_count: u32) -> Result<()> {
        // With SHA256-based keys, version cleanup must be done via metadata
        // which tracks the keys. Use delete_by_key to remove specific versions.
        Ok(())
    }

    async fn delete_by_key(&self, key: &str) -> Result<()> {
        self.client
            .delete_object(&DeleteObjectRequest {
                bucket: self.bucket.clone(),
                object: key.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| BsfsError::CloudStorage(e.to_string()))?;

        Ok(())
    }

    async fn upload_checkpoint(&self, data: &[u8]) -> Result<String> {
        // Use timestamp as version ID
        let version_id = self
            .clock
            .now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string();

        let object_name = Self::checkpoint_object_name(&version_id);

        let upload_type = UploadType::Simple(Media::new(object_name.clone()));
        let req = UploadObjectRequest {
            bucket: self.bucket.clone(),
            ..Default::default()
        };

        self.client
            .upload_object(&req, data.to_vec(), &upload_type)
            .await
            .map_err(|e| BsfsError::CloudStorage(e.to_string()))?;

        Ok(version_id)
    }

    async fn download_checkpoint(&self, version_id: Option<&str>) -> Result<Vec<u8>> {
        let object_name = match version_id {
            Some(vid) => Self::checkpoint_object_name(vid),
            None => {
                // Get latest version
                let versions = self.list_checkpoint_versions().await?;
                let latest = versions
                    .last()
                    .ok_or_else(|| BsfsError::FileNotFound("checkpoint".to_string()))?;
                Self::checkpoint_object_name(&latest.version_id)
            }
        };

        let data = self
            .client
            .download_object(
                &GetObjectRequest {
                    bucket: self.bucket.clone(),
                    object: object_name,
                    ..Default::default()
                },
                &Range::default(),
            )
            .await
            .map_err(|e| BsfsError::CloudStorage(e.to_string()))?;

        Ok(data)
    }

    async fn list_checkpoint_versions(&self) -> Result<Vec<VersionInfo>> {
        let prefix = Self::checkpoint_prefix().to_string();

        let objects = self
            .client
            .list_objects(&ListObjectsRequest {
                bucket: self.bucket.clone(),
                prefix: Some(prefix),
                ..Default::default()
            })
            .await
            .map_err(|e| BsfsError::CloudStorage(e.to_string()))?;

        let mut versions: Vec<VersionInfo> = objects
            .items
            .unwrap_or_default()
            .into_iter()
            .filter_map(|obj| {
                let version_id = Self::parse_version(&obj.name)?;
                Some(VersionInfo { version_id })
            })
            .collect();

        // Sort by version ID (timestamp)
        versions.sort_by(|a, b| a.version_id.cmp(&b.version_id));

        Ok(versions)
    }
}
