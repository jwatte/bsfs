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

    /// Get the object name for an inode version
    fn object_name(inode: u64, version: &str) -> String {
        format!("inodes/{}/{}", inode, version)
    }

    /// Get the prefix for all versions of an inode
    fn inode_prefix(inode: u64) -> String {
        format!("inodes/{}/", inode)
    }

    /// Parse version ID from object name
    fn parse_version(object_name: &str) -> Option<String> {
        object_name.rsplit('/').next().map(String::from)
    }
}

#[async_trait]
impl CloudStorage for GcsStorage {
    async fn upload(&self, inode: u64, data: &[u8], sha256: [u8; 32]) -> Result<String> {
        // Use timestamp as version ID
        let version_id = self
            .clock
            .now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string();

        let object_name = Self::object_name(inode, &version_id);

        // Store SHA256 as custom metadata (TODO: add to object metadata)
        let _sha256_hex = hex::encode(sha256);

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

    async fn download(&self, inode: u64, version_id: Option<&str>) -> Result<Vec<u8>> {
        let object_name = match version_id {
            Some(vid) => Self::object_name(inode, vid),
            None => {
                // Get latest version
                let versions = self.list_versions(inode).await?;
                let latest = versions
                    .last()
                    .ok_or_else(|| BsfsError::FileNotFound(format!("inode {}", inode)))?;
                Self::object_name(inode, &latest.version_id)
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

    async fn list_versions(&self, inode: u64) -> Result<Vec<VersionInfo>> {
        let prefix = Self::inode_prefix(inode);

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

    async fn delete_old_versions(&self, inode: u64, keep_count: u32) -> Result<()> {
        let versions = self.list_versions(inode).await?;

        if versions.len() <= keep_count as usize {
            return Ok(());
        }

        let to_delete = &versions[..versions.len() - keep_count as usize];

        for version in to_delete {
            let object_name = Self::object_name(inode, &version.version_id);
            self.client
                .delete_object(&DeleteObjectRequest {
                    bucket: self.bucket.clone(),
                    object: object_name,
                    ..Default::default()
                })
                .await
                .map_err(|e| BsfsError::CloudStorage(e.to_string()))?;
        }

        Ok(())
    }

}
