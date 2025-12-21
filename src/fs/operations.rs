use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::cloud::CloudStorage;
use crate::error::Result;
use crate::fs::path::inode_data_path;

/// Read data from a local file
pub fn read_local(data_root: &Path, inode: u64, offset: u64, size: u32) -> Result<Vec<u8>> {
    let path = inode_data_path(data_root, inode);
    let mut file = File::open(&path)?;
    file.seek(SeekFrom::Start(offset))?;

    let mut buffer = vec![0u8; size as usize];
    let bytes_read = file.read(&mut buffer)?;
    buffer.truncate(bytes_read);

    Ok(buffer)
}

/// Write data to a local file
pub fn write_local(data_root: &Path, inode: u64, offset: u64, data: &[u8]) -> Result<u64> {
    let path = inode_data_path(data_root, inode);

    // Ensure parent directories exist
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&path)?;

    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)?;

    // Get final file size
    let metadata = file.metadata()?;
    Ok(metadata.len())
}

/// Create a new empty file on disk
pub fn create_local_file(data_root: &Path, inode: u64) -> Result<()> {
    let path = inode_data_path(data_root, inode);

    // Ensure parent directories exist
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    File::create(&path)?;
    Ok(())
}

/// Delete a local file
pub fn delete_local_file(data_root: &Path, inode: u64) -> Result<()> {
    let path = inode_data_path(data_root, inode);
    if path.exists() {
        fs::remove_file(&path)?;
    }
    Ok(())
}

/// Get the size of a local file
pub fn get_local_size(data_root: &Path, inode: u64) -> Result<u64> {
    let path = inode_data_path(data_root, inode);
    let metadata = fs::metadata(&path)?;
    Ok(metadata.len())
}

/// Check if local data exists for an inode
pub fn local_data_exists(data_root: &Path, inode: u64) -> bool {
    let path = inode_data_path(data_root, inode);
    path.exists()
}

/// Compute SHA256 of a local file
pub fn compute_sha256(data_root: &Path, inode: u64) -> Result<[u8; 32]> {
    let path = inode_data_path(data_root, inode);
    let mut file = File::open(&path)?;
    let mut hasher = Sha256::new();

    let mut buffer = [0u8; 8192];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    let result = hasher.finalize();
    let mut sha256 = [0u8; 32];
    sha256.copy_from_slice(&result);
    Ok(sha256)
}

/// Truncate a local file to a specific size
pub fn truncate_local(data_root: &Path, inode: u64, size: u64) -> Result<()> {
    let path = inode_data_path(data_root, inode);

    // Ensure parent directories exist
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&path)?;
    file.set_len(size)?;
    Ok(())
}

/// Fetch a file from cloud storage to local disk
pub async fn fetch_from_cloud<C: CloudStorage>(
    cloud: &C,
    data_root: &Path,
    inode: u64,
    version_id: Option<&str>,
) -> Result<()> {
    let data = cloud.download(inode, version_id).await?;
    let path = inode_data_path(data_root, inode);

    // Ensure parent directories exist
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = File::create(&path)?;
    file.write_all(&data)?;

    Ok(())
}

/// Upload a local file to cloud storage
pub async fn upload_to_cloud<C: CloudStorage>(
    cloud: &C,
    data_root: &Path,
    inode: u64,
) -> Result<(String, [u8; 32])> {
    let path = inode_data_path(data_root, inode);
    let data = fs::read(&path)?;
    let sha256 = compute_sha256(data_root, inode)?;

    let version_id = cloud.upload(inode, &data, sha256).await?;

    Ok((version_id, sha256))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_and_write() {
        let dir = tempdir().unwrap();
        let data_root = dir.path();

        create_local_file(data_root, 42).unwrap();
        write_local(data_root, 42, 0, b"hello").unwrap();

        let data = read_local(data_root, 42, 0, 100).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn test_write_at_offset() {
        let dir = tempdir().unwrap();
        let data_root = dir.path();

        create_local_file(data_root, 1).unwrap();
        write_local(data_root, 1, 0, b"hello").unwrap();
        write_local(data_root, 1, 5, b" world").unwrap();

        let data = read_local(data_root, 1, 0, 100).unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn test_sha256() {
        let dir = tempdir().unwrap();
        let data_root = dir.path();

        create_local_file(data_root, 1).unwrap();
        write_local(data_root, 1, 0, b"test data").unwrap();

        let sha = compute_sha256(data_root, 1).unwrap();
        // SHA256 of "test data"
        assert_eq!(
            hex::encode(sha),
            "916f0027a575074ce72a331777c3478d6513f786a591bd892da1a577bf2335f9"
        );
    }

    #[test]
    fn test_truncate() {
        let dir = tempdir().unwrap();
        let data_root = dir.path();

        create_local_file(data_root, 1).unwrap();
        write_local(data_root, 1, 0, b"hello world").unwrap();
        truncate_local(data_root, 1, 5).unwrap();

        let size = get_local_size(data_root, 1).unwrap();
        assert_eq!(size, 5);
    }
}
