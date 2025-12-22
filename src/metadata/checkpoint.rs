use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::clock::Clock;
use crate::error::{BsfsError, Result};
use crate::metadata::{InodeMetadata, INODE_FIXED_SIZE};

const CHECKPOINT_MAGIC: &[u8; 8] = b"BSFS_CP\0";
const CHECKPOINT_VERSION: u32 = 2; // Bumped for inode_fixed_size field

/// Manages the checkpoint file containing full filesystem state
pub struct Checkpoint {
    pub inodes: HashMap<u64, InodeMetadata>,
    pub next_inode: u64,
}

impl Checkpoint {
    /// Create a new empty checkpoint with just the root directory
    pub fn new(clock: &dyn Clock, uid: u32, gid: u32) -> Self {
        let mut inodes = HashMap::new();
        let root = InodeMetadata::new_root(clock, uid, gid);
        inodes.insert(1, root);
        Self {
            inodes,
            next_inode: 2,
        }
    }

    /// Load checkpoint from file
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Err(BsfsError::FileNotFound(path.display().to_string()));
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read and verify magic
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != CHECKPOINT_MAGIC {
            return Err(BsfsError::Recovery("Invalid checkpoint magic".into()));
        }

        // Read version
        let mut version_bytes = [0u8; 4];
        reader.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);
        if version != CHECKPOINT_VERSION {
            return Err(BsfsError::Recovery(format!(
                "Unsupported checkpoint version: {} (expected {})",
                version, CHECKPOINT_VERSION
            )));
        }

        // Read and verify inode_fixed_size
        let mut inode_size_bytes = [0u8; 4];
        reader.read_exact(&mut inode_size_bytes)?;
        let inode_fixed_size = u32::from_le_bytes(inode_size_bytes);
        if inode_fixed_size != INODE_FIXED_SIZE {
            return Err(BsfsError::Recovery(format!(
                "Incompatible inode format: file has fixed_size={}, expected {}",
                inode_fixed_size, INODE_FIXED_SIZE
            )));
        }

        // Read next_inode
        let mut next_inode_bytes = [0u8; 8];
        reader.read_exact(&mut next_inode_bytes)?;
        let next_inode = u64::from_le_bytes(next_inode_bytes);

        // Read inode count
        let mut count_bytes = [0u8; 8];
        reader.read_exact(&mut count_bytes)?;
        let count = u64::from_le_bytes(count_bytes);

        // Read all inodes
        let mut inodes = HashMap::with_capacity(count as usize);
        for _ in 0..count {
            // Read entry length
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;

            // Read entry data with CRC
            let mut entry_data = vec![0u8; len];
            reader.read_exact(&mut entry_data)?;

            let metadata = InodeMetadata::from_bytes_with_crc(&entry_data)?;
            inodes.insert(metadata.inode, metadata);
        }

        Ok(Self { inodes, next_inode })
    }

    /// Save checkpoint to file atomically (write to temp, then rename)
    pub fn save(&self, path: &Path) -> Result<()> {
        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Write to temporary file first
        let temp_path = path.with_extension("tmp");
        {
            let file = File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);

            // Write magic
            writer.write_all(CHECKPOINT_MAGIC)?;

            // Write version
            writer.write_all(&CHECKPOINT_VERSION.to_le_bytes())?;

            // Write inode_fixed_size for compatibility checking
            writer.write_all(&INODE_FIXED_SIZE.to_le_bytes())?;

            // Write next_inode
            writer.write_all(&self.next_inode.to_le_bytes())?;

            // Write inode count
            writer.write_all(&(self.inodes.len() as u64).to_le_bytes())?;

            // Write each inode
            for metadata in self.inodes.values() {
                let entry_data = metadata.to_bytes_with_crc()?;
                writer.write_all(&(entry_data.len() as u32).to_le_bytes())?;
                writer.write_all(&entry_data)?;
            }

            writer.flush()?;
        }

        // Atomically rename temp file to target
        fs::rename(&temp_path, path)?;

        Ok(())
    }

    /// Allocate a new inode number
    pub fn allocate_inode(&mut self) -> u64 {
        let inode = self.next_inode;
        self.next_inode += 1;
        inode
    }

    /// Get metadata for an inode
    pub fn get(&self, inode: u64) -> Option<&InodeMetadata> {
        self.inodes.get(&inode)
    }

    /// Get mutable metadata for an inode
    pub fn get_mut(&mut self, inode: u64) -> Option<&mut InodeMetadata> {
        self.inodes.get_mut(&inode)
    }

    /// Insert or update metadata
    pub fn insert(&mut self, metadata: InodeMetadata) {
        self.inodes.insert(metadata.inode, metadata);
    }

    /// Remove an inode
    pub fn remove(&mut self, inode: u64) -> Option<InodeMetadata> {
        self.inodes.remove(&inode)
    }

    /// Look up a child by name in a directory
    pub fn lookup_child(&self, parent: u64, name: &str) -> Option<&InodeMetadata> {
        let parent_meta = self.inodes.get(&parent)?;
        for &child_inode in &parent_meta.children {
            if let Some(child_meta) = self.inodes.get(&child_inode) {
                if child_meta.filename == name {
                    return Some(child_meta);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use tempfile::tempdir;

    #[test]
    fn test_new_checkpoint() {
        let clock = MockClock::default();
        let cp = Checkpoint::new(&clock, 1000, 1000);
        assert!(cp.inodes.contains_key(&1));
        assert_eq!(cp.next_inode, 2);
        assert_eq!(cp.inodes.get(&1).unwrap().created_time, clock.now());
    }

    #[test]
    fn test_save_and_load() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint");
        let clock = MockClock::default();

        let mut cp = Checkpoint::new(&clock, 1000, 1000);
        let file = InodeMetadata::new_file(
            &clock,
            cp.allocate_inode(),
            "test.txt".into(),
            1,
            1000,
            1000,
            0o644,
        );
        cp.insert(file);
        cp.save(&path).unwrap();

        let loaded = Checkpoint::load(&path).unwrap();
        assert_eq!(loaded.inodes.len(), 2);
        assert_eq!(loaded.next_inode, 3);
        assert!(loaded.inodes.contains_key(&1));
        assert!(loaded.inodes.contains_key(&2));
        // Verify timestamps are preserved
        assert_eq!(loaded.inodes.get(&1).unwrap().created_time, clock.now());
    }

    #[test]
    fn test_allocate_inode() {
        let clock = MockClock::default();
        let mut cp = Checkpoint::new(&clock, 0, 0);
        assert_eq!(cp.allocate_inode(), 2);
        assert_eq!(cp.allocate_inode(), 3);
        assert_eq!(cp.allocate_inode(), 4);
    }
}
