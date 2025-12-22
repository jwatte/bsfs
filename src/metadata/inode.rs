use serde::{Deserialize, Serialize};
use std::time::SystemTime;

use crate::clock::Clock;

/// Minimum serialized size of InodeMetadata when all variable-length fields are empty.
/// This is used as a compatibility marker in file headers.
///
/// Bincode serialization sizes:
///   inode: u64 (8) + filename: String empty (8) + parent: u64 (8) +
///   file_type: u32 (4) + permissions: u16 (2) + uid: u32 (4) + gid: u32 (4) + size: u64 (8) +
///   created_time (12) + mutated_time (12) + accessed_time (12) + closed_time (12) +
///   unix_atime (12) + unix_mtime (12) + unix_ctime (12) +
///   checkpointed_time: Option None (1) + checkpointed_sha256: Option None (1) +
///   sha_history: Vec empty (8) + local_data_present: bool (1) + children: Vec empty (8)
///
/// Note: SystemTime is serialized by bincode as (secs: u64, nanos: u32) = 12 bytes
/// Note: Empty String/Vec has 8-byte length prefix, Option None is 1 byte
///
/// If this value doesn't match the actual serialized size, see test_inode_fixed_size_matches_serialized
pub const INODE_FIXED_SIZE: u32 = 149;

/// File type for an inode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    RegularFile,
    Directory,
}

impl FileType {
    #[allow(dead_code)]
    pub fn to_libc_type(self) -> u32 {
        match self {
            FileType::RegularFile => libc::S_IFREG as u32,
            FileType::Directory => libc::S_IFDIR as u32,
        }
    }
}

/// Metadata for a single inode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeMetadata {
    /// Inode number (starts at 1 for root directory)
    pub inode: u64,

    /// Filename (max 255 characters)
    pub filename: String,

    /// Parent inode (0 for root)
    pub parent: u64,

    /// File type
    pub file_type: FileType,

    /// Permission bits (Unix mode without file type bits)
    pub permissions: u16,

    /// Owner user ID
    pub uid: u32,

    /// Owner group ID
    pub gid: u32,

    /// File size in bytes (for regular files)
    pub size: u64,

    // === Internal bookkeeping timestamps ===
    // These are used for archival/eviction decisions and are not user-visible.
    // They track when the filesystem actually observed changes.

    /// Time the file was created (internal)
    pub created_time: SystemTime,

    /// Time the file content was last modified (internal, on close if written)
    /// This is updated even when user sets mtime to an old value.
    pub mutated_time: SystemTime,

    /// Time the file was last accessed (internal, on open and close)
    /// Used for cold storage eviction decisions.
    pub accessed_time: SystemTime,

    /// Time the file was last closed (internal)
    pub closed_time: SystemTime,

    // === Unix-level timestamps ===
    // These are what stat() returns and what users can modify via utimes/utimensat.

    /// Unix atime - last access time (user-visible, can be set by user)
    #[serde(default = "default_unix_time")]
    pub unix_atime: SystemTime,

    /// Unix mtime - last modification time (user-visible, can be set by user)
    #[serde(default = "default_unix_time")]
    pub unix_mtime: SystemTime,

    /// Unix ctime - last status change time (user-visible, updated on metadata changes)
    /// Note: ctime cannot be set by users, only updated by the kernel on metadata changes.
    #[serde(default = "default_unix_time")]
    pub unix_ctime: SystemTime,

    /// Time the file was last checkpointed to cloud storage (None if never)
    pub checkpointed_time: Option<SystemTime>,

    /// SHA256 hash of the last checkpointed version (None if never checkpointed)
    pub checkpointed_sha256: Option<[u8; 32]>,

    /// History of SHA256 hashes for this file (most recent first, up to version_count)
    /// Used to track which versions exist in cold storage for cleanup
    #[serde(default)]
    pub sha_history: Vec<[u8; 32]>,

    /// Whether local data is present (false if evicted to cloud)
    pub local_data_present: bool,

    /// For directories: list of child inode numbers
    #[serde(default)]
    pub children: Vec<u64>,

    /// CRC32 of the serialized entry (computed separately, not included in CRC)
    #[serde(skip)]
    pub crc: u32,
}

fn default_unix_time() -> SystemTime {
    SystemTime::UNIX_EPOCH
}

impl InodeMetadata {
    /// Create a new root directory inode
    pub fn new_root(clock: &dyn Clock, uid: u32, gid: u32) -> Self {
        let now = clock.now();
        Self {
            inode: 1,
            filename: String::new(),
            parent: 0,
            file_type: FileType::Directory,
            permissions: 0o755,
            uid,
            gid,
            size: 0,
            created_time: now,
            mutated_time: now,
            accessed_time: now,
            closed_time: now,
            unix_atime: now,
            unix_mtime: now,
            unix_ctime: now,
            checkpointed_time: None,
            checkpointed_sha256: None,
            sha_history: Vec::new(),
            local_data_present: true,
            children: Vec::new(),
            crc: 0,
        }
    }

    /// Create a new regular file inode
    pub fn new_file(clock: &dyn Clock, inode: u64, filename: String, parent: u64, uid: u32, gid: u32, mode: u16) -> Self {
        let now = clock.now();
        Self {
            inode,
            filename,
            parent,
            file_type: FileType::RegularFile,
            permissions: mode & 0o777,
            uid,
            gid,
            size: 0,
            created_time: now,
            mutated_time: now,
            accessed_time: now,
            closed_time: now,
            unix_atime: now,
            unix_mtime: now,
            unix_ctime: now,
            checkpointed_time: None,
            checkpointed_sha256: None,
            sha_history: Vec::new(),
            local_data_present: true,
            children: Vec::new(),
            crc: 0,
        }
    }

    /// Create a new directory inode
    pub fn new_directory(clock: &dyn Clock, inode: u64, filename: String, parent: u64, uid: u32, gid: u32, mode: u16) -> Self {
        let now = clock.now();
        Self {
            inode,
            filename,
            parent,
            file_type: FileType::Directory,
            permissions: mode & 0o777,
            uid,
            gid,
            size: 0,
            created_time: now,
            mutated_time: now,
            accessed_time: now,
            closed_time: now,
            unix_atime: now,
            unix_mtime: now,
            unix_ctime: now,
            checkpointed_time: None,
            checkpointed_sha256: None,
            sha_history: Vec::new(),
            local_data_present: true,
            children: Vec::new(),
            crc: 0,
        }
    }

    /// Serialize the metadata to bytes (without CRC field)
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Deserialize metadata from bytes
    #[cfg(test)]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }

    /// Compute CRC32 of the serialized metadata
    #[allow(dead_code)]
    pub fn compute_crc(&self) -> Result<u32, bincode::Error> {
        let bytes = self.to_bytes()?;
        Ok(crc32fast::hash(&bytes))
    }

    /// Serialize with CRC appended
    pub fn to_bytes_with_crc(&self) -> Result<Vec<u8>, bincode::Error> {
        let mut bytes = self.to_bytes()?;
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        Ok(bytes)
    }

    /// Deserialize and validate CRC
    pub fn from_bytes_with_crc(bytes: &[u8]) -> Result<Self, crate::error::BsfsError> {
        if bytes.len() < 4 {
            return Err(crate::error::BsfsError::CrcMismatch { inode: 0 });
        }
        let (data, crc_bytes) = bytes.split_at(bytes.len() - 4);
        let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        let computed_crc = crc32fast::hash(data);

        if stored_crc != computed_crc {
            // Try to deserialize to get inode for error message
            let inode = bincode::deserialize::<Self>(data)
                .map(|m| m.inode)
                .unwrap_or(0);
            return Err(crate::error::BsfsError::CrcMismatch { inode });
        }

        let mut metadata: Self = bincode::deserialize(data)?;
        metadata.crc = stored_crc;
        Ok(metadata)
    }

    /// Check if this file is "cold" (not accessed recently and could be evicted)
    #[allow(dead_code)]
    pub fn is_cold(&self, threshold: SystemTime) -> bool {
        self.file_type == FileType::RegularFile
            && self.local_data_present
            && self.closed_time < threshold
    }

    /// Check if this file needs checkpointing (modified since last checkpoint)
    pub fn needs_checkpoint(&self) -> bool {
        match self.checkpointed_time {
            None => true,
            Some(cp_time) => self.mutated_time > cp_time,
        }
    }

    /// Compute the cloud storage key for this file's checkpointed version
    /// Format: `{sha256[0:2]}/{sha256[0:4]}/{sha256}.{filename}`
    /// Returns None if not checkpointed
    pub fn cloud_key(&self) -> Option<String> {
        self.checkpointed_sha256
            .map(|sha256| Self::cloud_key_for_sha(sha256, &self.filename))
    }

    /// Compute cloud storage key for a given SHA256 and filename
    /// Format: `{sha256[0:2]}/{sha256[0:4]}/{sha256}.{filename}`
    pub fn cloud_key_for_sha(sha256: [u8; 32], filename: &str) -> String {
        let hex = hex::encode(sha256);
        format!("{}/{}/{}.{}", &hex[0..2], &hex[0..4], hex, filename)
    }

    /// Convert to fuser FileAttr
    /// Uses the Unix-level timestamps (user-visible) rather than internal bookkeeping times
    pub fn to_file_attr(&self) -> fuser::FileAttr {
        let kind = match self.file_type {
            FileType::RegularFile => fuser::FileType::RegularFile,
            FileType::Directory => fuser::FileType::Directory,
        };

        fuser::FileAttr {
            ino: self.inode,
            size: self.size,
            blocks: (self.size + 511) / 512,
            atime: self.unix_atime,
            mtime: self.unix_mtime,
            ctime: self.unix_ctime,
            crtime: self.created_time,
            kind,
            perm: self.permissions,
            nlink: if self.file_type == FileType::Directory {
                2 + self.children.len() as u32
            } else {
                1
            },
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;

    #[test]
    fn test_inode_fixed_size_matches_serialized() {
        // Create an inode with all variable-length fields empty/None
        // This tests that INODE_FIXED_SIZE is correct and will catch
        // any new fields that are added without updating the constant.
        let clock = MockClock::default();
        let inode = InodeMetadata {
            inode: 0,
            filename: String::new(),  // empty
            parent: 0,
            file_type: FileType::RegularFile,
            permissions: 0,
            uid: 0,
            gid: 0,
            size: 0,
            created_time: clock.now(),
            mutated_time: clock.now(),
            accessed_time: clock.now(),
            closed_time: clock.now(),
            unix_atime: clock.now(),
            unix_mtime: clock.now(),
            unix_ctime: clock.now(),
            checkpointed_time: None,  // None
            checkpointed_sha256: None,  // None
            sha_history: Vec::new(),  // empty
            local_data_present: false,
            children: Vec::new(),  // empty
            crc: 0,
        };

        let bytes = inode.to_bytes().unwrap();
        assert_eq!(
            bytes.len() as u32,
            INODE_FIXED_SIZE,
            "INODE_FIXED_SIZE ({}) doesn't match actual serialized size of minimal inode ({}). \
             Did you add a new field and forget to update INODE_FIXED_SIZE?",
            INODE_FIXED_SIZE,
            bytes.len()
        );
    }

    #[test]
    fn test_new_root() {
        let clock = MockClock::default();
        let root = InodeMetadata::new_root(&clock, 1000, 1000);
        assert_eq!(root.inode, 1);
        assert_eq!(root.parent, 0);
        assert_eq!(root.file_type, FileType::Directory);
        assert_eq!(root.permissions, 0o755);
        assert_eq!(root.created_time, clock.now());
    }

    #[test]
    fn test_new_file() {
        let clock = MockClock::default();
        let file = InodeMetadata::new_file(&clock, 42, "test.txt".into(), 1, 1000, 1000, 0o644);
        assert_eq!(file.inode, 42);
        assert_eq!(file.filename, "test.txt");
        assert_eq!(file.parent, 1);
        assert_eq!(file.file_type, FileType::RegularFile);
        assert_eq!(file.permissions, 0o644);
        assert_eq!(file.created_time, clock.now());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let clock = MockClock::default();
        let file = InodeMetadata::new_file(&clock, 123, "example.txt".into(), 1, 500, 500, 0o600);
        let bytes = file.to_bytes().unwrap();
        let restored = InodeMetadata::from_bytes(&bytes).unwrap();
        assert_eq!(restored.inode, file.inode);
        assert_eq!(restored.filename, file.filename);
        assert_eq!(restored.permissions, file.permissions);
        assert_eq!(restored.created_time, file.created_time);
    }

    #[test]
    fn test_crc_validation() {
        let clock = MockClock::default();
        let file = InodeMetadata::new_file(&clock, 456, "crc_test.txt".into(), 1, 0, 0, 0o644);
        let bytes_with_crc = file.to_bytes_with_crc().unwrap();
        let restored = InodeMetadata::from_bytes_with_crc(&bytes_with_crc).unwrap();
        assert_eq!(restored.inode, file.inode);
    }

    #[test]
    fn test_crc_mismatch() {
        let clock = MockClock::default();
        let file = InodeMetadata::new_file(&clock, 789, "corrupt.txt".into(), 1, 0, 0, 0o644);
        let mut bytes_with_crc = file.to_bytes_with_crc().unwrap();
        // Corrupt one byte
        bytes_with_crc[10] ^= 0xFF;
        let result = InodeMetadata::from_bytes_with_crc(&bytes_with_crc);
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod proptest_tests {
    use super::*;
    use proptest::prelude::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn arb_file_type() -> impl Strategy<Value = FileType> {
        prop_oneof![Just(FileType::RegularFile), Just(FileType::Directory),]
    }

    /// Generate a random but deterministic timestamp (seconds since epoch)
    fn arb_timestamp() -> impl Strategy<Value = SystemTime> {
        // Range from year 2000 to 2100 in seconds
        (946684800u64..4102444800u64).prop_map(|secs| UNIX_EPOCH + Duration::from_secs(secs))
    }

    prop_compose! {
        fn arb_inode_metadata()(
            inode in 1u64..u64::MAX,
            filename in "[a-zA-Z0-9_.-]{1,255}",
            parent in 0u64..u64::MAX,
            file_type in arb_file_type(),
            permissions in 0u16..0o777,
            uid in any::<u32>(),
            gid in any::<u32>(),
            size in any::<u64>(),
            created_time in arb_timestamp(),
            mutated_time in arb_timestamp(),
            accessed_time in arb_timestamp(),
            closed_time in arb_timestamp(),
            unix_atime in arb_timestamp(),
            unix_mtime in arb_timestamp(),
            unix_ctime in arb_timestamp(),
        ) -> InodeMetadata {
            InodeMetadata {
                inode,
                filename,
                parent,
                file_type,
                permissions,
                uid,
                gid,
                size,
                created_time,
                mutated_time,
                accessed_time,
                closed_time,
                unix_atime,
                unix_mtime,
                unix_ctime,
                checkpointed_time: None,
                checkpointed_sha256: None,
                sha_history: Vec::new(),
                local_data_present: true,
                children: Vec::new(),
                crc: 0,
            }
        }
    }

    proptest! {
        #[test]
        fn test_serialization_roundtrip_proptest(metadata in arb_inode_metadata()) {
            let bytes = metadata.to_bytes().unwrap();
            let restored = InodeMetadata::from_bytes(&bytes).unwrap();
            prop_assert_eq!(restored.inode, metadata.inode);
            prop_assert_eq!(restored.filename, metadata.filename);
            prop_assert_eq!(restored.parent, metadata.parent);
            prop_assert_eq!(restored.permissions, metadata.permissions);
            // Internal bookkeeping timestamps
            prop_assert_eq!(restored.created_time, metadata.created_time);
            prop_assert_eq!(restored.mutated_time, metadata.mutated_time);
            prop_assert_eq!(restored.accessed_time, metadata.accessed_time);
            prop_assert_eq!(restored.closed_time, metadata.closed_time);
            // Unix-level timestamps
            prop_assert_eq!(restored.unix_atime, metadata.unix_atime);
            prop_assert_eq!(restored.unix_mtime, metadata.unix_mtime);
            prop_assert_eq!(restored.unix_ctime, metadata.unix_ctime);
        }

        #[test]
        fn test_crc_roundtrip_proptest(metadata in arb_inode_metadata()) {
            let bytes = metadata.to_bytes_with_crc().unwrap();
            let restored = InodeMetadata::from_bytes_with_crc(&bytes).unwrap();
            prop_assert_eq!(restored.inode, metadata.inode);
            prop_assert_eq!(restored.created_time, metadata.created_time);
        }

        #[test]
        fn test_crc_detects_corruption(metadata in arb_inode_metadata(), corrupt_pos in 0usize..100, corrupt_val in 1u8..255) {
            let mut bytes = metadata.to_bytes_with_crc().unwrap();
            if corrupt_pos < bytes.len() - 4 {
                // Don't corrupt the CRC itself
                bytes[corrupt_pos] ^= corrupt_val;
                let result = InodeMetadata::from_bytes_with_crc(&bytes);
                prop_assert!(result.is_err());
            }
        }
    }
}
