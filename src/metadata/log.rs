use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::clock::Clock;
use crate::error::{BsfsError, Result};
use crate::metadata::{Checkpoint, INODE_FIXED_SIZE};

const LOG_FILE_MAGIC: &[u8; 8] = b"BSFS_LG\0";
const LOG_VERSION: u32 = 1;
const LOG_RECORD_MAGIC: u32 = 0x4C4F4752; // "LOGR"
const LOG_HEADER_SIZE: u64 = 8 + 4 + 4; // magic + version + inode_fixed_size

/// A single record in the metadata log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogRecord {
    /// File was opened
    Open {
        inode: u64,
        timestamp: SystemTime,
    },
    /// File was closed
    Close {
        inode: u64,
        timestamp: SystemTime,
        was_written: bool,
        new_sha256: Option<[u8; 32]>,
        new_size: u64,
    },
    /// File was created
    Create {
        inode: u64,
        parent: u64,
        filename: String,
        uid: u32,
        gid: u32,
        mode: u16,
        is_directory: bool,
        timestamp: SystemTime,
    },
    /// File was deleted
    Delete {
        inode: u64,
        parent: u64,
        timestamp: SystemTime,
    },
    /// File was renamed
    Rename {
        inode: u64,
        old_parent: u64,
        new_parent: u64,
        new_name: String,
        timestamp: SystemTime,
    },
    /// File attributes were changed
    SetAttr {
        inode: u64,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u16>,
        size: Option<u64>,
        timestamp: SystemTime,
    },
    /// Old version should be purged from cloud storage
    Purge {
        /// Cloud storage key to delete
        key: String,
        /// Inode this version belonged to (for logging)
        inode: u64,
        timestamp: SystemTime,
    },
}

impl LogRecord {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }
}

/// Manages the append-only metadata log
pub struct MetadataLog {
    path: std::path::PathBuf,
    writer: Option<BufWriter<File>>,
}

impl MetadataLog {
    /// Write the file header
    fn write_header(writer: &mut BufWriter<File>) -> Result<()> {
        writer.write_all(LOG_FILE_MAGIC)?;
        writer.write_all(&LOG_VERSION.to_le_bytes())?;
        writer.write_all(&INODE_FIXED_SIZE.to_le_bytes())?;
        writer.flush()?;
        Ok(())
    }

    /// Verify the file header, returns Ok(()) if valid
    fn verify_header(path: &Path) -> Result<()> {
        let mut file = File::open(path)?;

        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if &magic != LOG_FILE_MAGIC {
            return Err(BsfsError::Recovery("Invalid log file magic".into()));
        }

        let mut version_bytes = [0u8; 4];
        file.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);
        if version != LOG_VERSION {
            return Err(BsfsError::Recovery(format!(
                "Unsupported log version: {} (expected {})",
                version, LOG_VERSION
            )));
        }

        let mut inode_size_bytes = [0u8; 4];
        file.read_exact(&mut inode_size_bytes)?;
        let inode_fixed_size = u32::from_le_bytes(inode_size_bytes);
        if inode_fixed_size != INODE_FIXED_SIZE {
            return Err(BsfsError::Recovery(format!(
                "Incompatible inode format in log: file has fixed_size={}, expected {}",
                inode_fixed_size, INODE_FIXED_SIZE
            )));
        }

        Ok(())
    }

    /// Open or create the metadata log
    pub fn open(path: &Path) -> Result<Self> {
        let needs_header = !path.exists() || std::fs::metadata(path).map(|m| m.len() == 0).unwrap_or(true);

        if needs_header {
            // Create new file with header
            let file = File::create(path)?;
            let mut writer = BufWriter::new(file);
            Self::write_header(&mut writer)?;

            Ok(Self {
                path: path.to_path_buf(),
                writer: Some(writer),
            })
        } else {
            // Verify existing header
            Self::verify_header(path)?;

            // Open for appending
            let file = OpenOptions::new()
                .append(true)
                .open(path)?;
            let writer = BufWriter::new(file);

            Ok(Self {
                path: path.to_path_buf(),
                writer: Some(writer),
            })
        }
    }

    /// Append a record to the log
    pub fn append(&mut self, record: &LogRecord) -> Result<()> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            BsfsError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Log not open",
            ))
        })?;

        let data = record.to_bytes()?;
        let crc = crc32fast::hash(&data);

        // Write: magic, length, data, crc
        writer.write_all(&LOG_RECORD_MAGIC.to_le_bytes())?;
        writer.write_all(&(data.len() as u32).to_le_bytes())?;
        writer.write_all(&data)?;
        writer.write_all(&crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Read all records from the log
    pub fn read_all(path: &Path) -> Result<Vec<LogRecord>> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file_len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if file_len == 0 {
            return Ok(Vec::new());
        }

        // Verify header first
        Self::verify_header(path)?;

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Skip past the header
        reader.seek(SeekFrom::Start(LOG_HEADER_SIZE))?;

        let mut records = Vec::new();

        loop {
            // Try to read magic
            let mut magic_bytes = [0u8; 4];
            match reader.read_exact(&mut magic_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let magic = u32::from_le_bytes(magic_bytes);
            if magic != LOG_RECORD_MAGIC {
                // Corrupt record, stop here
                tracing::warn!("Corrupt log record found, stopping replay");
                break;
            }

            // Read length
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;

            // Read data
            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)?;

            // Read and verify CRC
            let mut crc_bytes = [0u8; 4];
            reader.read_exact(&mut crc_bytes)?;
            let stored_crc = u32::from_le_bytes(crc_bytes);
            let computed_crc = crc32fast::hash(&data);

            if stored_crc != computed_crc {
                tracing::warn!("Log record CRC mismatch, stopping replay");
                break;
            }

            let record = LogRecord::from_bytes(&data)?;
            records.push(record);
        }

        Ok(records)
    }

    /// Replay log records into a checkpoint
    pub fn replay(clock: &dyn Clock, records: &[LogRecord], checkpoint: &mut Checkpoint) -> Result<()> {
        use crate::metadata::InodeMetadata;

        for record in records {
            match record {
                LogRecord::Open { inode, timestamp } => {
                    if let Some(meta) = checkpoint.get_mut(*inode) {
                        meta.accessed_time = *timestamp;
                    }
                }
                LogRecord::Close {
                    inode,
                    timestamp,
                    was_written,
                    new_sha256,
                    new_size,
                } => {
                    if let Some(meta) = checkpoint.get_mut(*inode) {
                        meta.accessed_time = *timestamp;
                        meta.closed_time = *timestamp;
                        meta.size = *new_size;
                        if *was_written {
                            meta.mutated_time = *timestamp;
                            if let Some(sha) = new_sha256 {
                                meta.checkpointed_sha256 = Some(*sha);
                            }
                        }
                    }
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
                    // Use clock for initial timestamp, but override with record's timestamp
                    let mut meta = if *is_directory {
                        InodeMetadata::new_directory(clock, *inode, filename.clone(), *parent, *uid, *gid, *mode)
                    } else {
                        InodeMetadata::new_file(clock, *inode, filename.clone(), *parent, *uid, *gid, *mode)
                    };
                    meta.created_time = *timestamp;
                    meta.mutated_time = *timestamp;
                    meta.accessed_time = *timestamp;
                    meta.closed_time = *timestamp;
                    checkpoint.insert(meta);

                    // Update parent's children list
                    if let Some(parent_meta) = checkpoint.get_mut(*parent) {
                        if !parent_meta.children.contains(inode) {
                            parent_meta.children.push(*inode);
                        }
                    }

                    // Update next_inode if needed
                    if *inode >= checkpoint.next_inode {
                        checkpoint.next_inode = *inode + 1;
                    }
                }
                LogRecord::Delete {
                    inode,
                    parent,
                    timestamp: _,
                } => {
                    checkpoint.remove(*inode);
                    if let Some(parent_meta) = checkpoint.get_mut(*parent) {
                        parent_meta.children.retain(|&i| i != *inode);
                    }
                }
                LogRecord::Rename {
                    inode,
                    old_parent,
                    new_parent,
                    new_name,
                    timestamp,
                } => {
                    // Remove from old parent
                    if let Some(old_parent_meta) = checkpoint.get_mut(*old_parent) {
                        old_parent_meta.children.retain(|&i| i != *inode);
                    }

                    // Add to new parent
                    if let Some(new_parent_meta) = checkpoint.get_mut(*new_parent) {
                        if !new_parent_meta.children.contains(inode) {
                            new_parent_meta.children.push(*inode);
                        }
                    }

                    // Update inode metadata
                    if let Some(meta) = checkpoint.get_mut(*inode) {
                        meta.parent = *new_parent;
                        meta.filename = new_name.clone();
                        meta.mutated_time = *timestamp;
                    }
                }
                LogRecord::SetAttr {
                    inode,
                    uid,
                    gid,
                    mode,
                    size,
                    timestamp,
                } => {
                    if let Some(meta) = checkpoint.get_mut(*inode) {
                        if let Some(u) = uid {
                            meta.uid = *u;
                        }
                        if let Some(g) = gid {
                            meta.gid = *g;
                        }
                        if let Some(m) = mode {
                            meta.permissions = *m;
                        }
                        if let Some(s) = size {
                            meta.size = *s;
                        }
                        meta.mutated_time = *timestamp;
                    }
                }
                LogRecord::Purge { .. } => {
                    // Purge records don't modify checkpoint state
                    // They're processed by the sweeper for cloud storage cleanup
                }
            }
        }

        Ok(())
    }

    /// Truncate the log file (keeps header, removes all records)
    pub fn truncate(&mut self) -> Result<()> {
        // Close current writer
        self.writer = None;

        // Truncate file and rewrite header
        let file = File::create(&self.path)?;
        let mut writer = BufWriter::new(file);
        Self::write_header(&mut writer)?;

        self.writer = Some(writer);

        Ok(())
    }

    /// Flush the log to disk
    pub fn flush(&mut self) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_append_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("log");
        let clock = MockClock::default();

        let open_time = clock.now();
        clock.advance(Duration::from_secs(10));
        let close_time = clock.now();

        {
            let mut log = MetadataLog::open(&path).unwrap();
            log.append(&LogRecord::Open {
                inode: 42,
                timestamp: open_time,
            })
            .unwrap();
            log.append(&LogRecord::Close {
                inode: 42,
                timestamp: close_time,
                was_written: true,
                new_sha256: None,
                new_size: 100,
            })
            .unwrap();
        }

        let records = MetadataLog::read_all(&path).unwrap();
        assert_eq!(records.len(), 2);

        // Verify timestamps are preserved
        match &records[0] {
            LogRecord::Open { timestamp, .. } => assert_eq!(*timestamp, open_time),
            _ => panic!("Expected Open record"),
        }
        match &records[1] {
            LogRecord::Close { timestamp, .. } => assert_eq!(*timestamp, close_time),
            _ => panic!("Expected Close record"),
        }
    }

    #[test]
    fn test_replay_create() {
        let clock = MockClock::default();
        let mut cp = Checkpoint::new(&clock, 1000, 1000);

        clock.advance(Duration::from_secs(100));
        let create_time = clock.now();

        let records = vec![LogRecord::Create {
            inode: 2,
            parent: 1,
            filename: "test.txt".into(),
            uid: 1000,
            gid: 1000,
            mode: 0o644,
            is_directory: false,
            timestamp: create_time,
        }];

        MetadataLog::replay(&clock, &records, &mut cp).unwrap();
        assert!(cp.inodes.contains_key(&2));
        assert_eq!(cp.inodes.get(&2).unwrap().filename, "test.txt");
        // Check parent has child
        assert!(cp.inodes.get(&1).unwrap().children.contains(&2));
        // Verify timestamp from record is used
        assert_eq!(cp.inodes.get(&2).unwrap().created_time, create_time);
    }

    #[test]
    fn test_truncate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("log");
        let clock = MockClock::default();

        {
            let mut log = MetadataLog::open(&path).unwrap();
            log.append(&LogRecord::Open {
                inode: 1,
                timestamp: clock.now(),
            })
            .unwrap();
            log.truncate().unwrap();
        }

        let records = MetadataLog::read_all(&path).unwrap();
        assert_eq!(records.len(), 0);
    }
}

#[cfg(test)]
mod proptest_tests {
    use super::*;
    use proptest::prelude::*;
    use std::time::{Duration, UNIX_EPOCH};
    use tempfile::tempdir;

    /// Generate a random but deterministic timestamp
    fn arb_timestamp() -> impl Strategy<Value = SystemTime> {
        (946684800u64..4102444800u64).prop_map(|secs| UNIX_EPOCH + Duration::from_secs(secs))
    }

    fn arb_log_record() -> impl Strategy<Value = LogRecord> {
        prop_oneof![
            (any::<u64>(), arb_timestamp()).prop_map(|(inode, timestamp)| LogRecord::Open {
                inode,
                timestamp,
            }),
            (any::<u64>(), any::<bool>(), any::<u64>(), arb_timestamp()).prop_map(
                |(inode, was_written, size, timestamp)| {
                    LogRecord::Close {
                        inode,
                        timestamp,
                        was_written,
                        new_sha256: None,
                        new_size: size,
                    }
                }
            ),
            (
                any::<u64>(),
                any::<u64>(),
                "[a-zA-Z0-9_.-]{1,64}",
                any::<u32>(),
                any::<u32>(),
                0u16..0o777,
                any::<bool>(),
                arb_timestamp()
            )
                .prop_map(|(inode, parent, filename, uid, gid, mode, is_dir, timestamp)| {
                    LogRecord::Create {
                        inode,
                        parent,
                        filename,
                        uid,
                        gid,
                        mode,
                        is_directory: is_dir,
                        timestamp,
                    }
                }),
        ]
    }

    /// Extract timestamp from a LogRecord for comparison
    fn get_timestamp(record: &LogRecord) -> SystemTime {
        match record {
            LogRecord::Open { timestamp, .. } => *timestamp,
            LogRecord::Close { timestamp, .. } => *timestamp,
            LogRecord::Create { timestamp, .. } => *timestamp,
            LogRecord::Delete { timestamp, .. } => *timestamp,
            LogRecord::Rename { timestamp, .. } => *timestamp,
            LogRecord::SetAttr { timestamp, .. } => *timestamp,
            LogRecord::Purge { timestamp, .. } => *timestamp,
        }
    }

    proptest! {
        #[test]
        fn test_record_roundtrip(record in arb_log_record()) {
            let bytes = record.to_bytes().unwrap();
            let restored = LogRecord::from_bytes(&bytes).unwrap();
            // Now we can compare timestamps!
            prop_assert_eq!(get_timestamp(&restored), get_timestamp(&record));
        }

        #[test]
        fn test_log_roundtrip(records in prop::collection::vec(arb_log_record(), 0..20)) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("log");

            {
                let mut log = MetadataLog::open(&path).unwrap();
                for record in &records {
                    log.append(record).unwrap();
                }
            }

            let read_records = MetadataLog::read_all(&path).unwrap();
            prop_assert_eq!(read_records.len(), records.len());

            // Verify timestamps are preserved
            for (original, restored) in records.iter().zip(read_records.iter()) {
                prop_assert_eq!(get_timestamp(original), get_timestamp(restored));
            }
        }
    }
}
