use fuser::{
    FileType as FuseFileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio::runtime::Handle;

use crate::clock::SharedClock;
use crate::cloud::CloudStorage;
use crate::config::Config;
use crate::fs::operations;
use crate::metadata::{Checkpoint, FileType, InodeMetadata, LogRecord, MetadataLog};

const TTL: Duration = Duration::from_secs(1);

/// File handle tracking
#[allow(dead_code)]
struct OpenFile {
    inode: u64,
    flags: i32,
    written: bool,
}

/// The BSFS filesystem implementation
pub struct BsfsFilesystem<C: CloudStorage> {
    config: Config,
    clock: SharedClock,
    checkpoint: Arc<RwLock<Checkpoint>>,
    log: Arc<RwLock<MetadataLog>>,
    cloud: Arc<C>,
    runtime: Handle,
    open_files: RwLock<HashMap<u64, OpenFile>>,
    next_fh: RwLock<u64>,
}

impl<C: CloudStorage + 'static> BsfsFilesystem<C> {
    pub fn new(
        config: Config,
        clock: SharedClock,
        checkpoint: Arc<RwLock<Checkpoint>>,
        log: Arc<RwLock<MetadataLog>>,
        cloud: Arc<C>,
        runtime: Handle,
    ) -> Self {
        Self {
            config,
            clock,
            checkpoint,
            log,
            cloud,
            runtime,
            open_files: RwLock::new(HashMap::new()),
            next_fh: RwLock::new(1),
        }
    }

    fn allocate_fh(&self) -> u64 {
        let mut fh = self.next_fh.write().unwrap();
        let result = *fh;
        *fh += 1;
        result
    }

    fn data_root(&self) -> PathBuf {
        self.config.data_root()
    }

    /// Ensure local data is present, fetching from cloud if needed
    fn ensure_local_data(&self, inode: u64) -> Result<(), libc::c_int> {
        let cp = self.checkpoint.read().unwrap();
        let meta = cp.get(inode).ok_or(libc::ENOENT)?;

        if meta.local_data_present {
            return Ok(());
        }

        // Need to fetch from cloud
        let cloud = Arc::clone(&self.cloud);
        let data_root = self.data_root();
        let version_id = meta.checkpointed_sha256.map(|sha| hex::encode(sha));

        drop(cp); // Release lock before async operation

        // Fetch synchronously using the tokio runtime
        self.runtime
            .block_on(async {
                operations::fetch_from_cloud(
                    cloud.as_ref(),
                    &data_root,
                    inode,
                    version_id.as_deref(),
                )
                .await
            })
            .map_err(|e| e.to_errno())?;

        // Update metadata
        let mut cp = self.checkpoint.write().unwrap();
        if let Some(meta) = cp.get_mut(inode) {
            meta.local_data_present = true;
        }

        Ok(())
    }
}

impl<C: CloudStorage + 'static> Filesystem for BsfsFilesystem<C> {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        tracing::info!("BSFS filesystem initialized");
        Ok(())
    }

    fn destroy(&mut self) {
        tracing::info!("BSFS filesystem destroyed");
        // Flush log
        if let Ok(mut log) = self.log.write() {
            let _ = log.flush();
        }
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let cp = self.checkpoint.read().unwrap();
        match cp.lookup_child(parent, name) {
            Some(meta) => {
                reply.entry(&TTL, &meta.to_file_attr(), 0);
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let cp = self.checkpoint.read().unwrap();
        match cp.get(ino) {
            Some(meta) => {
                reply.attr(&TTL, &meta.to_file_attr());
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let mut cp = self.checkpoint.write().unwrap();
        let meta = match cp.get_mut(ino) {
            Some(m) => m,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let now = self.clock.now();

        if let Some(m) = mode {
            meta.permissions = (m & 0o777) as u16;
        }
        if let Some(u) = uid {
            meta.uid = u;
        }
        if let Some(g) = gid {
            meta.gid = g;
        }
        if let Some(s) = size {
            // Truncate file
            if meta.file_type == FileType::RegularFile {
                let data_root = self.data_root();
                if let Err(_) = operations::truncate_local(&data_root, ino, s) {
                    reply.error(libc::EIO);
                    return;
                }
                meta.size = s;
            }
        }

        meta.mutated_time = now;
        let attr = meta.to_file_attr();

        // Log the change
        drop(cp);
        if let Ok(mut log) = self.log.write() {
            let _ = log.append(&LogRecord::SetAttr {
                inode: ino,
                uid,
                gid,
                mode: mode.map(|m| (m & 0o777) as u16),
                size,
                timestamp: now,
            });
        }

        reply.attr(&TTL, &attr);
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        if name.len() > 255 {
            reply.error(libc::ENAMETOOLONG);
            return;
        }

        let mut cp = self.checkpoint.write().unwrap();

        // Check parent exists and is a directory
        match cp.get(parent) {
            Some(m) if m.file_type == FileType::Directory => {}
            Some(_) => {
                reply.error(libc::ENOTDIR);
                return;
            }
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        }

        // Check name doesn't already exist
        if cp.lookup_child(parent, name).is_some() {
            reply.error(libc::EEXIST);
            return;
        }

        let inode = cp.allocate_inode();
        let now = self.clock.now();

        let meta = InodeMetadata::new_directory(
            self.clock.as_ref(),
            inode,
            name.to_string(),
            parent,
            req.uid(),
            req.gid(),
            (mode & 0o777) as u16,
        );

        let attr = meta.to_file_attr();
        cp.insert(meta);

        // Add to parent's children
        if let Some(parent_meta) = cp.get_mut(parent) {
            parent_meta.children.push(inode);
        }

        // Log the creation
        drop(cp);
        if let Ok(mut log) = self.log.write() {
            let _ = log.append(&LogRecord::Create {
                inode,
                parent,
                filename: name.to_string(),
                uid: req.uid(),
                gid: req.gid(),
                mode: (mode & 0o777) as u16,
                is_directory: true,
                timestamp: now,
            });
        }

        reply.entry(&TTL, &attr, 0);
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let mut cp = self.checkpoint.write().unwrap();

        // Find the directory to remove
        let inode = match cp.lookup_child(parent, name) {
            Some(meta) => {
                if meta.file_type != FileType::Directory {
                    reply.error(libc::ENOTDIR);
                    return;
                }
                if !meta.children.is_empty() {
                    reply.error(libc::ENOTEMPTY);
                    return;
                }
                meta.inode
            }
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Remove from parent's children
        if let Some(parent_meta) = cp.get_mut(parent) {
            parent_meta.children.retain(|&i| i != inode);
        }

        // Remove the inode
        cp.remove(inode);

        let now = self.clock.now();
        drop(cp);

        // Log the deletion
        if let Ok(mut log) = self.log.write() {
            let _ = log.append(&LogRecord::Delete {
                inode,
                parent,
                timestamp: now,
            });
        }

        reply.ok();
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        if name.len() > 255 {
            reply.error(libc::ENAMETOOLONG);
            return;
        }

        let mut cp = self.checkpoint.write().unwrap();

        // Check parent exists and is a directory
        match cp.get(parent) {
            Some(m) if m.file_type == FileType::Directory => {}
            Some(_) => {
                reply.error(libc::ENOTDIR);
                return;
            }
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Check name doesn't already exist
        if cp.lookup_child(parent, name).is_some() {
            reply.error(libc::EEXIST);
            return;
        }

        let inode = cp.allocate_inode();
        let now = self.clock.now();

        let meta = InodeMetadata::new_file(
            self.clock.as_ref(),
            inode,
            name.to_string(),
            parent,
            req.uid(),
            req.gid(),
            (mode & 0o777) as u16,
        );

        let attr = meta.to_file_attr();
        cp.insert(meta);

        // Add to parent's children
        if let Some(parent_meta) = cp.get_mut(parent) {
            parent_meta.children.push(inode);
        }

        // Create empty file on disk
        let data_root = self.data_root();
        if let Err(_) = operations::create_local_file(&data_root, inode) {
            // Rollback
            cp.remove(inode);
            if let Some(parent_meta) = cp.get_mut(parent) {
                parent_meta.children.retain(|&i| i != inode);
            }
            reply.error(libc::EIO);
            return;
        }

        // Allocate file handle
        let fh = self.allocate_fh();
        {
            let mut open_files = self.open_files.write().unwrap();
            open_files.insert(
                fh,
                OpenFile {
                    inode,
                    flags,
                    written: false,
                },
            );
        }

        // Log the creation
        drop(cp);
        if let Ok(mut log) = self.log.write() {
            let _ = log.append(&LogRecord::Create {
                inode,
                parent,
                filename: name.to_string(),
                uid: req.uid(),
                gid: req.gid(),
                mode: (mode & 0o777) as u16,
                is_directory: false,
                timestamp: now,
            });
        }

        reply.created(&TTL, &attr, 0, fh, 0);
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let mut cp = self.checkpoint.write().unwrap();

        // Find the file to remove
        let inode = match cp.lookup_child(parent, name) {
            Some(meta) => {
                if meta.file_type != FileType::RegularFile {
                    reply.error(libc::EISDIR);
                    return;
                }
                meta.inode
            }
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Remove from parent's children
        if let Some(parent_meta) = cp.get_mut(parent) {
            parent_meta.children.retain(|&i| i != inode);
        }

        // Remove the inode
        cp.remove(inode);

        // Delete local file
        let data_root = self.data_root();
        let _ = operations::delete_local_file(&data_root, inode);

        let now = self.clock.now();
        drop(cp);

        // Log the deletion
        if let Ok(mut log) = self.log.write() {
            let _ = log.append(&LogRecord::Delete {
                inode,
                parent,
                timestamp: now,
            });
        }

        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        // Ensure local data is present
        if let Err(e) = self.ensure_local_data(ino) {
            reply.error(e);
            return;
        }

        // Allocate file handle
        let fh = self.allocate_fh();
        {
            let mut open_files = self.open_files.write().unwrap();
            open_files.insert(
                fh,
                OpenFile {
                    inode: ino,
                    flags,
                    written: false,
                },
            );
        }

        // Update accessed time and log
        let now = self.clock.now();
        {
            let mut cp = self.checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(ino) {
                meta.accessed_time = now;
            }
        }

        if let Ok(mut log) = self.log.write() {
            let _ = log.append(&LogRecord::Open {
                inode: ino,
                timestamp: now,
            });
        }

        reply.opened(fh, 0);
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let was_written = {
            let mut open_files = self.open_files.write().unwrap();
            open_files.remove(&fh).map(|f| f.written).unwrap_or(false)
        };

        let now = self.clock.now();
        let data_root = self.data_root();

        // Compute SHA256 if file was written
        let (new_sha256, new_size) = if was_written {
            let sha = operations::compute_sha256(&data_root, ino).ok();
            let size = operations::get_local_size(&data_root, ino).unwrap_or(0);
            (sha, size)
        } else {
            let size = {
                let cp = self.checkpoint.read().unwrap();
                cp.get(ino).map(|m| m.size).unwrap_or(0)
            };
            (None, size)
        };

        // Update metadata
        {
            let mut cp = self.checkpoint.write().unwrap();
            if let Some(meta) = cp.get_mut(ino) {
                meta.accessed_time = now;
                meta.closed_time = now;
                if was_written {
                    meta.mutated_time = now;
                    meta.size = new_size;
                }
            }
        }

        // Log the close
        if let Ok(mut log) = self.log.write() {
            let _ = log.append(&LogRecord::Close {
                inode: ino,
                timestamp: now,
                was_written,
                new_sha256,
                new_size,
            });
        }

        reply.ok();
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let data_root = self.data_root();
        match operations::read_local(&data_root, ino, offset as u64, size) {
            Ok(data) => reply.data(&data),
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let data_root = self.data_root();
        match operations::write_local(&data_root, ino, offset as u64, data) {
            Ok(new_size) => {
                // Mark file as written
                {
                    let mut open_files = self.open_files.write().unwrap();
                    if let Some(file) = open_files.get_mut(&fh) {
                        file.written = true;
                    }
                }

                // Update size in metadata
                {
                    let mut cp = self.checkpoint.write().unwrap();
                    if let Some(meta) = cp.get_mut(ino) {
                        meta.size = new_size;
                    }
                }

                reply.written(data.len() as u32);
            }
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let cp = self.checkpoint.read().unwrap();
        let meta = match cp.get(ino) {
            Some(m) if m.file_type == FileType::Directory => m,
            Some(_) => {
                reply.error(libc::ENOTDIR);
                return;
            }
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let mut entries: Vec<(u64, FuseFileType, &str)> = vec![
            (ino, FuseFileType::Directory, "."),
            (meta.parent.max(1), FuseFileType::Directory, ".."),
        ];

        for &child_ino in &meta.children {
            if let Some(child_meta) = cp.get(child_ino) {
                let ftype = match child_meta.file_type {
                    FileType::RegularFile => FuseFileType::RegularFile,
                    FileType::Directory => FuseFileType::Directory,
                };
                entries.push((child_ino, ftype, &child_meta.filename));
            }
        }

        for (i, (ino, ftype, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(*ino, (i + 1) as i64, *ftype, name) {
                break;
            }
        }

        reply.ok();
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        let newname = match newname.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        if newname.len() > 255 {
            reply.error(libc::ENAMETOOLONG);
            return;
        }

        let mut cp = self.checkpoint.write().unwrap();

        // Find source inode
        let inode = match cp.lookup_child(parent, name) {
            Some(meta) => meta.inode,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Check destination parent exists and is a directory
        match cp.get(newparent) {
            Some(m) if m.file_type == FileType::Directory => {}
            Some(_) => {
                reply.error(libc::ENOTDIR);
                return;
            }
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Remove from old parent
        if let Some(parent_meta) = cp.get_mut(parent) {
            parent_meta.children.retain(|&i| i != inode);
        }

        // Add to new parent (if not already there)
        if let Some(newparent_meta) = cp.get_mut(newparent) {
            if !newparent_meta.children.contains(&inode) {
                newparent_meta.children.push(inode);
            }
        }

        // Update inode
        let now = self.clock.now();
        if let Some(meta) = cp.get_mut(inode) {
            meta.parent = newparent;
            meta.filename = newname.to_string();
            meta.mutated_time = now;
        }

        drop(cp);

        // Log the rename
        if let Ok(mut log) = self.log.write() {
            let _ = log.append(&LogRecord::Rename {
                inode,
                old_parent: parent,
                new_parent: newparent,
                new_name: newname.to_string(),
                timestamp: now,
            });
        }

        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        // Flush the log
        if let Ok(mut log) = self.log.write() {
            let _ = log.flush();
        }
        reply.ok();
    }
}
