# Backup Storage File System (BSFS)

A userspace filesystem (FUSE) that provides transparent cloud backup with local caching.

**WARNING** this is 99% vibe coded and has not stood up to any kind of torture testing.
You almost certainly want to use Google's "gcsfuse" instead.

## Overview

BSFS stores files on a local disk while automatically archiving them to Google Cloud Storage.
When local disk space runs low, cold files (those not accessed recently) are evicted from
local storage but remain accessible - they are transparently fetched from cloud storage
when opened. Multiple versions of files are retained in cloud storage for point-in-time recovery.

## Usage

### Running the Filesystem

```
bsfs --mount <MOUNTPOINT> --config <CONFIG_FILE> [--foreground] [--debug]
```

Options:
- `-m, --mount <PATH>`: Mount point for the filesystem
- `-c, --config <PATH>`: Path to JSON configuration file
- `-f, --foreground`: Run in foreground (don't daemonize)
- `-d, --debug`: Enable debug logging

### Inspecting Metadata Files

```
bsfs --list <FILE>
```

Inspects a checkpoint or log file without starting the daemon. Output is machine-parseable.

**Checkpoint output format:**
```
={"type": "checkpoint", "version": 2, "inode_fixed_size": 149, ...}
/: -
/docs: -
/docs/notes.md: -
/readme.txt: ab/abab/abab...abab.readme.txt
```

The first line (prefixed with `=`) is JSON containing file header metadata. Subsequent
lines are `path: storage-key` pairs, where `-` indicates no cloud storage key (not yet
checkpointed or a directory).

**Log output format:**
```
={"type": "log", "version": 1, "inode_fixed_size": 149, "total_records": 3}
Create: {"inode": 2, "parent": 1, "filename": "test.txt", ...}
Open: {"inode": 2, "timestamp": 1704067210}
Close: {"inode": 2, "was_written": true, "new_size": 256, ...}
```

The first line (prefixed with `=`) is JSON containing file header metadata. Subsequent
lines are `RecordType: {json}` pairs. Record types include: `Open`, `Close`, `Create`,
`Delete`, `Rename`, `SetAttr`. Timestamps are Unix epoch seconds.

## Configuration

Configuration is provided via a JSON file. Example:

```json
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
    "inconsistent_start": false,
    "max_storage": 0,
    "parallel_upload": 8
}
```

### Configuration Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `local_root` | Yes | - | Local directory for file storage and metadata |
| `gcs_bucket` | Yes | - | GCS bucket name for cloud archival |
| `gcs_credentials` | No | ambient | Authentication method (see below) |
| `target_free_space` | No | 10GB | Bytes to keep free on local storage |
| `sweep_interval_secs` | No | 300 | Interval between archive sweeps (seconds) |
| `version_count` | No | 5 | Number of file versions to retain in cloud |
| `inconsistent_start` | No | false | Allow future timestamps on startup |
| `max_storage` | No | 0 | Max storage capacity reported by statfs (0 = use physical) |
| `parallel_upload` | No | 8 | Maximum concurrent uploads to cloud storage |

### GCS Credentials

Three authentication methods are supported:

```json
{"type": "ambient"}
```
Uses ambient credentials (GCP VM metadata or `GOOGLE_APPLICATION_CREDENTIALS`).

```json
{"type": "service_account_file", "path": "/path/to/key.json"}
```
Uses a service account JSON key file.

```json
{"type": "service_account_key", "key": "..."}
```
Uses an inline service account JSON key.

## Architecture

### Local Storage Layout

Metadata is stored in two files at `local_root`:
- `bsfs_checkpoint`: Full state snapshot of all inodes
- `bsfs_log`: Append-only log of operations since last checkpoint

File data is stored under `local_root/data/` using an inode-based path structure.
Inode paths are constructed by taking progressively longer prefixes of the decimal
inode number, each padded with zeros:

```
inode 1234567 -> 1000000/1200000/1230000/1234000/1234500/1234560/1234567
inode 3210    -> 3000/3200/3210/3210
inode 42      -> 40/42
```

### Cloud Storage Layout

Files are stored in GCS using content-addressed paths based on SHA256:
```
{sha256[0:2]}/{sha256[0:4]}/{sha256}.{filename}
```

The checkpoint is also uploaded to cloud storage after each sweep cycle.

### Inode Metadata

Each inode stores:
- `inode`: Inode number (starts at 1 for root directory)
- `filename`: Name (max 255 characters)
- `parent`: Parent inode number (0 for root)
- `file_type`: RegularFile or Directory
- `permissions`: Unix permission bits (u16)
- `uid`, `gid`: Owner and group IDs
- `size`: File size in bytes
- `checkpointed_time`: When last uploaded to cloud (null if never)
- `checkpointed_sha256`: SHA256 of last uploaded version (null if never)
- `sha_history`: History of SHA256 hashes for version tracking
- `local_data_present`: Whether data exists locally
- `children`: Child inode numbers (for directories)
- CRC32 checksum for integrity validation

#### Dual Timestamp System

BSFS maintains two separate sets of timestamps:

**Internal bookkeeping timestamps** (not visible to users):
- `created_time`: When the inode was created
- `mutated_time`: When the filesystem last observed content changes
- `accessed_time`: When the file was last opened/closed (used for eviction decisions)
- `closed_time`: When the file was last closed

**Unix-level timestamps** (visible via `stat`, settable via `utimes`/`touch`):
- `unix_atime`: Last access time
- `unix_mtime`: Last modification time
- `unix_ctime`: Last status change time (updated on any metadata change)

This separation is necessary because archive utilities (like `tar`) often restore files
with old timestamps. Without separate timestamps, a file restored with `mtime` from 2020
would appear to not need checkpointing (since the old mtime < checkpointed_time). By
tracking the actual modification time internally (`mutated_time`), BSFS correctly
identifies that the file content changed and needs to be backed up.

When users set atime/mtime via `utimes` or `touch -t`:
1. The Unix-level timestamps are set to the user-requested values
2. The internal `mutated_time` is set to "now" (because we observed a change)
3. The `unix_ctime` is set to "now" (standard Unix behavior)

### Sweeper Behavior

The background sweeper runs at `sweep_interval_secs` intervals and:

1. **Proactive checkpointing**: Uploads modified files to cloud storage regardless
   of disk pressure. Files are uploaded if `mutated_time > checkpointed_time`.

2. **Version management**: Maintains up to `version_count` versions per file in
   cloud storage. Older versions are purged after new uploads.

3. **Space management**: When free space falls below `target_free_space`, evicts
   cold files (not accessed in the last hour) that are safely checkpointed.
   Files are evicted oldest-first.

4. **Checkpoint persistence**: After any changes, saves the checkpoint to disk
   and uploads it to cloud storage.

### On-Demand Space Reclamation

When a program tries to create a new file and free space is below `target_free_space`:

1. The `create()` call triggers an immediate on-demand sweep
2. The sweep targets 150% of `target_free_space` to provide headroom
3. The `create()` blocks until the sweep completes
4. If sufficient space cannot be freed (no evictable files), returns `ENOSPC`

This ensures that file creation only fails when space genuinely cannot be reclaimed,
while avoiding unnecessary blocking when space is available.

### Recovery

On startup, BSFS:
1. Loads the checkpoint file (or creates a new one)
2. Replays any log entries to restore state
3. Validates timestamps - if any are in the future:
   - With `inconsistent_start: false`: Refuses to start
   - With `inconsistent_start: true`: Adjusts all timestamps backward by the delta
4. Checks for files that were open during a crash (accessed_time > closed_time)
   and recalculates their SHA256 to detect modifications

## Dependencies

- Linux with FUSE support
- Google Cloud Storage bucket with appropriate permissions
