Backup Storage File System

This is a userspace FS that implements backup file storage.
It is pointed at a local disk where new files go, and a cloud storage bucket where
files are archived.
If files have not been opened for a while, the files are removed from the local device
to keep it from becoming full.
Metadata is still stored about the files locally.
When a file is opened, and not yet available locally, the file is pulled back in from
cloud storage.
Old versions of files are kept in cloud storage too, so the file system can time travel.

When mounting the ufs, the mount options may include a config file name, which specifies
properties for the implementation:
- local directory root
- cloud storage credentials (inline key, or file path to key on disk)
- target free space (in bytes)
- frequency of sweeping files that need archiving (in seconds)
- number of versions of backup files stored

Local file metadata is stored in two files:
- a full state file, which stores the known state of each inode
- a metadata log, which appends one record per open/close
These are stored in the root of the local storage. Each time the archive files are swept,
the checkpoint is re-written (into a new file) and atomically renamed into place, after
which the log is truncated.
When the handler process starts up, it recovers by loading the checkpoint, then loading
and replaying each record in the log, to bring the in-memory state up to date.

Inode numbers are allocated starting at 1 (for the root directory) and incrementing.
Local files are stored with a filename based on their inode number. The subdirectory
path in local storage is constructed as:

def expand_inode(n: int) -> str:
    s = str(n)
    length = len(s)
    parts = []

    for i in range(1, length + 1):
        prefix = s[:i]
        padded = prefix + "0" * (length - i)
        parts.append(padded)

    return "/".join(parts)

For example, the inode with value 1234567 would be stored in:
  1000000/1200000/1230000/1234000/1234500/1234560/1234567
The inode with value 3210 would be stored in:
  3000/3200/3210/3210

The metadata for a directory entry is:
- inode
- filename (max 255 characters)
- modebits (regular, directory, permissions)
- owner/group ids
- createdtime (struct timeval)
- mutatedtime (struct timeval)
- accessedtime (struct timeval)
- closedtime (struct timeval)
- checkpointedtime (struct timeval or null)
- checkpointed_sha256 (binary or null)
- crc of the entry (entry is invalid if crc doesn't match)
Note that the filesystem needs to store accessedtime even when mounted without atime.
This is to support the demotion to cold storage when not accessed after some time.
Accessedtime changes on open and close of the file
Mutatedtime changes on close of the file if the file was written to
Closedtime changes on close of the file
The checkpointed sha is the SHA256 of the last checkpointed version of the file, and is null
until checkpointed the first time.

When the daemon starts up, if there is a record with accessedtime greater than closedtime,
the daemon will start an asynchronous operation to calculate the sha256 of the file on disk,
and compare it to the previous sha. If the sha is different, the mutatedtime is updated to
be the current time.
If, on daemon start, any timestamp is found that's after the current time, the daemon will
refuse to start, and print a list of the records that mis-match. If a flag named
"inconsistent_start" is set to true in the settings, those timestamps are instead mutated
so that the latest timestamp field in the record is the current time, and each
other timestamp field is moved backwards to be the same relative time to that
time; i e the same delta is subtracted from each of the timestamps.

The initial version supports only google cloud storage for cold storage, and
supports using either ambient credentials of running on a GCP VM, or a service
account JSON file as credential key.
