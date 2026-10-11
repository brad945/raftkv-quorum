//! Crash-safe durable storage for one Raft node (design §5).
//!
//! Three files in the node's directory:
//!
//! - `hardstate`: term and vote. Replaced atomically: written to a temp
//!   file, synced, renamed over the old one, then the directory is synced
//!   so the rename itself survives a crash.
//! - `wal`: the log, as an append-only sequence of records
//!   `[len u32][crc32 u32][payload]`. A record is an `Append(entry)` or a
//!   `TruncateFrom(index)` (a Raft conflict cut the log). On recovery the
//!   records are replayed. A short or failing record at the very end is a
//!   torn tail from a crash mid-write, and the file is cut there; a failing
//!   record with more data after it is corruption, and opening fails.
//! - `snapshot`: the latest snapshot, replaced atomically like `hardstate`.
//!   After a snapshot is saved the WAL is rewritten without the entries it
//!   covers (log compaction).
//!
//! Every mutating call is durable when it returns: the caller (the node
//! driver) relies on that to send a vote or an ack only after the promise
//! is on disk.

use crate::crc::crc32;
use raft_core::codec::{Reader, Writer};
use raft_core::{Entry, HardState, Index, Snapshot};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const HARDSTATE: &str = "hardstate";
const WAL: &str = "wal";
const SNAPSHOT: &str = "snapshot";
const HEADER: usize = 8; // len u32 + crc u32

const REC_APPEND: u8 = 1;
const REC_TRUNCATE: u8 = 2;

/// How hard to push data to the disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    /// Flush through the drive's own cache. On macOS `fsync` alone does not
    /// (the data can sit in the disk's volatile cache), so this uses
    /// `fcntl(F_FULLFSYNC)`; elsewhere it is `fsync`.
    Full,
    /// `fsync` only.
    Fsync,
    /// No syncing: tests that never simulate power loss, for speed.
    None,
}

/// What `open` found on disk: what the node restarts from.
#[derive(Debug, Default)]
pub struct Recovered {
    pub hard: HardState,
    pub snapshot: Option<Snapshot>,
    /// The log after the snapshot, contiguous and in order.
    pub entries: Vec<Entry>,
    /// Bytes cut from the end of the WAL: a torn or corrupt tail.
    pub torn_bytes: u64,
}

/// One node's durable state.
pub struct DiskStorage {
    dir: PathBuf,
    wal: File,
    sync: SyncMode,
    hard: HardState,
    snapshot: Option<Snapshot>,
    entries: Vec<Entry>,
}

/// An error from storage. Corruption that is not a torn tail (a record
/// that checks out but makes no sense) is refused, never silently skipped.
#[derive(Debug)]
pub enum StorageError {
    Io(io::Error),
    Corrupt(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io(e) => write!(f, "storage I/O: {e}"),
            StorageError::Corrupt(m) => write!(f, "storage corrupt: {m}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<io::Error> for StorageError {
    fn from(e: io::Error) -> Self {
        StorageError::Io(e)
    }
}

type Result<T> = std::result::Result<T, StorageError>;

#[cfg(target_os = "macos")]
fn full_sync(f: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    extern "C" {
        fn fcntl(fd: i32, cmd: i32, ...) -> i32;
    }
    const F_FULLFSYNC: i32 = 51;
    // SAFETY: fcntl with F_FULLFSYNC takes no argument and only reads the
    // descriptor, which `f` keeps open for the duration of the call.
    if unsafe { fcntl(f.as_raw_fd(), F_FULLFSYNC) } == -1 {
        // Some file systems do not support it: fall back to fsync.
        return f.sync_all();
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn full_sync(f: &File) -> io::Result<()> {
    f.sync_all()
}

fn sync_file(f: &File, mode: SyncMode) -> io::Result<()> {
    match mode {
        SyncMode::Full => full_sync(f),
        SyncMode::Fsync => f.sync_all(),
        SyncMode::None => Ok(()),
    }
}

fn sync_dir(dir: &Path, mode: SyncMode) -> io::Result<()> {
    if mode == SyncMode::None {
        return Ok(());
    }
    File::open(dir)?.sync_all()
}

/// Writes `data` to `dir/name` atomically: temp file, sync, rename, sync dir.
fn write_atomic(dir: &Path, name: &str, data: &[u8], mode: SyncMode) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        sync_file(&f, mode)?;
    }
    fs::rename(&tmp, dir.join(name))?;
    sync_dir(dir, mode)
}

/// A framed record: `[len][crc][payload]`.
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

fn append_record(e: &Entry) -> Vec<u8> {
    let mut w = Writer::default();
    w.u8(REC_APPEND);
    e.encode(&mut w);
    frame(&w.finish())
}

fn truncate_record(from: Index) -> Vec<u8> {
    let mut w = Writer::default();
    w.u8(REC_TRUNCATE);
    w.u64(from);
    frame(&w.finish())
}

/// The index after the last one held, given a snapshot and the entries.
fn next_index(snapshot: &Option<Snapshot>, entries: &[Entry]) -> Index {
    match entries.last() {
        Some(e) => e.index + 1,
        None => snapshot.as_ref().map_or(1, |s| s.last_index + 1),
    }
}

/// Applies one replayed record to the in-memory log. Entries the snapshot
/// already covers are skipped (a crash between saving a snapshot and
/// rewriting the WAL leaves them behind).
fn replay(rec: &[u8], snapshot: &Option<Snapshot>, entries: &mut Vec<Entry>) -> Result<()> {
    let mut r = Reader::new(rec);
    let bad = |m: String| StorageError::Corrupt(m);
    match r.u8().map_err(|e| bad(format!("record kind: {e}")))? {
        REC_APPEND => {
            let e = Entry::decode(&mut r).map_err(|e| bad(format!("entry: {e}")))?;
            if snapshot.as_ref().is_some_and(|s| e.index <= s.last_index) {
                return Ok(());
            }
            let want = next_index(snapshot, entries);
            if e.index != want {
                return Err(bad(format!("entry {} where {} was expected (the log has a gap)", e.index, want)));
            }
            entries.push(e);
        }
        REC_TRUNCATE => {
            let from = r.u64().map_err(|e| bad(format!("truncate: {e}")))?;
            entries.retain(|e| e.index < from);
        }
        k => return Err(bad(format!("unknown record kind {k}"))),
    }
    if !r.is_empty() {
        return Err(bad("trailing bytes in a record".into()));
    }
    Ok(())
}

impl DiskStorage {
    /// Opens (creating if needed) the storage in `dir` and recovers its
    /// state, cutting a torn WAL tail.
    pub fn open(dir: impl AsRef<Path>, sync: SyncMode) -> Result<(Self, Recovered)> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let hard = match fs::read(dir.join(HARDSTATE)) {
            Ok(b) => HardState::decode(&mut Reader::new(&b)).map_err(|e| StorageError::Corrupt(format!("hardstate: {e}")))?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => HardState::default(),
            Err(e) => return Err(e.into()),
        };
        let snapshot = match fs::read(dir.join(SNAPSHOT)) {
            Ok(b) => Some(Snapshot::decode(&mut Reader::new(&b)).map_err(|e| StorageError::Corrupt(format!("snapshot: {e}")))?),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };

        let wal_path = dir.join(WAL);
        let mut buf = Vec::new();
        match File::open(&wal_path) {
            Ok(mut f) => {
                f.read_to_end(&mut buf)?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let mut entries = Vec::new();
        let mut off = 0usize;
        while off < buf.len() {
            if buf.len() - off < HEADER {
                break; // torn header
            }
            let len = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap());
            let end = match (off + HEADER).checked_add(len) {
                Some(e) if e <= buf.len() => e,
                _ => break, // torn payload (or a garbage length)
            };
            let payload = &buf[off + HEADER..end];
            if crc32(payload) != crc {
                if end < buf.len() {
                    // A crash only tears the end of the file. A bad record
                    // with more data after it is corruption: cutting here
                    // would silently drop entries this node already acked,
                    // which breaks Raft's safety. Refuse to start instead.
                    return Err(StorageError::Corrupt(format!(
                        "WAL record at byte {off} fails its checksum but {} bytes follow it: corruption, not a torn tail",
                        buf.len() - end
                    )));
                }
                break; // the last record is torn
            }
            replay(payload, &snapshot, &mut entries)?;
            off = end;
        }
        let torn_bytes = (buf.len() - off) as u64;

        let wal = OpenOptions::new().create(true).append(true).open(&wal_path)?;
        if torn_bytes > 0 {
            wal.set_len(off as u64)?;
            sync_file(&wal, sync)?;
        }
        let rec = Recovered { hard: hard.clone(), snapshot: snapshot.clone(), entries: entries.clone(), torn_bytes };
        Ok((DiskStorage { dir, wal, sync, hard, snapshot, entries }, rec))
    }

    pub fn hard_state(&self) -> &HardState {
        &self.hard
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// Durably records the term and vote.
    pub fn save_hard_state(&mut self, hs: &HardState) -> Result<()> {
        let mut w = Writer::default();
        hs.encode(&mut w);
        write_atomic(&self.dir, HARDSTATE, &w.finish(), self.sync)?;
        self.hard = hs.clone();
        Ok(())
    }

    /// Durably cuts the log from `truncate_from` (if any), then appends
    /// `entries`. The result must stay contiguous; otherwise nothing is
    /// written.
    pub fn append(&mut self, truncate_from: Option<Index>, entries: &[Entry]) -> Result<()> {
        let mut next = self.entries.clone();
        if let Some(from) = truncate_from {
            if self.snapshot.as_ref().is_some_and(|s| from <= s.last_index) {
                return Err(StorageError::Corrupt(format!("truncating at {from}, inside the snapshot")));
            }
            next.retain(|e| e.index < from);
        }
        for e in entries {
            let want = next_index(&self.snapshot, &next);
            if e.index != want {
                return Err(StorageError::Corrupt(format!("appending entry {} where {} was expected", e.index, want)));
            }
            next.push(e.clone());
        }
        let mut out = Vec::new();
        if let Some(from) = truncate_from {
            out.extend(truncate_record(from));
        }
        for e in entries {
            out.extend(append_record(e));
        }
        if out.is_empty() {
            return Ok(());
        }
        self.wal.write_all(&out)?;
        sync_file(&self.wal, self.sync)?;
        self.entries = next;
        Ok(())
    }

    /// Durably saves a snapshot, then compacts the WAL. Paper §7: if the log
    /// holds an entry at the snapshot's last index with the same term, the
    /// entries after it are kept; otherwise the whole log is discarded.
    /// A snapshot older than the current one is ignored.
    pub fn save_snapshot(&mut self, snap: &Snapshot) -> Result<()> {
        if self.snapshot.as_ref().is_some_and(|s| s.last_index >= snap.last_index) {
            return Ok(());
        }
        let mut w = Writer::default();
        snap.encode(&mut w);
        write_atomic(&self.dir, SNAPSHOT, &w.finish(), self.sync)?;

        let keep = self.entries.iter().any(|e| e.index == snap.last_index && e.term == snap.last_term);
        let tail: Vec<Entry> = if keep { self.entries.iter().filter(|e| e.index > snap.last_index).cloned().collect() } else { Vec::new() };
        let mut out = Vec::new();
        for e in &tail {
            out.extend(append_record(e));
        }
        // A crash before this rename leaves the old WAL: its entries up to
        // the snapshot are skipped on replay, so recovery is the same.
        write_atomic(&self.dir, WAL, &out, self.sync)?;
        self.wal = OpenOptions::new().append(true).open(self.dir.join(WAL))?;
        self.snapshot = Some(snap.clone());
        self.entries = tail;
        Ok(())
    }
}
