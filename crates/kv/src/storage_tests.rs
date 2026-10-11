//! Crash-safety tests for `DiskStorage` (design §8: wal_torn_tail_truncated,
//! wal_corrupt_byte_detected, and the snapshot/compaction cases).

use crate::storage::{DiskStorage, StorageError, SyncMode};
use raft_core::{Entry, EntryKind, HardState, Snapshot};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

/// A fresh, empty directory for one test (removed when dropped).
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!("raftkv-test-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    fn wal(&self) -> PathBuf {
        self.0.join("wal")
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn e(term: u64, index: u64, data: &str) -> Entry {
    Entry { term, index, kind: EntryKind::Command(data.as_bytes().to_vec()) }
}

fn open(d: &TempDir) -> DiskStorage {
    DiskStorage::open(&d.0, SyncMode::Fsync).unwrap().0
}

#[test]
fn fresh_directory_is_empty() {
    let d = TempDir::new();
    let (s, r) = DiskStorage::open(&d.0, SyncMode::Fsync).unwrap();
    assert_eq!(r.hard, HardState::default());
    assert!(r.snapshot.is_none() && r.entries.is_empty() && r.torn_bytes == 0);
    assert!(s.entries().is_empty());
}

#[test]
fn hard_state_survives_reopen() {
    let d = TempDir::new();
    let mut s = open(&d);
    s.save_hard_state(&HardState { term: 4, voted_for: Some(2) }).unwrap();
    s.save_hard_state(&HardState { term: 5, voted_for: None }).unwrap();
    drop(s);
    let (_, r) = DiskStorage::open(&d.0, SyncMode::Fsync).unwrap();
    assert_eq!(r.hard, HardState { term: 5, voted_for: None });
}

#[test]
fn leftover_temp_file_is_ignored() {
    // A crash after writing hardstate.tmp but before the rename: the old
    // hard state is still the truth.
    let d = TempDir::new();
    let mut s = open(&d);
    s.save_hard_state(&HardState { term: 2, voted_for: Some(1) }).unwrap();
    drop(s);
    fs::write(d.0.join("hardstate.tmp"), b"half-written garbage").unwrap();
    let (_, r) = DiskStorage::open(&d.0, SyncMode::Fsync).unwrap();
    assert_eq!(r.hard, HardState { term: 2, voted_for: Some(1) });
}

#[test]
fn appended_entries_survive_reopen() {
    let d = TempDir::new();
    let mut s = open(&d);
    s.append(None, &[e(1, 1, "a"), e(1, 2, "b")]).unwrap();
    s.append(None, &[e(2, 3, "c")]).unwrap();
    drop(s);
    let (s, r) = DiskStorage::open(&d.0, SyncMode::Fsync).unwrap();
    assert_eq!(r.entries, vec![e(1, 1, "a"), e(1, 2, "b"), e(2, 3, "c")]);
    assert_eq!(s.entries(), r.entries.as_slice());
}

#[test]
fn conflict_truncation_survives_reopen() {
    let d = TempDir::new();
    let mut s = open(&d);
    s.append(None, &[e(1, 1, "a"), e(1, 2, "b"), e(1, 3, "c")]).unwrap();
    // A new leader's log disagrees from index 2.
    s.append(Some(2), &[e(2, 2, "B")]).unwrap();
    assert_eq!(s.entries(), &[e(1, 1, "a"), e(2, 2, "B")]);
    drop(s);
    assert_eq!(DiskStorage::open(&d.0, SyncMode::Fsync).unwrap().1.entries, vec![e(1, 1, "a"), e(2, 2, "B")]);
}

#[test]
fn gaps_are_refused_and_nothing_is_written() {
    let d = TempDir::new();
    let mut s = open(&d);
    s.append(None, &[e(1, 1, "a")]).unwrap();
    assert!(matches!(s.append(None, &[e(1, 3, "gap")]), Err(StorageError::Corrupt(_))));
    assert_eq!(s.entries(), &[e(1, 1, "a")]);
    drop(s);
    assert_eq!(DiskStorage::open(&d.0, SyncMode::Fsync).unwrap().1.entries, vec![e(1, 1, "a")]);
}

// A crash mid-write tears the last record. Every possible cut point inside
// it must recover the earlier entries and drop only the torn one; the log
// then keeps working.
#[test]
fn wal_torn_tail_truncated() {
    let d0 = TempDir::new();
    let mut s = open(&d0);
    s.append(None, &[e(1, 1, "first"), e(1, 2, "second")]).unwrap();
    let two = fs::metadata(d0.wal()).unwrap().len();
    s.append(None, &[e(1, 3, "third, which the crash tears")]).unwrap();
    let three = fs::metadata(d0.wal()).unwrap().len();
    drop(s);
    let full = fs::read(d0.wal()).unwrap();
    for cut in two + 1..three {
        let d = TempDir::new();
        fs::write(d.wal(), &full[..cut as usize]).unwrap();
        let (mut s, r) = DiskStorage::open(&d.0, SyncMode::Fsync).unwrap();
        assert_eq!(r.entries, vec![e(1, 1, "first"), e(1, 2, "second")], "cut at {cut}");
        assert_eq!(r.torn_bytes, cut - two, "cut at {cut}");
        assert_eq!(fs::metadata(d.wal()).unwrap().len(), two, "the torn tail is cut from the file");
        s.append(None, &[e(2, 3, "after recovery")]).unwrap();
        drop(s);
        assert_eq!(DiskStorage::open(&d.0, SyncMode::Fsync).unwrap().1.entries.last(), Some(&e(2, 3, "after recovery")));
    }
}

// A flipped byte in the LAST record: a torn write, cut like one.
#[test]
fn wal_corrupt_last_record_is_cut() {
    let d = TempDir::new();
    let mut s = open(&d);
    s.append(None, &[e(1, 1, "a"), e(1, 2, "b")]).unwrap();
    drop(s);
    let mut b = fs::read(d.wal()).unwrap();
    let last = b.len() - 1;
    b[last] ^= 0xFF;
    fs::write(d.wal(), &b).unwrap();
    let (_, r) = DiskStorage::open(&d.0, SyncMode::Fsync).unwrap();
    assert_eq!(r.entries, vec![e(1, 1, "a")]);
    assert!(r.torn_bytes > 0);
}

// A flipped byte in a record with more records after it is NOT a crash
// artifact: cutting there would drop acked entries. Opening must fail
// rather than silently lose them.
#[test]
fn wal_corrupt_byte_detected() {
    let d = TempDir::new();
    let mut s = open(&d);
    s.append(None, &[e(1, 1, "a")]).unwrap();
    let first = fs::metadata(d.wal()).unwrap().len() as usize;
    s.append(None, &[e(1, 2, "b"), e(1, 3, "c")]).unwrap();
    drop(s);
    let mut b = fs::read(d.wal()).unwrap();
    b[first + 10] ^= 0x01; // inside record 2's payload
    fs::write(d.wal(), &b).unwrap();
    match DiskStorage::open(&d.0, SyncMode::Fsync) {
        Err(StorageError::Corrupt(m)) => assert!(m.contains("not a torn tail"), "{m}"),
        Err(other) => panic!("wrong error: {other}"),
        Ok(_) => panic!("opened a corrupt WAL"),
    }
}

#[test]
fn snapshot_compacts_and_keeps_the_tail() {
    let d = TempDir::new();
    let mut s = open(&d);
    s.append(None, &(1..=5).map(|i| e(1, i, "x")).collect::<Vec<_>>()).unwrap();
    s.save_snapshot(&Snapshot { last_index: 3, last_term: 1, data: b"state@3".to_vec() }).unwrap();
    assert_eq!(s.entries(), &[e(1, 4, "x"), e(1, 5, "x")]);
    s.append(None, &[e(2, 6, "y")]).unwrap();
    drop(s);
    let (s, r) = DiskStorage::open(&d.0, SyncMode::Fsync).unwrap();
    assert_eq!(r.snapshot.as_ref().map(|s| s.last_index), Some(3));
    assert_eq!(r.entries, vec![e(1, 4, "x"), e(1, 5, "x"), e(2, 6, "y")]);
    // The compacted WAL holds only the tail.
    assert!(fs::metadata(d.wal()).unwrap().len() < 200);
    drop(s);
}

#[test]
fn snapshot_from_another_history_discards_the_log() {
    // Paper §7: the log has no entry matching the snapshot's last index and
    // term, so none of it can be trusted to follow the snapshot.
    let d = TempDir::new();
    let mut s = open(&d);
    s.append(None, &(1..=4).map(|i| e(1, i, "x")).collect::<Vec<_>>()).unwrap();
    s.save_snapshot(&Snapshot { last_index: 3, last_term: 2, data: b"leader@3".to_vec() }).unwrap();
    assert!(s.entries().is_empty());
    s.append(None, &[e(2, 4, "next")]).unwrap();
    drop(s);
    let r = DiskStorage::open(&d.0, SyncMode::Fsync).unwrap().1;
    assert_eq!(r.entries, vec![e(2, 4, "next")]);
}

#[test]
fn older_snapshot_is_ignored() {
    let d = TempDir::new();
    let mut s = open(&d);
    s.append(None, &(1..=6).map(|i| e(1, i, "x")).collect::<Vec<_>>()).unwrap();
    s.save_snapshot(&Snapshot { last_index: 5, last_term: 1, data: b"@5".to_vec() }).unwrap();
    s.save_snapshot(&Snapshot { last_index: 2, last_term: 1, data: b"@2".to_vec() }).unwrap();
    assert_eq!(s.snapshot().map(|s| s.last_index), Some(5));
    assert_eq!(s.entries(), &[e(1, 6, "x")]);
}

#[test]
fn crash_between_snapshot_and_wal_rewrite_recovers_the_same() {
    // Write the snapshot file but keep the old, uncompacted WAL, as a crash
    // right after the snapshot's rename would.
    let d = TempDir::new();
    let mut s = open(&d);
    s.append(None, &(1..=5).map(|i| e(1, i, "x")).collect::<Vec<_>>()).unwrap();
    drop(s);
    let old_wal = fs::read(d.wal()).unwrap();
    let mut s = open(&d);
    s.save_snapshot(&Snapshot { last_index: 3, last_term: 1, data: b"@3".to_vec() }).unwrap();
    drop(s);
    fs::write(d.wal(), &old_wal).unwrap();
    let r = DiskStorage::open(&d.0, SyncMode::Fsync).unwrap().1;
    assert_eq!(r.snapshot.map(|s| s.last_index), Some(3));
    assert_eq!(r.entries, vec![e(1, 4, "x"), e(1, 5, "x")]);
}

#[test]
fn truncating_inside_the_snapshot_is_refused() {
    let d = TempDir::new();
    let mut s = open(&d);
    s.append(None, &(1..=4).map(|i| e(1, i, "x")).collect::<Vec<_>>()).unwrap();
    s.save_snapshot(&Snapshot { last_index: 3, last_term: 1, data: Vec::new() }).unwrap();
    assert!(s.append(Some(2), &[]).is_err());
}

#[test]
fn full_sync_mode_works_on_this_machine() {
    // F_FULLFSYNC on macOS (falls back to fsync if unsupported).
    let d = TempDir::new();
    let mut s = DiskStorage::open(&d.0, SyncMode::Full).unwrap().0;
    s.append(None, &[e(1, 1, "durable")]).unwrap();
    s.save_hard_state(&HardState { term: 1, voted_for: Some(1) }).unwrap();
    drop(s);
    assert_eq!(DiskStorage::open(&d.0, SyncMode::Full).unwrap().1.entries.len(), 1);
}

// Not a correctness test: measures what each sync mode costs per append on
// this machine. Run with `cargo test -p kv -- --ignored --nocapture`.
#[test]
#[ignore]
fn measure_sync_cost() {
    for mode in [SyncMode::None, SyncMode::Fsync, SyncMode::Full] {
        let d = TempDir::new();
        let mut s = DiskStorage::open(&d.0, mode).unwrap().0;
        let n = 200;
        let start = std::time::Instant::now();
        for i in 1..=n {
            s.append(None, &[e(1, i, "a 64-byte-ish payload for measuring durable appends.....")]).unwrap();
        }
        let per = start.elapsed() / n as u32;
        println!("{mode:?}: {per:?} per durable append ({n} appends)");
    }
}
