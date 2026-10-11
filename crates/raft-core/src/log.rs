//! The in-memory Raft log with a snapshot offset.
//!
//! Indices `1..=snapshot_index` are compacted into the snapshot; only their
//! last term is kept (`snapshot_term`). `entries[0]` has index
//! `snapshot_index + 1`. Every index computation goes through this type, so
//! the off-by-one rules live in one place (design §8: snapshot offsets).

use crate::{Entry, Index, Snapshot, Term};

#[derive(Clone, Debug, Default)]
pub struct Log {
    snapshot_index: Index,
    snapshot_term: Term,
    /// The snapshot itself, if this node has one (sent to followers that
    /// fall behind it).
    snapshot: Option<Snapshot>,
    entries: Vec<Entry>,
}

impl Log {
    /// A log after the snapshot at (`snapshot_index`, `snapshot_term`).
    pub fn new(snapshot_index: Index, snapshot_term: Term, entries: Vec<Entry>) -> Self {
        let mut l = Log { snapshot_index, snapshot_term, snapshot: None, entries: Vec::new() };
        for e in entries {
            l.push(e);
        }
        l
    }

    /// Restores the snapshot's data too, so the node can send it.
    pub fn with_snapshot(mut self, s: Option<Snapshot>) -> Self {
        self.snapshot = s;
        self
    }

    pub fn snapshot_index(&self) -> Index {
        self.snapshot_index
    }
    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }

    pub fn last_index(&self) -> Index {
        self.entries.last().map_or(self.snapshot_index, |e| e.index)
    }

    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(self.snapshot_term, |e| e.term)
    }

    /// The term of the entry at `index`: the snapshot's term at its own
    /// index, 0 at index 0, `None` if compacted away or past the end.
    pub fn term_at(&self, index: Index) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.entry(index).map(|e| e.term)
    }

    /// The entry at `index`, if it is held (not compacted, not past the end).
    pub fn entry(&self, index: Index) -> Option<&Entry> {
        if index <= self.snapshot_index {
            return None;
        }
        self.entries.get((index - self.snapshot_index - 1) as usize)
    }

    /// Up to `max` entries starting at `from`.
    pub fn slice(&self, from: Index, max: usize) -> Vec<Entry> {
        if from <= self.snapshot_index {
            return Vec::new();
        }
        let start = (from - self.snapshot_index - 1) as usize;
        self.entries.iter().skip(start).take(max).cloned().collect()
    }

    /// Appends one entry; it must be the next index.
    pub fn push(&mut self, e: Entry) {
        assert_eq!(e.index, self.last_index() + 1, "log entries must be contiguous");
        self.entries.push(e);
    }

    /// Drops every entry at or after `from` (never the snapshot).
    pub fn truncate_from(&mut self, from: Index) {
        assert!(from > self.snapshot_index, "truncating at {from}, inside the snapshot at {}", self.snapshot_index);
        self.entries.retain(|e| e.index < from);
    }

    /// Installs a snapshot. Paper §7: if an entry at the snapshot's last
    /// index has the snapshot's term, the entries after it are kept;
    /// otherwise the whole log is discarded. Same rule as the storage.
    pub fn install_snapshot(&mut self, s: &Snapshot) {
        if s.last_index <= self.snapshot_index {
            return;
        }
        let keep = self.term_at(s.last_index) == Some(s.last_term);
        if keep {
            self.entries.retain(|e| e.index > s.last_index);
        } else {
            self.entries.clear();
        }
        self.snapshot_index = s.last_index;
        self.snapshot_term = s.last_term;
        self.snapshot = Some(s.clone());
    }

    /// The snapshot to send a follower that is behind it.
    pub fn snapshot_for_send(&self) -> Option<Snapshot> {
        self.snapshot.clone()
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EntryKind;

    fn e(term: Term, index: Index) -> Entry {
        Entry { term, index, kind: EntryKind::Noop }
    }

    #[test]
    fn offsets_without_a_snapshot() {
        let l = Log::new(0, 0, vec![e(1, 1), e(1, 2), e(2, 3)]);
        assert_eq!((l.last_index(), l.last_term()), (3, 2));
        assert_eq!(l.term_at(0), Some(0));
        assert_eq!(l.term_at(2), Some(1));
        assert_eq!(l.term_at(4), None);
        assert_eq!(l.slice(2, 10), vec![e(1, 2), e(2, 3)]);
        assert_eq!(l.slice(2, 1), vec![e(1, 2)]);
        assert!(l.slice(4, 10).is_empty());
    }

    // design §8 snapshot_offsets: around the snapshot boundary.
    #[test]
    fn snapshot_offsets() {
        let l = Log::new(5, 3, vec![e(3, 6), e(4, 7)]);
        assert_eq!(l.term_at(5), Some(3), "the snapshot point keeps its term");
        assert_eq!(l.term_at(4), None, "compacted");
        assert_eq!(l.entry(5), None, "the snapshot point is not an entry");
        assert_eq!(l.entry(6), Some(&e(3, 6)));
        assert_eq!(l.slice(5, 10), Vec::<Entry>::new(), "can't send compacted entries");
        assert_eq!(l.slice(6, 10), vec![e(3, 6), e(4, 7)]);
        assert_eq!((l.last_index(), l.last_term()), (7, 4));
        let empty = Log::new(5, 3, Vec::new());
        assert_eq!((empty.last_index(), empty.last_term()), (5, 3));
    }

    #[test]
    fn install_keeps_a_matching_tail_and_drops_a_conflicting_log() {
        let mut l = Log::new(0, 0, (1..=5).map(|i| e(1, i)).collect());
        l.install_snapshot(&Snapshot { last_index: 3, last_term: 1, data: Vec::new() });
        assert_eq!(l.entries(), &[e(1, 4), e(1, 5)]);
        let mut l2 = Log::new(0, 0, (1..=5).map(|i| e(1, i)).collect());
        l2.install_snapshot(&Snapshot { last_index: 3, last_term: 2, data: Vec::new() });
        assert!(l2.entries().is_empty());
        assert_eq!((l2.last_index(), l2.last_term()), (3, 2));
        // An older snapshot changes nothing.
        l.install_snapshot(&Snapshot { last_index: 2, last_term: 1, data: Vec::new() });
        assert_eq!(l.snapshot_index(), 3);
    }

    #[test]
    #[should_panic(expected = "contiguous")]
    fn gaps_panic() {
        let mut l = Log::new(0, 0, vec![e(1, 1)]);
        l.push(e(1, 3));
    }
}
