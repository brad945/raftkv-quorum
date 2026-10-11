//! A pure Raft state machine (design: docs/design.md).
//!
//! This crate never touches a disk, a socket, a clock or a thread. Callers
//! drive it with ticks, peer messages and proposals; it answers with a
//! [`Ready`] that says what to persist, what to send and what to apply,
//! and the caller must make the persist part durable before sending.

pub mod codec;
pub mod log;
pub mod node;

pub use node::{Bugs, Config, Envelope, Message, ProposeError, RaftNode, Ready, Role};

/// A Raft term.
pub type Term = u64;
/// A position in the replicated log, starting at 1 (0 means "nothing").
pub type Index = u64;
/// A node's id within the cluster.
pub type NodeId = u64;

/// One entry of the replicated log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    pub index: Index,
    pub kind: EntryKind,
}

/// What an entry carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryKind {
    /// Appended by a new leader at the start of its term so it can commit
    /// earlier entries (paper §5.4.2: a leader only commits entries of its
    /// own term by counting replicas).
    Noop,
    /// An opaque state-machine command.
    Command(Vec<u8>),
}

/// The state Raft must make durable before acting on it (Figure 2:
/// "persistent state on all servers", minus the log).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

/// A snapshot of the state machine up to and including `last_index`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub last_index: Index,
    pub last_term: Term,
    pub data: Vec<u8>,
}

impl Entry {
    pub fn encode(&self, w: &mut codec::Writer) {
        w.u64(self.term);
        w.u64(self.index);
        match &self.kind {
            EntryKind::Noop => w.u8(0),
            EntryKind::Command(data) => {
                w.u8(1);
                w.bytes(data);
            }
        }
    }

    pub fn decode(r: &mut codec::Reader) -> Result<Self, codec::Error> {
        let term = r.u64()?;
        let index = r.u64()?;
        let kind = match r.u8()? {
            0 => EntryKind::Noop,
            1 => EntryKind::Command(r.bytes()?.to_vec()),
            t => return Err(codec::Error::BadTag(t)),
        };
        Ok(Entry { term, index, kind })
    }
}

impl HardState {
    pub fn encode(&self, w: &mut codec::Writer) {
        w.u64(self.term);
        w.opt_u64(self.voted_for);
    }

    pub fn decode(r: &mut codec::Reader) -> Result<Self, codec::Error> {
        Ok(HardState { term: r.u64()?, voted_for: r.opt_u64()? })
    }
}

impl Snapshot {
    pub fn encode(&self, w: &mut codec::Writer) {
        w.u64(self.last_index);
        w.u64(self.last_term);
        w.bytes(&self.data);
    }

    pub fn decode(r: &mut codec::Reader) -> Result<Self, codec::Error> {
        Ok(Snapshot { last_index: r.u64()?, last_term: r.u64()?, data: r.bytes()?.to_vec() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_round_trips() {
        for e in [
            Entry { term: 3, index: 7, kind: EntryKind::Noop },
            Entry { term: u64::MAX, index: 1, kind: EntryKind::Command(b"put k v".to_vec()) },
            Entry { term: 1, index: 2, kind: EntryKind::Command(Vec::new()) },
        ] {
            let mut w = codec::Writer::default();
            e.encode(&mut w);
            let buf = w.finish();
            let mut r = codec::Reader::new(&buf);
            assert_eq!(Entry::decode(&mut r).unwrap(), e);
            assert!(r.is_empty());
        }
    }

    #[test]
    fn hard_state_and_snapshot_round_trip() {
        let hs = HardState { term: 9, voted_for: Some(2) };
        let mut w = codec::Writer::default();
        hs.encode(&mut w);
        assert_eq!(HardState::decode(&mut codec::Reader::new(&w.finish())).unwrap(), hs);
        let none = HardState { term: 1, voted_for: None };
        let mut w = codec::Writer::default();
        none.encode(&mut w);
        assert_eq!(HardState::decode(&mut codec::Reader::new(&w.finish())).unwrap(), none);
        let s = Snapshot { last_index: 40, last_term: 5, data: vec![1, 2, 3] };
        let mut w = codec::Writer::default();
        s.encode(&mut w);
        assert_eq!(Snapshot::decode(&mut codec::Reader::new(&w.finish())).unwrap(), s);
    }

    #[test]
    fn truncated_input_is_an_error_not_a_panic() {
        let mut w = codec::Writer::default();
        Entry { term: 1, index: 1, kind: EntryKind::Command(b"abc".to_vec()) }.encode(&mut w);
        let buf = w.finish();
        for cut in 0..buf.len() {
            assert!(Entry::decode(&mut codec::Reader::new(&buf[..cut])).is_err(), "cut at {cut}");
        }
    }
}
