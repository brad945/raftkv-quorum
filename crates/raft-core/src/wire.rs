//! The on-wire form of a message (milestone R6). The TCP runtime frames
//! each encoded [`Envelope`] as `[len u32][payload]`; this module is only
//! the payload. Decoding never panics: a socket may carry anything.

use crate::codec::{Error, Reader, Writer};
use crate::{Entry, Envelope, Message, Snapshot};

impl Envelope {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::default();
        w.u64(self.from);
        w.u64(self.to);
        match &self.msg {
            Message::RequestVote { term, last_index, last_term } => {
                w.u8(0);
                w.u64(*term);
                w.u64(*last_index);
                w.u64(*last_term);
            }
            Message::RequestVoteResp { term, granted } => {
                w.u8(1);
                w.u64(*term);
                w.u8(u8::from(*granted));
            }
            Message::AppendEntries { term, prev_index, prev_term, entries, leader_commit } => {
                w.u8(2);
                w.u64(*term);
                w.u64(*prev_index);
                w.u64(*prev_term);
                w.u64(*leader_commit);
                w.u32(entries.len() as u32);
                for e in entries {
                    e.encode(&mut w);
                }
            }
            Message::AppendEntriesResp { term, success, match_index, hint, rejected } => {
                w.u8(3);
                w.u64(*term);
                w.u8(u8::from(*success));
                w.u64(*match_index);
                w.u64(*hint);
                w.u64(*rejected);
            }
            Message::InstallSnapshot { term, snapshot } => {
                w.u8(4);
                w.u64(*term);
                snapshot.encode(&mut w);
            }
            Message::InstallSnapshotResp { term, last_index } => {
                w.u8(5);
                w.u64(*term);
                w.u64(*last_index);
            }
        }
        w.finish()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        let mut r = Reader::new(buf);
        let from = r.u64()?;
        let to = r.u64()?;
        let msg = match r.u8()? {
            0 => Message::RequestVote { term: r.u64()?, last_index: r.u64()?, last_term: r.u64()? },
            1 => Message::RequestVoteResp { term: r.u64()?, granted: bool_(&mut r)? },
            2 => {
                let (term, prev_index, prev_term, leader_commit) = (r.u64()?, r.u64()?, r.u64()?, r.u64()?);
                let n = r.u32()?;
                // No preallocation from an untrusted count: each entry
                // consumes input, so a bogus count just runs out (`Short`).
                let mut entries = Vec::new();
                for _ in 0..n {
                    entries.push(Entry::decode(&mut r)?);
                }
                Message::AppendEntries { term, prev_index, prev_term, entries, leader_commit }
            }
            3 => Message::AppendEntriesResp { term: r.u64()?, success: bool_(&mut r)?, match_index: r.u64()?, hint: r.u64()?, rejected: r.u64()? },
            4 => Message::InstallSnapshot { term: r.u64()?, snapshot: Snapshot::decode(&mut r)? },
            5 => Message::InstallSnapshotResp { term: r.u64()?, last_index: r.u64()? },
            t => return Err(Error::BadTag(t)),
        };
        if !r.is_empty() {
            return Err(Error::Trailing);
        }
        Ok(Envelope { from, to, msg })
    }
}

fn bool_(r: &mut Reader) -> Result<bool, Error> {
    match r.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        t => Err(Error::BadTag(t)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EntryKind;

    fn all() -> Vec<Envelope> {
        let entries = vec![Entry { term: 3, index: 7, kind: EntryKind::Noop }, Entry { term: 3, index: 8, kind: EntryKind::Command(b"put k v".to_vec()) }];
        [
            Message::RequestVote { term: 4, last_index: 9, last_term: 3 },
            Message::RequestVoteResp { term: 4, granted: true },
            Message::RequestVoteResp { term: 4, granted: false },
            Message::AppendEntries { term: 4, prev_index: 6, prev_term: 2, entries, leader_commit: 5 },
            Message::AppendEntries { term: 4, prev_index: 6, prev_term: 2, entries: vec![], leader_commit: 5 },
            Message::AppendEntriesResp { term: 4, success: false, match_index: 0, hint: 3, rejected: 6 },
            Message::InstallSnapshot { term: 4, snapshot: Snapshot { last_index: 100, last_term: 3, data: vec![1, 2, 3] } },
            Message::InstallSnapshotResp { term: 4, last_index: 100 },
        ]
        .into_iter()
        .map(|msg| Envelope { from: 1, to: u64::MAX, msg })
        .collect()
    }

    #[test]
    fn every_message_round_trips() {
        for m in all() {
            assert_eq!(Envelope::decode(&m.encode()), Ok(m.clone()), "{m:?}");
        }
    }

    #[test]
    fn truncated_or_extended_frames_are_errors() {
        for m in all() {
            let b = m.encode();
            for cut in 0..b.len() {
                assert!(Envelope::decode(&b[..cut]).is_err(), "{m:?} cut at {cut}");
            }
            let mut longer = b.clone();
            longer.push(0);
            assert_eq!(Envelope::decode(&longer), Err(Error::Trailing));
        }
    }

    #[test]
    fn garbage_never_panics() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for len in 0..2_000 {
            let buf: Vec<u8> = (0..len % 97)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect();
            let _ = Envelope::decode(&buf);
        }
        // A huge entry count with no entries behind it.
        let mut w = Writer::default();
        w.u64(1);
        w.u64(2);
        w.u8(2);
        for _ in 0..4 {
            w.u64(0);
        }
        w.u32(u32::MAX);
        assert_eq!(Envelope::decode(&w.finish()), Err(Error::Short));
    }
}
