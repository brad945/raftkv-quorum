//! The replicated state machine: a key-value map plus client sessions
//! (design §4; DECISIONS 2026-10-11 "Client sessions").
//!
//! Every command carries `(client_id, seq)`. A client sends one request at
//! a time and retries it with the same `seq` until it gets an answer. The
//! machine remembers each client's last `seq` and the reply it gave, and
//! answers a retry with that saved reply instead of applying it again:
//! otherwise a retried `Put` could land after a newer write.
//!
//! Every map is a `BTreeMap` so iteration (and therefore snapshots) is
//! deterministic.

use raft_core::codec::{self, Reader, Writer};
use std::collections::BTreeMap;

pub type ClientId = u64;

/// A client operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Get(String),
    Put(String, String),
    Delete(String),
}

/// A command as replicated through the log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command {
    pub client: ClientId,
    pub seq: u64,
    pub op: Op,
}

/// The machine's answer to a command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// Get: the value, if the key exists.
    Value(Option<String>),
    /// Put: done.
    Written,
    /// Delete: whether the key existed.
    Deleted(bool),
    /// A seq older than the client's last one: the client already moved on,
    /// so this is a stray duplicate and is not applied.
    Stale,
}

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::default();
        w.u64(self.client);
        w.u64(self.seq);
        match &self.op {
            Op::Get(k) => {
                w.u8(0);
                w.str(k);
            }
            Op::Put(k, v) => {
                w.u8(1);
                w.str(k);
                w.str(v);
            }
            Op::Delete(k) => {
                w.u8(2);
                w.str(k);
            }
        }
        w.finish()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, codec::Error> {
        let mut r = Reader::new(buf);
        let client = r.u64()?;
        let seq = r.u64()?;
        let op = match r.u8()? {
            0 => Op::Get(r.str()?.to_string()),
            1 => Op::Put(r.str()?.to_string(), r.str()?.to_string()),
            2 => Op::Delete(r.str()?.to_string()),
            t => return Err(codec::Error::BadTag(t)),
        };
        Ok(Command { client, seq, op })
    }
}

fn encode_reply(w: &mut Writer, r: &Reply) {
    match r {
        Reply::Value(None) => w.u8(0),
        Reply::Value(Some(v)) => {
            w.u8(1);
            w.str(v);
        }
        Reply::Written => w.u8(2),
        Reply::Deleted(b) => w.u8(if *b { 3 } else { 4 }),
        Reply::Stale => w.u8(5),
    }
}

fn decode_reply(r: &mut Reader) -> Result<Reply, codec::Error> {
    Ok(match r.u8()? {
        0 => Reply::Value(None),
        1 => Reply::Value(Some(r.str()?.to_string())),
        2 => Reply::Written,
        3 => Reply::Deleted(true),
        4 => Reply::Deleted(false),
        5 => Reply::Stale,
        t => return Err(codec::Error::BadTag(t)),
    })
}

/// The key-value state machine.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Kv {
    data: BTreeMap<String, String>,
    /// Per client: the last applied seq and the reply it got.
    sessions: BTreeMap<ClientId, (u64, Reply)>,
}

impl Kv {
    /// Applies a committed command and returns its reply. A retry of the
    /// client's last command gets the saved reply and changes nothing.
    pub fn apply(&mut self, cmd: &Command) -> Reply {
        if let Some((last, reply)) = self.sessions.get(&cmd.client) {
            if cmd.seq == *last {
                return reply.clone();
            }
            if cmd.seq < *last {
                return Reply::Stale;
            }
        }
        let reply = match &cmd.op {
            Op::Get(k) => Reply::Value(self.data.get(k).cloned()),
            Op::Put(k, v) => {
                self.data.insert(k.clone(), v.clone());
                Reply::Written
            }
            Op::Delete(k) => Reply::Deleted(self.data.remove(k).is_some()),
        };
        self.sessions.insert(cmd.client, (cmd.seq, reply.clone()));
        reply
    }

    pub fn get(&self, k: &str) -> Option<&String> {
        self.data.get(k)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// The machine's full state, including sessions (they are replicated
    /// state: a follower that restores a snapshot must dedup the same way).
    pub fn snapshot(&self) -> Vec<u8> {
        let mut w = Writer::default();
        w.u64(self.data.len() as u64);
        for (k, v) in &self.data {
            w.str(k);
            w.str(v);
        }
        w.u64(self.sessions.len() as u64);
        for (c, (seq, reply)) in &self.sessions {
            w.u64(*c);
            w.u64(*seq);
            encode_reply(&mut w, reply);
        }
        w.finish()
    }

    pub fn restore(buf: &[u8]) -> Result<Self, codec::Error> {
        let mut r = Reader::new(buf);
        let mut kv = Kv::default();
        for _ in 0..r.u64()? {
            let k = r.str()?.to_string();
            kv.data.insert(k, r.str()?.to_string());
        }
        for _ in 0..r.u64()? {
            let c = r.u64()?;
            let seq = r.u64()?;
            kv.sessions.insert(c, (seq, decode_reply(&mut r)?));
        }
        if !r.is_empty() {
            return Err(codec::Error::Short); // trailing bytes: not a snapshot we wrote
        }
        Ok(kv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(client: u64, seq: u64, op: Op) -> Command {
        Command { client, seq, op }
    }
    fn put(k: &str, v: &str) -> Op {
        Op::Put(k.into(), v.into())
    }

    #[test]
    fn get_put_delete() {
        let mut kv = Kv::default();
        assert_eq!(kv.apply(&cmd(1, 1, Op::Get("a".into()))), Reply::Value(None));
        assert_eq!(kv.apply(&cmd(1, 2, put("a", "1"))), Reply::Written);
        assert_eq!(kv.apply(&cmd(1, 3, Op::Get("a".into()))), Reply::Value(Some("1".into())));
        assert_eq!(kv.apply(&cmd(1, 4, Op::Delete("a".into()))), Reply::Deleted(true));
        assert_eq!(kv.apply(&cmd(1, 5, Op::Delete("a".into()))), Reply::Deleted(false));
    }

    // The reason sessions exist: client 1's Put is committed twice (a retry
    // after a timeout), with client 2's newer Put in between. Without dedup
    // the retry would overwrite the newer value.
    #[test]
    fn retried_write_is_not_applied_twice() {
        let mut kv = Kv::default();
        kv.apply(&cmd(1, 1, put("x", "old")));
        kv.apply(&cmd(2, 1, put("x", "new")));
        assert_eq!(kv.apply(&cmd(1, 1, put("x", "old"))), Reply::Written, "a retry gets the saved reply");
        assert_eq!(kv.get("x").map(String::as_str), Some("new"), "the retry changed nothing");
    }

    #[test]
    fn retried_read_returns_the_original_answer() {
        let mut kv = Kv::default();
        kv.apply(&cmd(2, 1, put("x", "a")));
        assert_eq!(kv.apply(&cmd(1, 1, Op::Get("x".into()))), Reply::Value(Some("a".into())));
        kv.apply(&cmd(2, 2, put("x", "b")));
        assert_eq!(kv.apply(&cmd(1, 1, Op::Get("x".into()))), Reply::Value(Some("a".into())));
    }

    #[test]
    fn older_seq_is_stale_and_not_applied() {
        let mut kv = Kv::default();
        kv.apply(&cmd(1, 5, put("x", "five")));
        assert_eq!(kv.apply(&cmd(1, 4, put("x", "four"))), Reply::Stale);
        assert_eq!(kv.get("x").map(String::as_str), Some("five"));
    }

    #[test]
    fn command_round_trips() {
        for c in [cmd(7, 9, Op::Get("k".into())), cmd(1, 2, put("k", "v")), cmd(u64::MAX, 0, Op::Delete("".into()))] {
            assert_eq!(Command::decode(&c.encode()).unwrap(), c);
        }
        assert!(Command::decode(&[1, 2, 3]).is_err());
    }

    #[test]
    fn snapshot_restores_data_and_sessions() {
        let mut kv = Kv::default();
        kv.apply(&cmd(1, 1, put("a", "1")));
        kv.apply(&cmd(2, 3, Op::Delete("zz".into())));
        kv.apply(&cmd(3, 1, Op::Get("a".into())));
        let restored = Kv::restore(&kv.snapshot()).unwrap();
        assert_eq!(restored, kv);
        // Dedup survives the snapshot.
        let mut r = restored;
        assert_eq!(r.apply(&cmd(1, 1, put("a", "other"))), Reply::Written);
        assert_eq!(r.get("a").map(String::as_str), Some("1"));
        // Same state, same bytes: snapshots are deterministic.
        assert_eq!(Kv::restore(&kv.snapshot()).unwrap().snapshot(), kv.snapshot());
        assert!(Kv::restore(&[0, 1]).is_err());
    }
}
