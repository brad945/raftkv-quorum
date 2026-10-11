//! A thin TCP runtime (design §1, milestone R6): one process per node, a
//! real disk, real sockets, and the same pure Raft core the simulator
//! drives.
//!
//! One thread owns the node (`RaftNode`, `DiskStorage`, `Kv`) and runs the
//! same loop as the simulator's driver: take one input, then drain
//! `Ready` (persist, then send, then apply). Every other thread only feeds
//! that loop through one channel (ticks, peer messages, client requests)
//! or carries its output (one writer thread per peer). Nothing else
//! touches Raft state, so there are no locks.
//!
//! Wire format, peers and clients alike: `[len u32 LE][payload]`.

use kv::{Command, DiskStorage, Kv, Op, Reply, SyncMode};
use raft_core::codec::{Reader, Writer};
use raft_core::{Config, EntryKind, Envelope, Index, NodeId, ProposeError, RaftNode, Role, Term};
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TrySendError};
use std::thread;
use std::time::{Duration, Instant};

/// Frames larger than this are refused (a snapshot of this small store is
/// far below it); a bad length must not make us allocate gigabytes.
pub const MAX_FRAME: usize = 64 << 20;

pub fn write_frame(w: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    w.write_all(&buf)
}

pub fn read_frame(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("frame of {len} bytes")));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// One cluster member: where its peers reach it, and where clients do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub id: NodeId,
    pub raft: SocketAddr,
    pub client: SocketAddr,
}

/// Parses `1=127.0.0.1:7001/127.0.0.1:8001,2=...`.
pub fn parse_cluster(s: &str) -> Result<Vec<Member>, String> {
    let mut out = Vec::new();
    for part in s.split(',').filter(|p| !p.is_empty()) {
        let (id, addrs) = part.split_once('=').ok_or(format!("{part:?}: expected id=raft_addr/client_addr"))?;
        let (raft, client) = addrs.split_once('/').ok_or(format!("{part:?}: expected id=raft_addr/client_addr"))?;
        let id = id.parse().map_err(|_| format!("{id:?} is not a node id"))?;
        let raft = raft.parse().map_err(|e| format!("{raft:?}: {e}"))?;
        let client = client.parse().map_err(|e| format!("{client:?}: {e}"))?;
        out.push(Member { id, raft, client });
    }
    if out.is_empty() {
        return Err("empty cluster".into());
    }
    Ok(out)
}

/// A client request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// Goes through the log (reads too: design §3).
    Command(Command),
    /// This node's view, answered locally (for demos and tests).
    Status,
}

impl Request {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Request::Command(c) => {
                let mut v = vec![0];
                v.extend(c.encode());
                v
            }
            Request::Status => vec![1],
        }
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        match buf.split_first()? {
            (0, rest) => Command::decode(rest).ok().map(Request::Command),
            (1, []) => Some(Request::Status),
            _ => None,
        }
    }
}

/// A node's answer to a client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    Done(Reply),
    /// Not the leader; try this node.
    Redirect(NodeId),
    /// No leader known, or the outcome is unknown (the entry may still
    /// commit under a new leader): retry with the same `(client, seq)`.
    Retry,
    Status(Status),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    pub id: NodeId,
    pub role: Role,
    pub term: Term,
    pub leader: Option<NodeId>,
    pub commit: Index,
    pub applied: Index,
    pub last_index: Index,
    pub snapshot_index: Index,
    pub keys: u64,
}

impl Response {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::default();
        match self {
            Response::Done(r) => {
                w.u8(0);
                match r {
                    Reply::Value(v) => {
                        w.u8(0);
                        match v {
                            Some(v) => {
                                w.u8(1);
                                w.str(v);
                            }
                            None => w.u8(0),
                        }
                    }
                    Reply::Written => w.u8(1),
                    Reply::Deleted(b) => {
                        w.u8(2);
                        w.u8(u8::from(*b));
                    }
                    Reply::Stale => w.u8(3),
                }
            }
            Response::Redirect(id) => {
                w.u8(1);
                w.u64(*id);
            }
            Response::Retry => w.u8(2),
            Response::Status(s) => {
                w.u8(3);
                w.u64(s.id);
                w.u8(match s.role {
                    Role::Follower => 0,
                    Role::Candidate => 1,
                    Role::Leader => 2,
                });
                w.u64(s.term);
                w.opt_u64(s.leader);
                for v in [s.commit, s.applied, s.last_index, s.snapshot_index, s.keys] {
                    w.u64(v);
                }
            }
        }
        w.finish()
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        let mut r = Reader::new(buf);
        let resp = match r.u8().ok()? {
            0 => Response::Done(match r.u8().ok()? {
                0 => Reply::Value(match r.u8().ok()? {
                    0 => None,
                    1 => Some(r.str().ok()?.to_string()),
                    _ => return None,
                }),
                1 => Reply::Written,
                2 => Reply::Deleted(r.u8().ok()? == 1),
                3 => Reply::Stale,
                _ => return None,
            }),
            1 => Response::Redirect(r.u64().ok()?),
            2 => Response::Retry,
            3 => Response::Status(Status {
                id: r.u64().ok()?,
                role: match r.u8().ok()? {
                    0 => Role::Follower,
                    1 => Role::Candidate,
                    2 => Role::Leader,
                    _ => return None,
                },
                term: r.u64().ok()?,
                leader: r.opt_u64().ok()?,
                commit: r.u64().ok()?,
                applied: r.u64().ok()?,
                last_index: r.u64().ok()?,
                snapshot_index: r.u64().ok()?,
                keys: r.u64().ok()?,
            }),
            _ => return None,
        };
        r.is_empty().then_some(resp)
    }
}

/// How one node runs.
#[derive(Clone, Debug)]
pub struct NodeOptions {
    pub id: NodeId,
    pub cluster: Vec<Member>,
    pub dir: PathBuf,
    pub sync: SyncMode,
    /// Wall-clock length of one Raft tick. Elections time out after 10–20
    /// ticks and the leader sends heartbeats every 3.
    pub tick: Duration,
    /// Snapshot the state machine every this many applied entries.
    pub compact_every: u64,
}

enum Event {
    Tick,
    Peer(Envelope),
    Client(Request, Sender<Response>),
}

/// Runs a node until the process is killed. Returns only on a fatal error
/// (the storage refused a write or found corruption: fail-stop, never limp
/// on with state we can't trust).
pub fn run_node(opts: NodeOptions) -> Result<(), String> {
    let me = opts.cluster.iter().find(|m| m.id == opts.id).ok_or(format!("node {} is not in the cluster", opts.id))?.clone();
    let (mut storage, rec) = DiskStorage::open(&opts.dir, opts.sync).map_err(|e| format!("opening {}: {e}", opts.dir.display()))?;
    if rec.torn_bytes > 0 {
        eprintln!("node {}: cut a torn WAL tail of {} bytes", opts.id, rec.torn_bytes);
    }
    let mut kv = match &rec.snapshot {
        Some(s) => Kv::restore(&s.data).map_err(|e| format!("snapshot: {e}"))?,
        None => Kv::default(),
    };
    let mut applied = rec.snapshot.as_ref().map_or(0, |s| s.last_index);
    // Distinct per process and per start, so three nodes started together
    // don't draw the same election timeouts.
    let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos() as u64) ^ u64::from(std::process::id());
    let cfg = Config::new(opts.id, opts.cluster.iter().map(|m| m.id).collect(), seed);
    let mut node = RaftNode::new(cfg, rec.hard, rec.snapshot, rec.entries);
    eprintln!("node {}: up at term {}, log to {}, applied {}", opts.id, node.term(), node.last_index(), applied);

    let (tx, rx) = mpsc::channel::<Event>();
    let peer_listener = TcpListener::bind(me.raft).map_err(|e| format!("bind {}: {e}", me.raft))?;
    let client_listener = TcpListener::bind(me.client).map_err(|e| format!("bind {}: {e}", me.client))?;
    spawn_ticker(tx.clone(), opts.tick);
    spawn_peer_listener(peer_listener, opts.id, tx.clone());
    spawn_client_listener(client_listener, tx);
    let peers: BTreeMap<NodeId, SyncSender<Vec<u8>>> = opts.cluster.iter().filter(|m| m.id != opts.id).map(|m| (m.id, spawn_peer_writer(m.raft))).collect();

    // Client requests waiting for their entry to apply: index -> (term, reply).
    let mut pending: BTreeMap<Index, (Term, Sender<Response>)> = BTreeMap::new();
    let mut was_leader = false;
    for event in rx {
        match event {
            Event::Tick => node.tick(),
            Event::Peer(env) => node.step(env),
            Event::Client(Request::Status, reply) => {
                let _ = reply.send(Response::Status(Status {
                    id: opts.id,
                    role: node.role(),
                    term: node.term(),
                    leader: node.leader(),
                    commit: node.commit_index(),
                    applied,
                    last_index: node.last_index(),
                    snapshot_index: node.log().snapshot_index(),
                    keys: kv.len() as u64,
                }));
            }
            Event::Client(Request::Command(cmd), reply) => match node.propose(cmd.encode()) {
                Ok((index, term)) => {
                    pending.insert(index, (term, reply));
                }
                Err(ProposeError::NotLeader { leader }) => {
                    let _ = reply.send(leader.map_or(Response::Retry, Response::Redirect));
                }
            },
        }
        // Drain after every input (see raft_core::node's contract).
        loop {
            let ready = node.ready();
            if ready.is_empty() {
                break;
            }
            // 1. Persist, in the simulator's order. A storage error is fatal.
            let fatal = |e: kv::StorageError| format!("node {}: storage: {e}", opts.id);
            if let Some(hs) = &ready.hard_state {
                storage.save_hard_state(hs).map_err(fatal)?;
            }
            if let Some(s) = &ready.snapshot {
                storage.save_snapshot(s).map_err(fatal)?;
            }
            storage.append(ready.truncate_from, &ready.entries).map_err(fatal)?;
            // 2. Only now send: every vote and ack is backed by the disk.
            for m in ready.messages {
                if let Some(q) = peers.get(&m.to) {
                    match q.try_send(m.encode()) {
                        Ok(()) | Err(TrySendError::Full(_)) => {} // full: Raft retries
                        Err(TrySendError::Disconnected(_)) => return Err("a peer writer died".into()),
                    }
                }
            }
            // 3. Apply.
            if let Some(s) = &ready.restore {
                kv = Kv::restore(&s.data).map_err(|e| format!("snapshot from leader: {e}"))?;
                applied = s.last_index;
                eprintln!("node {}: installed the leader's snapshot at index {}", opts.id, s.last_index);
                // Anything we were waiting on below it was decided elsewhere.
                let below: Vec<Index> = pending.range(..=s.last_index).map(|(i, _)| *i).collect();
                for i in below {
                    let (_, r) = pending.remove(&i).unwrap();
                    let _ = r.send(Response::Retry);
                }
            }
            for e in &ready.committed {
                applied = e.index;
                let reply = match &e.kind {
                    EntryKind::Command(data) => Command::decode(data).ok().map(|c| kv.apply(&c)),
                    EntryKind::Noop => None,
                };
                if let Some((term, r)) = pending.remove(&e.index) {
                    let _ = r.send(match reply {
                        Some(reply) if term == e.term => Response::Done(reply),
                        _ => Response::Retry, // another leader's entry won this index
                    });
                }
            }
            if opts.compact_every > 0 && applied >= node.log().snapshot_index() + opts.compact_every {
                node.compact(applied, kv.snapshot());
            }
        }
        let leader = node.role() == Role::Leader;
        if was_leader && !leader {
            // Lost leadership: outcomes are unknown until a retry says.
            for (_, (_, r)) in std::mem::take(&mut pending) {
                let _ = r.send(Response::Retry);
            }
        }
        was_leader = leader;
    }
    Ok(())
}

fn spawn_ticker(tx: Sender<Event>, tick: Duration) {
    thread::spawn(move || loop {
        thread::sleep(tick);
        if tx.send(Event::Tick).is_err() {
            return;
        }
    });
}

fn spawn_peer_listener(l: TcpListener, me: NodeId, tx: Sender<Event>) {
    thread::spawn(move || {
        for conn in l.incoming().flatten() {
            let tx = tx.clone();
            thread::spawn(move || {
                let _ = conn.set_nodelay(true);
                let mut conn = io::BufReader::new(conn);
                // A bad frame closes the connection; it never reaches Raft.
                while let Ok(buf) = read_frame(&mut conn) {
                    match Envelope::decode(&buf) {
                        Ok(env) if env.to == me => {
                            if tx.send(Event::Peer(env)).is_err() {
                                return;
                            }
                        }
                        _ => return,
                    }
                }
            });
        }
    });
}

fn spawn_client_listener(l: TcpListener, tx: Sender<Event>) {
    thread::spawn(move || {
        for conn in l.incoming().flatten() {
            let tx = tx.clone();
            thread::spawn(move || {
                let _ = conn.set_nodelay(true);
                let mut conn = conn;
                while let Ok(buf) = read_frame(&mut conn) {
                    let Some(req) = Request::decode(&buf) else { return };
                    let (rtx, rrx) = mpsc::channel();
                    if tx.send(Event::Client(req, rtx)).is_err() {
                        return;
                    }
                    let resp = rrx.recv_timeout(Duration::from_secs(5)).unwrap_or(Response::Retry);
                    if write_frame(&mut conn, &resp.encode()).is_err() {
                        return;
                    }
                }
            });
        }
    });
}

/// One thread per peer: (re)connects and writes frames. The queue is
/// bounded and the loop never waits on it; while the peer is unreachable,
/// queued messages are dropped (Raft resends what matters).
fn spawn_peer_writer(addr: SocketAddr) -> SyncSender<Vec<u8>> {
    let (tx, rx): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::sync_channel(1024);
    thread::spawn(move || {
        let mut conn: Option<TcpStream> = None;
        let mut next_try = Instant::now();
        for frame in rx {
            if conn.is_none() && Instant::now() >= next_try {
                conn = TcpStream::connect_timeout(&addr, Duration::from_millis(200)).ok();
                if let Some(c) = &conn {
                    let _ = c.set_nodelay(true);
                } else {
                    next_try = Instant::now() + Duration::from_millis(100);
                }
            }
            if let Some(c) = conn.as_mut() {
                if write_frame(c, &frame).is_err() {
                    conn = None;
                }
            }
        }
    });
    tx
}

/// A client that finds the leader and retries with the same `(client,
/// seq)`, so a retried write applies at most once.
pub struct Client {
    pub cluster: Vec<Member>,
    pub id: u64,
    seq: u64,
    target: usize,
    pub timeout: Duration,
}

impl Client {
    pub fn new(cluster: Vec<Member>) -> Self {
        let id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(1, |d| d.as_nanos() as u64) ^ (u64::from(std::process::id()) << 32);
        Client { cluster, id: id.max(1), seq: 0, target: 0, timeout: Duration::from_secs(10) }
    }

    pub fn call(&mut self, op: Op) -> Result<Reply, String> {
        self.seq += 1;
        let req = Request::Command(Command { client: self.id, seq: self.seq, op });
        let deadline = Instant::now() + self.timeout;
        let mut last_err = String::from("no attempt");
        while Instant::now() < deadline {
            let m = self.cluster[self.target % self.cluster.len()].clone();
            match ask(m.client, &req) {
                Ok(Response::Done(r)) => return Ok(r),
                Ok(Response::Redirect(id)) => {
                    if let Some(i) = self.cluster.iter().position(|m| m.id == id) {
                        self.target = i;
                        continue;
                    }
                    self.target += 1;
                }
                Ok(Response::Retry) => {
                    last_err = format!("node {}: retry", m.id);
                    self.target += 1;
                }
                Ok(other) => return Err(format!("unexpected answer {other:?}")),
                Err(e) => {
                    last_err = format!("node {}: {e}", m.id);
                    self.target += 1;
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err(format!("gave up after {:?} ({last_err})", self.timeout))
    }
}

/// One request to one node.
pub fn ask(addr: SocketAddr, req: &Request) -> io::Result<Response> {
    let mut c = TcpStream::connect_timeout(&addr, Duration::from_millis(500))?;
    c.set_nodelay(true)?;
    c.set_read_timeout(Some(Duration::from_secs(6)))?;
    write_frame(&mut c, &req.encode())?;
    let buf = read_frame(&mut c)?;
    Response::decode(&buf).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad response"))
}

pub fn status(addr: SocketAddr) -> io::Result<Status> {
    match ask(addr, &Request::Status)? {
        Response::Status(s) => Ok(s),
        other => Err(io::Error::new(io::ErrorKind::InvalidData, format!("{other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_and_refuse_huge_lengths() {
        let mut buf = Vec::new();
        write_frame(&mut buf, b"hello").unwrap();
        write_frame(&mut buf, b"").unwrap();
        let mut r = &buf[..];
        assert_eq!(read_frame(&mut r).unwrap(), b"hello");
        assert_eq!(read_frame(&mut r).unwrap(), b"");
        assert!(read_frame(&mut r).is_err(), "end of stream");
        let huge = (u32::MAX).to_le_bytes();
        assert!(read_frame(&mut &huge[..]).is_err());
    }

    #[test]
    fn requests_and_responses_round_trip() {
        for req in [Request::Status, Request::Command(Command { client: 7, seq: 3, op: Op::Put("k".into(), "v w".into()) })] {
            assert_eq!(Request::decode(&req.encode()), Some(req));
        }
        let st = Status { id: 2, role: Role::Leader, term: 4, leader: Some(2), commit: 9, applied: 9, last_index: 10, snapshot_index: 5, keys: 3 };
        for r in [
            Response::Done(Reply::Value(Some("x".into()))),
            Response::Done(Reply::Value(None)),
            Response::Done(Reply::Written),
            Response::Done(Reply::Deleted(true)),
            Response::Done(Reply::Stale),
            Response::Redirect(3),
            Response::Retry,
            Response::Status(st),
        ] {
            assert_eq!(Response::decode(&r.encode()), Some(r));
        }
        assert_eq!(Request::decode(&[9]), None);
        assert_eq!(Response::decode(&[2, 0]), None, "trailing bytes");
    }

    #[test]
    fn cluster_parses() {
        let c = parse_cluster("1=127.0.0.1:7001/127.0.0.1:8001,2=127.0.0.1:7002/127.0.0.1:8002").unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(c[1].client, "127.0.0.1:8002".parse().unwrap());
        assert!(parse_cluster("1=127.0.0.1:7001").is_err());
        assert!(parse_cluster("").is_err());
    }
}
