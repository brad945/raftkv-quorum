//! One Raft node as a pure state machine (paper Figure 2; design §2–§3).
//!
//! Inputs: [`RaftNode::tick`] (one unit of logical time),
//! [`RaftNode::step`] (a message from a peer) and [`RaftNode::propose`]
//! (a client command). Output: [`RaftNode::ready`] drains what the caller
//! must do, in this order:
//!
//! 1. make `Ready::hard_state`, `truncate_from` + `entries` and `snapshot`
//!    durable (the node's in-memory state already reflects them);
//! 2. only then send `Ready::messages` (a vote or an ack is a promise about
//!    durable state);
//! 3. then apply `Ready::committed` to the state machine.
//!
//! The node never reads a clock or an OS random source: election timeouts
//! come from a seeded generator, so a run is reproducible from its seed.

use crate::log::Log;
use crate::{Entry, EntryKind, HardState, Index, NodeId, Snapshot, Term};
use std::collections::{BTreeMap, BTreeSet};

/// Static configuration of one node.
#[derive(Clone, Debug)]
pub struct Config {
    pub id: NodeId,
    /// Every member of the cluster, including this node.
    pub members: Vec<NodeId>,
    /// An election timeout is drawn uniformly from this range of ticks.
    pub election_ticks: (u32, u32),
    /// The leader sends heartbeats every this many ticks.
    pub heartbeat_ticks: u32,
    /// At most this many entries per AppendEntries.
    pub max_batch: usize,
    /// Seeds the election-timeout generator.
    pub seed: u64,
}

impl Config {
    pub fn new(id: NodeId, members: Vec<NodeId>, seed: u64) -> Self {
        Config { id, members, election_ticks: (10, 20), heartbeat_ticks: 3, max_batch: 64, seed }
    }
}

/// A node's role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// A Raft RPC (requests and responses are one-way messages).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    RequestVote { term: Term, last_index: Index, last_term: Term },
    RequestVoteResp { term: Term, granted: bool },
    AppendEntries { term: Term, prev_index: Index, prev_term: Term, entries: Vec<Entry>, leader_commit: Index },
    /// `success`: `match_index` is the last index this append made agree
    /// with the leader. Failure: `hint` is the index the leader should try
    /// next (an optimization over decrementing by one; paper §5.3).
    AppendEntriesResp { term: Term, success: bool, match_index: Index, hint: Index },
    InstallSnapshot { term: Term, snapshot: Snapshot },
    InstallSnapshotResp { term: Term, last_index: Index },
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote { term, .. }
            | Message::RequestVoteResp { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesResp { term, .. }
            | Message::InstallSnapshot { term, .. }
            | Message::InstallSnapshotResp { term, .. } => *term,
        }
    }

    fn is_response(&self) -> bool {
        matches!(self, Message::RequestVoteResp { .. } | Message::AppendEntriesResp { .. } | Message::InstallSnapshotResp { .. })
    }
}

/// A message with its addresses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message,
}

/// What the caller must do, in order (see the module comment).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Ready {
    pub hard_state: Option<HardState>,
    /// Cut the durable log from this index before appending `entries`.
    pub truncate_from: Option<Index>,
    pub entries: Vec<Entry>,
    /// Save this snapshot durably (it replaces the log up to its index).
    pub snapshot: Option<Snapshot>,
    pub messages: Vec<Envelope>,
    /// Restore the state machine from this snapshot before applying
    /// `committed` (a follower got a snapshot from the leader).
    pub restore: Option<Snapshot>,
    pub committed: Vec<Entry>,
}

impl Ready {
    pub fn is_empty(&self) -> bool {
        *self == Ready::default()
    }
}

/// Why a proposal was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposeError {
    /// Only the leader takes proposals; `leader` is who this node believes
    /// leads, if anyone.
    NotLeader { leader: Option<NodeId> },
}

/// Test-only switches that turn on known Raft bugs, so the chaos tests and
/// the checker can prove they catch them (design §7). Never set in real use.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bugs {
    /// Commit an entry from an earlier term by counting replicas (the
    /// paper's Figure 8 bug).
    pub commit_old_term_by_count: bool,
    /// Truncate the log after `prev_index` on every successful
    /// AppendEntries, even when the entries already match (breaks under
    /// reordered or duplicated messages).
    pub truncate_on_every_append: bool,
}

/// SplitMix64: a tiny, high-quality deterministic generator.
#[derive(Clone, Debug)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn range(&mut self, lo: u32, hi: u32) -> u32 {
        if hi <= lo {
            return lo;
        }
        lo + (self.next() % u64::from(hi - lo + 1)) as u32
    }
}

/// One Raft node.
pub struct RaftNode {
    cfg: Config,
    hard: HardState,
    log: Log,
    role: Role,
    leader: Option<NodeId>,
    commit: Index,
    applied: Index,
    elapsed: u32,
    timeout: u32,
    votes: BTreeSet<NodeId>,
    next_index: BTreeMap<NodeId, Index>,
    match_index: BTreeMap<NodeId, Index>,
    rng: Rng,
    bugs: Bugs,
    // What `ready` will hand out.
    out_hard: bool,
    out_truncate: Option<Index>,
    out_entries: Vec<Entry>,
    out_snapshot: Option<Snapshot>,
    out_restore: Option<Snapshot>,
    out_msgs: Vec<Envelope>,
}

impl RaftNode {
    /// A node restarting from its durable state (all empty on first boot).
    pub fn new(cfg: Config, hard: HardState, snapshot: Option<Snapshot>, entries: Vec<Entry>) -> Self {
        let (snap_index, snap_term) = snapshot.as_ref().map_or((0, 0), |s| (s.last_index, s.last_term));
        let mut rng = Rng(cfg.seed ^ cfg.id.wrapping_mul(0x2545_F491_4F6C_DD1D));
        let timeout = rng.range(cfg.election_ticks.0, cfg.election_ticks.1);
        let mut n = RaftNode {
            hard,
            log: Log::new(snap_index, snap_term, entries).with_snapshot(snapshot),
            role: Role::Follower,
            leader: None,
            // Everything in the snapshot is committed and applied.
            commit: snap_index,
            applied: snap_index,
            elapsed: 0,
            timeout,
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            rng,
            bugs: Bugs::default(),
            out_hard: false,
            out_truncate: None,
            out_entries: Vec::new(),
            out_snapshot: None,
            // On restart the caller restores the state machine from the
            // snapshot it loaded itself.
            out_restore: None,
            out_msgs: Vec::new(),
            cfg,
        };
        if n.cfg.members.len() == 1 {
            n.timeout = 1; // a single node elects itself at the first tick
        }
        n
    }

    pub fn id(&self) -> NodeId {
        self.cfg.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> Term {
        self.hard.term
    }
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }
    pub fn commit_index(&self) -> Index {
        self.commit
    }
    pub fn last_index(&self) -> Index {
        self.log.last_index()
    }
    pub fn log(&self) -> &Log {
        &self.log
    }
    pub fn hard_state(&self) -> &HardState {
        &self.hard
    }
    /// The leader's view of how much of its log `peer` holds.
    pub fn match_index(&self, peer: NodeId) -> Index {
        *self.match_index.get(&peer).unwrap_or(&0)
    }
    /// The next index the leader will send `peer`.
    pub fn next_index(&self, peer: NodeId) -> Index {
        *self.next_index.get(&peer).unwrap_or(&0)
    }

    /// Test only: turn on known bugs (see [`Bugs`]).
    pub fn set_bugs(&mut self, bugs: Bugs) {
        self.bugs = bugs;
    }

    /// Test only: force the next election timeout (to build a split vote).
    pub fn force_election_timeout(&mut self, ticks: u32) {
        self.timeout = ticks.max(1);
        self.elapsed = 0;
    }

    fn quorum(&self) -> usize {
        self.cfg.members.len() / 2 + 1
    }

    fn peers(&self) -> Vec<NodeId> {
        self.cfg.members.iter().copied().filter(|&m| m != self.cfg.id).collect()
    }

    fn send(&mut self, to: NodeId, msg: Message) {
        self.out_msgs.push(Envelope { from: self.cfg.id, to, msg });
    }

    fn reset_election_timer(&mut self) {
        self.elapsed = 0;
        self.timeout = self.rng.range(self.cfg.election_ticks.0, self.cfg.election_ticks.1);
    }

    fn set_hard(&mut self, term: Term, voted_for: Option<NodeId>) {
        if self.hard.term != term || self.hard.voted_for != voted_for {
            self.hard = HardState { term, voted_for };
            self.out_hard = true;
        }
    }

    /// Appends to the in-memory log and queues the durable write.
    fn append_local(&mut self, entries: Vec<Entry>) {
        for e in &entries {
            self.log.push(e.clone());
        }
        self.out_entries.extend(entries);
    }

    /// Cuts the in-memory log from `from` and queues the durable cut.
    fn truncate_local(&mut self, from: Index) {
        self.log.truncate_from(from);
        // Entries queued for writing at or after the cut are void too.
        self.out_entries.retain(|e| e.index < from);
        self.out_truncate = Some(self.out_truncate.map_or(from, |t| t.min(from)));
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.hard.term {
            self.set_hard(term, None);
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.reset_election_timer();
    }

    fn start_election(&mut self) {
        let term = self.hard.term + 1;
        self.set_hard(term, Some(self.cfg.id));
        self.role = Role::Candidate;
        self.leader = None;
        self.votes = BTreeSet::from([self.cfg.id]);
        self.reset_election_timer();
        if self.votes.len() >= self.quorum() {
            self.become_leader();
            return;
        }
        let (last_index, last_term) = (self.log.last_index(), self.log.last_term());
        for p in self.peers() {
            self.send(p, Message::RequestVote { term, last_index, last_term });
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id);
        self.votes.clear();
        let next = self.log.last_index() + 1;
        self.next_index = self.peers().into_iter().map(|p| (p, next)).collect();
        self.match_index = self.peers().into_iter().map(|p| (p, 0)).collect();
        // A no-op of the new term lets this leader commit everything before
        // it (it may only commit entries of its own term by counting).
        let e = Entry { term: self.hard.term, index: next, kind: EntryKind::Noop };
        self.append_local(vec![e]);
        self.elapsed = 0;
        self.broadcast_append();
        self.maybe_commit();
    }

    /// One unit of logical time.
    pub fn tick(&mut self) {
        self.elapsed += 1;
        match self.role {
            Role::Leader => {
                if self.elapsed >= self.cfg.heartbeat_ticks {
                    self.elapsed = 0;
                    self.broadcast_append();
                }
            }
            Role::Follower | Role::Candidate => {
                if self.elapsed >= self.timeout {
                    self.start_election();
                }
            }
        }
    }

    /// A client command. Returns the index and term it was appended at; it
    /// is committed only when that entry shows up in `Ready::committed`.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<(Index, Term), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader { leader: self.leader });
        }
        let e = Entry { term: self.hard.term, index: self.log.last_index() + 1, kind: EntryKind::Command(data) };
        let at = (e.index, e.term);
        self.append_local(vec![e]);
        self.broadcast_append();
        self.maybe_commit(); // a single-node cluster commits at once
        Ok(at)
    }

    fn broadcast_append(&mut self) {
        for p in self.peers() {
            self.send_append(p);
        }
    }

    fn send_append(&mut self, to: NodeId) {
        let next = *self.next_index.get(&to).unwrap_or(&1);
        if next <= self.log.snapshot_index() {
            // The entries it needs were compacted away: send the snapshot.
            if let Some(snapshot) = self.log.snapshot_for_send() {
                self.send(to, Message::InstallSnapshot { term: self.hard.term, snapshot });
                return;
            }
        }
        let prev_index = next - 1;
        let prev_term = self.log.term_at(prev_index).expect("prev entry is in the log or the snapshot");
        let entries = self.log.slice(next, self.cfg.max_batch);
        self.send(to, Message::AppendEntries { term: self.hard.term, prev_index, prev_term, entries, leader_commit: self.commit });
    }

    /// Paper §5.3–§5.4: commit the highest index stored on a majority whose
    /// entry is from the current term (Figure 8: never an earlier term's
    /// entry by counting replicas; it commits indirectly with a newer one).
    fn maybe_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let mut n = self.log.last_index();
        while n > self.commit {
            let current_term = self.log.term_at(n) == Some(self.hard.term);
            if current_term || self.bugs.commit_old_term_by_count {
                let replicas = 1 + self.match_index.values().filter(|&&m| m >= n).count();
                if replicas >= self.quorum() {
                    self.commit = n;
                    break;
                }
            }
            n -= 1;
        }
    }

    /// A message from a peer.
    pub fn step(&mut self, env: Envelope) {
        let from = env.from;
        let term = env.msg.term();
        // Any message with a higher term: this node is out of date.
        if term > self.hard.term {
            let leader = match env.msg {
                Message::AppendEntries { .. } | Message::InstallSnapshot { .. } => Some(from),
                _ => None,
            };
            self.become_follower(term, leader);
        }
        if term < self.hard.term {
            // Stale. A request is answered so its sender learns the newer
            // term; a stale response is simply dropped.
            if !env.msg.is_response() {
                let t = self.hard.term;
                let reply = match env.msg {
                    Message::RequestVote { .. } => Message::RequestVoteResp { term: t, granted: false },
                    Message::AppendEntries { .. } => Message::AppendEntriesResp { term: t, success: false, match_index: 0, hint: 0 },
                    Message::InstallSnapshot { .. } => Message::InstallSnapshotResp { term: t, last_index: 0 },
                    _ => unreachable!(),
                };
                self.send(from, reply);
            }
            return;
        }
        match env.msg {
            Message::RequestVote { last_index, last_term, .. } => self.on_request_vote(from, last_index, last_term),
            Message::RequestVoteResp { granted, .. } => {
                if self.role == Role::Candidate && granted {
                    self.votes.insert(from);
                    if self.votes.len() >= self.quorum() {
                        self.become_leader();
                    }
                }
            }
            Message::AppendEntries { prev_index, prev_term, entries, leader_commit, .. } => {
                self.on_append(from, prev_index, prev_term, entries, leader_commit)
            }
            Message::AppendEntriesResp { success, match_index, hint, .. } => self.on_append_resp(from, success, match_index, hint),
            Message::InstallSnapshot { snapshot, .. } => self.on_install_snapshot(from, snapshot),
            Message::InstallSnapshotResp { last_index, .. } => {
                if self.role == Role::Leader {
                    self.advance_peer(from, last_index);
                    self.send_append(from);
                }
            }
        }
    }

    fn on_request_vote(&mut self, from: NodeId, last_index: Index, last_term: Term) {
        let free = self.hard.voted_for.is_none_or(|v| v == from);
        // §5.4.1: only vote for a candidate whose log is at least as
        // up to date as ours (higher last term, or same term and as long).
        let ours = (self.log.last_term(), self.log.last_index());
        let up_to_date = (last_term, last_index) >= ours;
        let granted = free && up_to_date && self.role != Role::Leader;
        if granted {
            self.set_hard(self.hard.term, Some(from));
            self.reset_election_timer();
        }
        self.send(from, Message::RequestVoteResp { term: self.hard.term, granted });
    }

    fn on_append(&mut self, from: NodeId, prev_index: Index, prev_term: Term, mut entries: Vec<Entry>, leader_commit: Index) {
        let term = self.hard.term;
        // A current-term AppendEntries means `from` won this term.
        self.role = Role::Follower;
        self.leader = Some(from);
        self.votes.clear();
        self.reset_election_timer();

        let (mut prev_index, mut prev_term) = (prev_index, prev_term);
        // Entries we already compacted are committed and therefore match:
        // skip them and treat our snapshot point as `prev`.
        if prev_index < self.log.snapshot_index() {
            let snap = self.log.snapshot_index();
            entries.retain(|e| e.index > snap);
            prev_index = snap;
            prev_term = self.log.snapshot_term();
        }
        if prev_index > self.log.last_index() {
            let hint = self.log.last_index() + 1;
            self.send(from, Message::AppendEntriesResp { term, success: false, match_index: 0, hint });
            return;
        }
        if self.log.term_at(prev_index) != Some(prev_term) {
            // Back up to the first index of the conflicting term (§5.3).
            let conflict = self.log.term_at(prev_index).unwrap_or(0);
            let mut hint = prev_index;
            while hint > self.log.snapshot_index() + 1 && self.log.term_at(hint - 1) == Some(conflict) {
                hint -= 1;
            }
            self.send(from, Message::AppendEntriesResp { term, success: false, match_index: 0, hint: hint.max(1) });
            return;
        }
        let last_new = prev_index + entries.len() as Index;
        if self.bugs.truncate_on_every_append && prev_index < self.log.last_index() {
            self.truncate_local(prev_index + 1);
        }
        // Append what is new. Truncate only at a real conflict: a delayed or
        // duplicated append whose entries already match must never delete
        // the entries after them.
        for (i, e) in entries.iter().enumerate() {
            match self.log.term_at(e.index) {
                Some(t) if t == e.term => continue,
                Some(_) => {
                    self.truncate_local(e.index);
                    self.append_local(entries[i..].to_vec());
                    break;
                }
                None => {
                    self.append_local(entries[i..].to_vec());
                    break;
                }
            }
        }
        if leader_commit > self.commit {
            self.commit = leader_commit.min(last_new);
        }
        self.send(from, Message::AppendEntriesResp { term, success: true, match_index: last_new, hint: 0 });
    }

    fn advance_peer(&mut self, peer: NodeId, matched: Index) {
        let m = self.match_index.entry(peer).or_insert(0);
        if matched > *m {
            *m = matched; // never moves backwards: stale acks are ignored
        }
        let m = *m;
        let n = self.next_index.entry(peer).or_insert(1);
        if *n < m + 1 {
            *n = m + 1;
        }
        self.maybe_commit();
    }

    fn on_append_resp(&mut self, from: NodeId, success: bool, matched: Index, hint: Index) {
        if self.role != Role::Leader {
            return;
        }
        if success {
            self.advance_peer(from, matched);
            if self.next_index[&from] <= self.log.last_index() {
                self.send_append(from); // more to send
            }
        } else {
            let m = *self.match_index.get(&from).unwrap_or(&0);
            // Never back up past what the peer is known to hold.
            let next = hint.max(m + 1).max(1);
            self.next_index.insert(from, next);
            self.send_append(from);
        }
    }

    fn on_install_snapshot(&mut self, from: NodeId, snap: Snapshot) {
        let term = self.hard.term;
        self.role = Role::Follower;
        self.leader = Some(from);
        self.reset_election_timer();
        if snap.last_index <= self.commit {
            // Older than what we already have: nothing to do (§7).
            self.send(from, Message::InstallSnapshotResp { term, last_index: self.commit });
            return;
        }
        self.log.install_snapshot(&snap);
        // Queued log writes before the snapshot point are superseded.
        self.out_entries.retain(|e| e.index > snap.last_index);
        self.commit = snap.last_index;
        self.applied = snap.last_index;
        self.out_restore = Some(snap.clone());
        self.out_snapshot = Some(snap.clone());
        self.send(from, Message::InstallSnapshotResp { term, last_index: snap.last_index });
    }

    /// The caller took a snapshot of its state machine at `index` (which
    /// must be applied): compact the log behind it.
    pub fn compact(&mut self, index: Index, data: Vec<u8>) {
        if index <= self.log.snapshot_index() || index > self.applied {
            return;
        }
        let term = self.log.term_at(index).expect("an applied entry is in the log");
        let snap = Snapshot { last_index: index, last_term: term, data };
        self.log.install_snapshot(&snap);
        self.out_snapshot = Some(snap);
    }

    /// Drains what the caller must do (see the module comment for the order).
    pub fn ready(&mut self) -> Ready {
        let mut committed = Vec::new();
        while self.applied < self.commit {
            self.applied += 1;
            let e = self.log.entry(self.applied).expect("committed entries are in the log").clone();
            committed.push(e);
        }
        Ready {
            hard_state: std::mem::take(&mut self.out_hard).then(|| self.hard.clone()),
            truncate_from: self.out_truncate.take(),
            entries: std::mem::take(&mut self.out_entries),
            snapshot: self.out_snapshot.take(),
            messages: std::mem::take(&mut self.out_msgs),
            restore: self.out_restore.take(),
            committed,
        }
    }
}

#[cfg(test)]
#[path = "node_tests.rs"]
mod tests;
