//! The Raft paper's Figure 8, scripted message by message.
//!
//! Random chaos never produced it (R5 sweeps: 0 of 500 seeds with the bug
//! planted): a new leader appends a no-op and ships it with the old
//! entries, so a majority almost never holds an old-term entry without a
//! current-term one. This test builds the exact sequence instead, with
//! one-entry batches. Five servers, all starting with entry 1 (term 1):
//!
//! (a) S1 leads term 2 and replicates entry 2 (term 2) to S2 only.
//! (b) S1 crashes. S5 wins term 3 with votes from S3 and S4, appends entry 2
//!     (term 3), and crashes before replicating it.
//! (c) S1 restarts and wins term 4 (S2, S3). It replicates entry 2 (term 2)
//!     to S3, so a majority (S1, S2, S3) now stores it. **Correct Raft must
//!     not commit it**: it's from term 2, not the current term 4. Then S1
//!     crashes before its term-4 entry reaches a majority.
//! (d) S5 restarts and wins term 5 (S2, S3, S4: its last term, 3, beats
//!     their term-2 entries). It overwrites index 2 with its term-3 entry
//!     and commits it.
//!
//! If S1 committed and applied entry 2 (term 2) in (c), two servers have now
//! applied different entries at index 2: a state machine safety violation.

use raft_core::{Bugs, Config, Entry, EntryKind, Envelope, HardState, Index, Message, NodeId, RaftNode, Ready, Role, Term};
use std::collections::{BTreeMap, VecDeque};

#[derive(Default, Clone)]
struct Disk {
    hard: HardState,
    entries: Vec<Entry>,
}

struct Cluster {
    nodes: BTreeMap<NodeId, Option<RaftNode>>,
    disks: BTreeMap<NodeId, Disk>,
    bugs: Bugs,
    /// (node, index) -> term of the entry it applied.
    applied: BTreeMap<(NodeId, Index), Term>,
    queue: VecDeque<Envelope>,
}

fn config(id: NodeId) -> Config {
    let mut c = Config::new(id, vec![1, 2, 3, 4, 5], 8);
    c.max_batch = 1;
    c.election_ticks = (1000, 1000); // no elections unless a test forces one
    c
}

impl Cluster {
    fn new(bugs: Bugs) -> Self {
        let first = Entry { term: 1, index: 1, kind: EntryKind::Noop };
        let mut c = Cluster { nodes: BTreeMap::new(), disks: BTreeMap::new(), bugs, applied: BTreeMap::new(), queue: VecDeque::new() };
        for id in 1..=5 {
            c.disks.insert(id, Disk { hard: HardState { term: 1, voted_for: None }, entries: vec![first.clone()] });
            c.start(id);
        }
        c
    }

    fn start(&mut self, id: NodeId) {
        let d = self.disks[&id].clone();
        let mut n = RaftNode::new(config(id), d.hard, None, d.entries);
        n.set_bugs(self.bugs);
        self.nodes.insert(id, Some(n));
    }

    fn crash(&mut self, id: NodeId) {
        self.nodes.insert(id, None);
        self.queue.retain(|m| m.to != id && m.from != id);
    }

    fn node(&mut self, id: NodeId) -> &mut RaftNode {
        self.nodes.get_mut(&id).unwrap().as_mut().expect("node is up")
    }

    /// The correct driver: persist, then send, then apply.
    fn drain(&mut self, id: NodeId) {
        loop {
            let Some(Some(n)) = self.nodes.get_mut(&id) else { return };
            let r: Ready = n.ready();
            if r.is_empty() {
                return;
            }
            let d = self.disks.get_mut(&id).unwrap();
            if let Some(h) = r.hard_state {
                d.hard = h;
            }
            if let Some(from) = r.truncate_from {
                d.entries.retain(|e| e.index < from);
            }
            d.entries.extend(r.entries);
            self.queue.extend(r.messages);
            for e in r.committed {
                self.applied.insert((id, e.index), e.term);
            }
        }
    }

    /// Delivers queued messages that `allow` lets through (others are
    /// dropped) until nothing is left.
    fn pump(&mut self, allow: impl Fn(&Envelope) -> bool) {
        for id in 1..=5 {
            self.drain(id);
        }
        while let Some(m) = self.queue.pop_front() {
            // `allow` may count messages: call it once per message.
            let deliver = allow(&m) && self.nodes[&m.to].is_some();
            if std::env::var("FIG8_TRACE").is_ok() {
                eprintln!("{} {}->{} {:?}", if deliver { "deliver" } else { "drop   " }, m.from, m.to, m.msg);
            }
            if !deliver {
                continue;
            }
            let to = m.to;
            self.node(to).step(m);
            self.drain(to);
        }
    }

    fn elect(&mut self, id: NodeId) {
        self.node(id).force_election_timeout(1);
        self.node(id).tick();
    }

    /// Campaigns until `id` leads: a lost campaign (a voter already voted
    /// in that term) still raises the term, as in the paper's sequence.
    fn win(&mut self, id: NodeId, allow: impl Fn(&Envelope) -> bool) {
        for _ in 0..5 {
            self.elect(id);
            self.pump(&allow);
            if self.node(id).role() == Role::Leader {
                return;
            }
        }
        let states: Vec<String> = (1..=5).map(|i| match &self.nodes[&i] {
            Some(n) => format!("S{i}: term {} voted {:?} role {:?} last ({}, {})", n.term(), n.hard_state().voted_for, n.role(), n.log().last_term(), n.last_index()),
            None => format!("S{i}: down"),
        }).collect();
        panic!("node {id} could not win an election:\n{}", states.join("\n"));
    }

    /// Entries applied at `index` with different terms, by node.
    fn conflicts_at(&self, index: Index) -> BTreeMap<Term, Vec<NodeId>> {
        let mut by_term: BTreeMap<Term, Vec<NodeId>> = BTreeMap::new();
        for (&(node, i), &t) in &self.applied {
            if i == index {
                by_term.entry(t).or_default().push(node);
            }
        }
        by_term
    }
}

fn is_vote(m: &Envelope) -> bool {
    matches!(m.msg, Message::RequestVote { .. } | Message::RequestVoteResp { .. })
}

fn between(m: &Envelope, a: NodeId, others: &[NodeId]) -> bool {
    (m.from == a && others.contains(&m.to)) || (m.to == a && others.contains(&m.from))
}

/// Runs Figure 8 and returns, per term, which nodes applied an entry at
/// index 2 with that term.
fn figure8(bugs: Bugs) -> (BTreeMap<Term, Vec<NodeId>>, Index) {
    let mut c = Cluster::new(bugs);

    // (a) S1 leads term 2 (votes S2, S3); entry 2 reaches S2 only.
    c.win(1, |m| (is_vote(m) && between(m, 1, &[2, 3])) || (!is_vote(m) && between(m, 1, &[2])));
    assert_eq!(c.node(1).term(), 2);
    assert_eq!(c.disks[&2].entries.last().map(|e| (e.index, e.term)), Some((2, 2)), "S2 holds entry 2 (term 2)");
    assert_eq!(c.disks[&3].entries.len(), 1, "S3 does not");

    // (b) S1 crashes; S5 wins term 3 (S3, S4), appends entry 2 (term 3) alone.
    c.crash(1);
    c.win(5, |m| is_vote(m) && between(m, 5, &[3, 4]));
    assert_eq!(c.node(5).term(), 3);
    c.crash(5);
    assert_eq!(c.disks[&5].entries.last().map(|e| (e.index, e.term)), Some((2, 3)));

    // (c) S1 restarts and wins term 4 (S2, S3); entry 2 (term 2) reaches S3.
    c.start(1);
    // Votes with S2 and S3; appends only to S3 and only up to entry 2 (the
    // term-4 entry at index 3 never leaves S1: it crashes first).
    // The first "prev 2 + entry 3" to S3 is a probe S3 rejects (it lacks
    // entry 2), which makes S1 back up and send entry 2; the second copy
    // would succeed, and is the one S1's crash prevents.
    let probes = std::cell::Cell::new(0);
    let only_entry2 = |m: &Envelope| match &m.msg {
        Message::AppendEntries { entries, .. } => {
            if m.from != 1 || m.to != 3 {
                return false;
            }
            if entries.iter().any(|e| e.index >= 3) {
                probes.set(probes.get() + 1);
                return probes.get() == 1;
            }
            true
        }
        Message::AppendEntriesResp { .. } => m.from == 3 && m.to == 1,
        _ => is_vote(m) && between(m, 1, &[2, 3]),
    };
    c.win(1, only_entry2);
    assert_eq!(c.node(1).term(), 4);
    // S2 already holds entry 2 (term 2) from (a); its term-4 ack says so.
    c.queue.push_back(Envelope { from: 2, to: 1, msg: Message::AppendEntriesResp { term: 4, success: true, match_index: 2, hint: 0, rejected: 0 } });
    c.pump(|m| m.from == 2 && m.to == 1);
    assert_eq!(c.disks[&3].entries.get(1).map(|e| e.term), Some(2), "S3 now holds entry 2 (term 2)");
    let s1_commit = c.node(1).commit_index();
    // S1 crashes before its term-4 entry reaches a majority.
    c.crash(1);

    // (d) S5 restarts and wins term 5 (S2, S3, S4); its entry 2 (term 3)
    // overwrites theirs and commits.
    c.start(5);
    c.win(5, |m| between(m, 5, &[2, 3, 4]));
    assert_eq!(c.node(5).term(), 5);
    assert!(c.node(5).commit_index() >= 2, "S5 committed index 2");
    (c.conflicts_at(2), s1_commit)
}

#[test]
fn figure8_correct_raft_stays_safe() {
    let (applied, s1_commit) = figure8(Bugs::default());
    assert!(s1_commit < 2, "S1 must not commit the term-2 entry by counting (commit was {s1_commit})");
    assert_eq!(applied.len(), 1, "everyone who applied index 2 applied the same entry: {applied:?}");
    assert!(applied.contains_key(&3), "the surviving entry is S5's term-3 one: {applied:?}");
}

#[test]
fn figure8_planted_bug_breaks_state_machine_safety() {
    let (applied, s1_commit) = figure8(Bugs { commit_old_term_by_count: true, ..Bugs::default() });
    assert_eq!(s1_commit, 2, "the bug commits the term-2 entry by counting");
    assert_eq!(applied.get(&2).cloned(), Some(vec![1]), "S1 applied entry 2 from term 2: {applied:?}");
    assert!(applied.get(&3).is_some_and(|ns| !ns.is_empty()), "and S5's side applied entry 2 from term 3: {applied:?}");
}
