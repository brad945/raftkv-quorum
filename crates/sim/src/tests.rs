//! Cluster tests in the simulator (design §8, milestone R3).

use super::*;
use kv::{Command, Op};
use raft_core::{Message, RaftNode};

fn put(client: u64, seq: u64, k: &str, v: &str) -> Vec<u8> {
    Command { client, seq, op: Op::Put(k.into(), v.into()) }.encode()
}

fn assert_safe(sim: &Sim) {
    assert!(sim.violations.is_empty(), "safety violations: {:#?}", sim.violations);
}

fn elect(sim: &mut Sim) -> NodeId {
    assert!(sim.run_until(500, |s| s.leader().is_some()), "no leader within 500 ticks");
    sim.leader().unwrap()
}

#[test]
fn elects_one_leader_and_followers_learn_it() {
    let mut sim = Sim::new(SimConfig::new(3, 1));
    let l = elect(&mut sim);
    sim.run(20);
    let term = sim.core(l).unwrap().term();
    for (id, n) in &sim.nodes {
        let c = n.core.as_ref().unwrap();
        assert_eq!(c.term(), term, "node {id}");
        assert_eq!(c.leader(), Some(l), "node {id} knows the leader");
        assert_eq!(c.role() == Role::Leader, *id == l);
    }
    assert_safe(&sim);
}

#[test]
fn replicates_commands_to_every_node_in_order() {
    let mut sim = Sim::new(SimConfig::new(3, 2));
    elect(&mut sim);
    for i in 0..20 {
        sim.propose(put(1, i + 1, &format!("k{}", i % 5), &format!("v{i}"))).expect("a leader");
        sim.run(2);
    }
    let last = sim.core(sim.leader().unwrap()).unwrap().last_index();
    assert!(sim.run_until(200, |s| s.nodes.values().all(|n| n.applied == last)), "not everyone applied {last}");
    let first = sim.nodes[&1].kv.clone();
    for n in sim.nodes.values() {
        assert_eq!(n.kv, first, "node {} diverged", n.id);
    }
    assert_eq!(first.get("k4").map(String::as_str), Some("v19"));
    assert_safe(&sim);
}

#[test]
fn survives_drops_duplicates_and_reordering() {
    let mut cfg = SimConfig::new(5, 3);
    cfg.delay = (1, 8); // wide delays reorder messages
    cfg.drop = 100_000; // 10%
    cfg.duplicate = 50_000; // 5%
    let mut sim = Sim::new(cfg);
    let mut seq = 0;
    for _ in 0..40 {
        if sim.leader().is_some() {
            seq += 1;
            sim.propose(put(1, seq, "x", &seq.to_string()));
        }
        sim.run(5);
    }
    sim.run(300);
    let applied: Vec<Index> = sim.nodes.values().map(|n| n.applied).collect();
    assert!(applied.iter().all(|&a| a > 0), "every node applied something: {applied:?}");
    assert_safe(&sim);
}

#[test]
fn leader_crash_elects_a_new_leader_and_keeps_committed_entries() {
    let mut sim = Sim::new(SimConfig::new(3, 4));
    let old = elect(&mut sim);
    for i in 1..=5 {
        sim.propose(put(1, i, "k", &i.to_string()));
        sim.run(3);
    }
    let committed = sim.core(old).unwrap().commit_index();
    assert!(committed >= 5);
    sim.crash(old);
    assert!(sim.run_until(500, |s| s.leader().is_some_and(|l| l != old)), "no new leader");
    let new = sim.leader().unwrap();
    assert!(sim.core(new).unwrap().last_index() >= committed, "the new leader has every committed entry");
    sim.restart(old);
    sim.propose(put(1, 6, "k", "after"));
    let last = sim.core(new).unwrap().last_index();
    assert!(sim.run_until(500, |s| s.nodes.values().all(|n| n.applied >= last)), "the restarted node caught up");
    assert_eq!(sim.nodes[&old].kv.get("k").map(String::as_str), Some("after"));
    assert_safe(&sim);
}

#[test]
fn same_seed_same_trace() {
    let run = |seed: u64| {
        let mut cfg = SimConfig::new(5, seed);
        cfg.delay = (1, 6);
        cfg.drop = 50_000;
        cfg.duplicate = 20_000;
        let mut sim = Sim::new(cfg);
        for i in 1..=30 {
            sim.propose(put(1, i, "k", "v"));
            sim.run(4);
            if i == 10 {
                sim.crash(2);
            }
            if i == 20 {
                sim.restart(2);
            }
        }
        sim.trace
    };
    let a = run(42);
    assert!(a.len() > 500, "a meaningful trace ({} events)", a.len());
    assert_eq!(a, run(42), "same seed, same run");
    assert_ne!(a, run(43), "a different seed is a different run");
}

/// A 3-node cluster built from bare nodes, so a test can control exactly
/// when a node persists, sends and crashes.
struct Manual {
    nodes: BTreeMap<NodeId, RaftNode>,
    disks: BTreeMap<NodeId, Disk>,
}

impl Manual {
    fn new() -> Self {
        let mut nodes = BTreeMap::new();
        let mut disks = BTreeMap::new();
        for id in 1..=3 {
            nodes.insert(id, RaftNode::new(Config::new(id, vec![1, 2, 3], 9), HardState::default(), None, vec![]));
            disks.insert(id, Disk::default());
        }
        Manual { nodes, disks }
    }
    /// Drains `id`'s Ready: persists (unless `crash_before_persist`) and
    /// returns the messages it would send.
    fn ready(&mut self, id: NodeId, persist: bool) -> Vec<Envelope> {
        let r = self.nodes.get_mut(&id).unwrap().ready();
        if persist {
            self.disks.get_mut(&id).unwrap().persist(&r);
        }
        r.messages
    }
    fn restart(&mut self, id: NodeId) {
        let d = &self.disks[&id];
        self.nodes.insert(id, RaftNode::new(Config::new(id, vec![1, 2, 3], 9), d.hard.clone(), None, d.entries.clone()));
    }
    fn deliver(&mut self, msgs: Vec<Envelope>, to: NodeId) {
        for m in msgs.into_iter().filter(|m| m.to == to) {
            self.nodes.get_mut(&to).unwrap().step(m);
        }
    }
}

/// Runs the double-vote scenario: node 2 grants node 1's vote, then crashes
/// either before or after persisting that vote, restarts, and is asked by
/// node 3 for its vote in the same term. Returns who became leader of term 1.
fn double_vote_scenario(persist_before_send: bool) -> Vec<NodeId> {
    let mut m = Manual::new();
    // Node 1 times out and asks for votes in term 1.
    m.nodes.get_mut(&1).unwrap().force_election_timeout(1);
    m.nodes.get_mut(&1).unwrap().tick();
    let req1 = m.ready(1, true);
    // Node 2 grants it. The vote reaches node 1...
    m.deliver(req1.clone(), 2);
    let grant = m.ready(2, persist_before_send);
    m.deliver(grant, 1);
    // ...and node 2 crashes and restarts from its disk.
    m.restart(2);
    // Node 3 also times out in term 1 and asks node 2.
    m.nodes.get_mut(&3).unwrap().force_election_timeout(1);
    m.nodes.get_mut(&3).unwrap().tick();
    let req3 = m.ready(3, true);
    m.deliver(req3, 2);
    let reply = m.ready(2, true);
    m.deliver(reply, 3);
    m.nodes.iter().filter(|(_, n)| n.role() == Role::Leader && n.term() == 1).map(|(&id, _)| id).collect()
}

// Design §2: a vote is a promise about durable state. Sent before it is
// persisted, a crash lets the node vote twice in one term: two leaders.
#[test]
fn vote_is_durable_before_sent() {
    assert_eq!(double_vote_scenario(true), vec![1], "persisting first: node 2 refuses node 3, one leader");
    assert_eq!(double_vote_scenario(false), vec![1, 3], "sending before persisting: two leaders in term 1");
}

// A split vote: with the network cut so each candidate reaches only one
// voter, a 4-node cluster has no majority (3) in that term. Healing lets
// randomized timeouts resolve it.
#[test]
fn split_vote_resolves() {
    let mut sim = Sim::new(SimConfig::new(4, 5));
    sim.partition(vec![vec![1, 3], vec![2, 4]]);
    sim.core_mut(1).unwrap().force_election_timeout(1);
    sim.core_mut(2).unwrap().force_election_timeout(1);
    sim.run(30);
    assert!(sim.leader().is_none(), "no side has a majority of 4");
    let split_term = sim.nodes.values().map(|n| n.core.as_ref().unwrap().term()).max().unwrap();
    assert!(split_term >= 1);
    sim.heal();
    let l = elect(&mut sim);
    assert!(sim.core(l).unwrap().term() > split_term - 1);
    sim.run(30);
    assert_eq!(sim.nodes.values().filter(|n| n.core.as_ref().unwrap().role() == Role::Leader).count(), 1);
    assert_safe(&sim);
}

#[test]
fn minority_leader_cannot_commit_and_steps_down_after_heal() {
    let mut sim = Sim::new(SimConfig::new(5, 6));
    let old = elect(&mut sim);
    sim.run(10);
    let others: Vec<NodeId> = (1..=5).filter(|&i| i != old).collect();
    // The old leader keeps one follower: a minority of 2 out of 5.
    sim.partition(vec![vec![old, others[0]], others[1..].to_vec()]);
    let before = sim.core(old).unwrap().commit_index();
    let (_, idx, _) = sim.propose(put(9, 1, "lost?", "minority")).expect("the old leader still accepts it");
    sim.run(100);
    assert_eq!(sim.core(old).unwrap().commit_index(), before, "a minority cannot commit index {idx}");
    assert!(sim.nodes.values().any(|n| n.id != old && n.core.as_ref().unwrap().role() == Role::Leader), "the majority elected a leader");
    sim.heal();
    sim.run(100);
    assert_ne!(sim.core(old).unwrap().role(), Role::Leader, "the old leader stepped down");
    assert_safe(&sim);
}

#[test]
fn stale_message_is_dropped_by_term() {
    // A message from an old term, delivered after a new election, changes nothing.
    let mut sim = Sim::new(SimConfig::new(3, 7));
    let l = elect(&mut sim);
    let term = sim.core(l).unwrap().term();
    let f = (1..=3).find(|&i| i != l).unwrap();
    let before = sim.core(f).unwrap().last_index();
    sim.inject(Envelope { from: l, to: f, msg: Message::AppendEntries { term: term - 1, prev_index: 0, prev_term: 0, entries: vec![], leader_commit: 0 } });
    assert_eq!(sim.core(f).unwrap().last_index(), before);
    assert_eq!(sim.core(f).unwrap().term(), term);
    assert_safe(&sim);
}
