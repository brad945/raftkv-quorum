//! A deterministic simulator for raft-core (design §6).
//!
//! Everything comes from one seed: message delays, drops, duplicates and
//! (through the delays) reordering; election timeouts are seeded inside
//! each node. There is no wall clock, no thread and no `HashMap`, so the
//! same seed always produces the same run, byte for byte (`trace`).
//!
//! The simulator is also the nodes' driver, and keeps the `Ready` contract:
//! persist (to a simulated disk that survives crashes), then send, then
//! apply. A crash loses everything not yet persisted. While it runs it
//! checks two Raft safety properties after every step:
//!
//! - election safety: at most one leader per term;
//! - state machine safety: no two nodes ever apply different entries at
//!   the same index.



pub mod chaos;

use kv::Kv;
use raft_core::{Bugs, Config, Entry, EntryKind, Envelope, HardState, Index, NodeId, RaftNode, Role, Snapshot, Term};
use std::collections::{BTreeMap, BTreeSet};

/// SplitMix64 (the same generator the nodes use, seeded differently).
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }
    #[allow(clippy::should_implement_trait)] // an infinite generator, not an Iterator
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// True with probability `per_million` / 1,000,000.
    pub fn chance(&mut self, per_million: u32) -> bool {
        self.next() % 1_000_000 < u64::from(per_million)
    }
    /// Uniform in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        lo + self.next() % (hi - lo + 1)
    }
}

/// Network and cluster settings.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub nodes: u64,
    pub seed: u64,
    /// Message delay in ticks, uniform in this range (different delays are
    /// what reorder messages).
    pub delay: (u64, u64),
    /// Probability (per million) that a message is dropped.
    pub drop: u32,
    /// Probability (per million) that a message is delivered twice.
    pub duplicate: u32,
    /// Compact a node's log once it has applied this many entries past its
    /// snapshot (0: never).
    pub compact_every: u64,
    /// Test only: the driver sends messages *before* persisting, which is
    /// the bug the `Ready` ordering exists to prevent.
    pub send_before_persist: bool,
    /// Probability (per million, per driver step) that the node crashes
    /// *between* the driver's two phases: after persisting and before
    /// sending (or, with `send_before_persist`, after sending and before
    /// persisting). Real machines can die there; a crash only between
    /// whole steps would hide exactly the ordering bug the contract is for.
    pub crash_mid_step: u32,
    /// At most this many entries per AppendEntries (the nodes' config).
    pub max_batch: usize,
}

impl SimConfig {
    pub fn new(nodes: u64, seed: u64) -> Self {
        SimConfig { nodes, seed, delay: (1, 3), drop: 0, duplicate: 0, compact_every: 0, send_before_persist: false, crash_mid_step: 0, max_batch: 64 }
    }
}

/// What survives a crash: the node's durable state.
#[derive(Clone, Debug, Default)]
pub struct Disk {
    pub hard: HardState,
    pub snapshot: Option<Snapshot>,
    pub entries: Vec<Entry>,
}

impl Disk {
    /// The same rules as `kv::DiskStorage`, in memory.
    fn persist(&mut self, ready: &raft_core::Ready) {
        if let Some(hs) = &ready.hard_state {
            self.hard = hs.clone();
        }
        if let Some(s) = &ready.snapshot {
            if self.snapshot.as_ref().is_none_or(|old| old.last_index < s.last_index) {
                let keep = self.entries.iter().any(|e| e.index == s.last_index && e.term == s.last_term);
                if keep {
                    self.entries.retain(|e| e.index > s.last_index);
                } else {
                    self.entries.clear();
                }
                self.snapshot = Some(s.clone());
            }
        }
        if let Some(from) = ready.truncate_from {
            self.entries.retain(|e| e.index < from);
        }
        for e in &ready.entries {
            let next = self.entries.last().map_or(self.snapshot.as_ref().map_or(1, |s| s.last_index + 1), |l| l.index + 1);
            assert_eq!(e.index, next, "the node asked to persist a non-contiguous log");
            self.entries.push(e.clone());
        }
    }
}

/// One simulated node.
pub struct SimNode {
    pub id: NodeId,
    pub core: Option<RaftNode>,
    pub disk: Disk,
    pub kv: Kv,
    /// The state machine's position.
    pub applied: Index,
    pub bugs: Bugs,
    /// How many times this node restarted.
    pub restarts: u32,
}

impl SimNode {
    pub fn alive(&self) -> bool {
        self.core.is_some()
    }
}

/// A committed command as the cluster applied it, for clients (R5).
#[derive(Clone, Debug)]
pub struct Applied {
    pub node: NodeId,
    pub index: Index,
    pub term: Term,
    pub data: Vec<u8>,
    pub reply: Option<kv::Reply>,
}

/// The simulated cluster.
pub struct Sim {
    pub cfg: SimConfig,
    pub nodes: BTreeMap<NodeId, SimNode>,
    pub now: u64,
    rng: Rng,
    /// In-flight messages, by (delivery time, send order).
    inflight: BTreeMap<(u64, u64), Envelope>,
    seq: u64,
    /// Groups of nodes that can reach each other; `None`: fully connected.
    partition: Option<Vec<BTreeSet<NodeId>>>,
    /// Every observable event, for the determinism test.
    pub trace: Vec<String>,
    /// Who led each term (election safety).
    pub leaders: BTreeMap<Term, NodeId>,
    /// The first entry applied at each index (state machine safety).
    pub committed: BTreeMap<Index, (Term, EntryKind)>,
    /// Safety violations seen so far (empty in a correct run).
    pub violations: Vec<String>,
    /// Commands applied, in order, for client bookkeeping.
    pub applied_log: Vec<Applied>,
    /// Crashes injected between a driver step's two phases.
    pub mid_step_crashes: u32,
}

impl Sim {
    pub fn new(cfg: SimConfig) -> Self {
        let members: Vec<NodeId> = (1..=cfg.nodes).collect();
        let mut nodes = BTreeMap::new();
        for &id in &members {
            let mut c = Config::new(id, members.clone(), cfg.seed);
            c.max_batch = cfg.max_batch;
            let core = RaftNode::new(c, HardState::default(), None, Vec::new());
            nodes.insert(id, SimNode { id, core: Some(core), disk: Disk::default(), kv: Kv::default(), applied: 0, bugs: Bugs::default(), restarts: 0 });
        }
        Sim {
            rng: Rng::new(cfg.seed ^ 0x5151_5151),
            cfg,
            nodes,
            now: 0,
            inflight: BTreeMap::new(),
            seq: 0,
            partition: None,
            trace: Vec::new(),
            leaders: BTreeMap::new(),
            committed: BTreeMap::new(),
            violations: Vec::new(),
            applied_log: Vec::new(),
            mid_step_crashes: 0,
        }
    }

    fn members(&self) -> Vec<NodeId> {
        (1..=self.cfg.nodes).collect()
    }

    fn config(&self, id: NodeId) -> Config {
        let mut c = Config::new(id, self.members(), self.cfg.seed);
        c.max_batch = self.cfg.max_batch;
        c
    }

    /// Test only: turn on known bugs in one node (kept across restarts).
    pub fn set_bugs(&mut self, id: NodeId, bugs: Bugs) {
        let n = self.nodes.get_mut(&id).unwrap();
        n.bugs = bugs;
        if let Some(c) = n.core.as_mut() {
            c.set_bugs(bugs);
        }
    }

    pub fn core(&self, id: NodeId) -> Option<&RaftNode> {
        self.nodes[&id].core.as_ref()
    }

    pub fn core_mut(&mut self, id: NodeId) -> Option<&mut RaftNode> {
        self.nodes.get_mut(&id).unwrap().core.as_mut()
    }

    /// The live leader with the highest term, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .values()
            .filter_map(|n| n.core.as_ref().filter(|c| c.role() == Role::Leader).map(|c| (c.term(), n.id)))
            .max()
            .map(|(_, id)| id)
    }

    fn reachable(&self, a: NodeId, b: NodeId) -> bool {
        match &self.partition {
            None => true,
            Some(groups) => groups.iter().any(|g| g.contains(&a) && g.contains(&b)),
        }
    }

    /// Splits the network into groups; messages cross no group boundary.
    pub fn partition(&mut self, groups: Vec<Vec<NodeId>>) {
        self.trace.push(format!("t{} partition {groups:?}", self.now));
        self.partition = Some(groups.into_iter().map(|g| g.into_iter().collect()).collect());
    }

    pub fn heal(&mut self) {
        self.trace.push(format!("t{} heal", self.now));
        self.partition = None;
    }

    /// Kills a node: everything not persisted is lost.
    pub fn crash(&mut self, id: NodeId) {
        self.trace.push(format!("t{} crash {id}", self.now));
        let n = self.nodes.get_mut(&id).unwrap();
        n.core = None;
        n.kv = Kv::default();
        n.applied = 0;
    }

    /// Restarts a crashed node from its disk.
    pub fn restart(&mut self, id: NodeId) {
        self.trace.push(format!("t{} restart {id}", self.now));
        let cfg = self.config(id);
        let n = self.nodes.get_mut(&id).unwrap();
        assert!(n.core.is_none(), "restarting a live node");
        let mut core = RaftNode::new(cfg, n.disk.hard.clone(), n.disk.snapshot.clone(), n.disk.entries.clone());
        core.set_bugs(n.bugs);
        n.core = Some(core);
        n.restarts += 1;
        match &n.disk.snapshot {
            Some(s) => {
                n.kv = Kv::restore(&s.data).expect("snapshots we wrote decode");
                n.applied = s.last_index;
            }
            None => {
                n.kv = Kv::default();
                n.applied = 0;
            }
        }
    }

    fn enqueue(&mut self, env: Envelope) {
        if self.rng.chance(self.cfg.drop) {
            self.trace.push(format!("t{} drop {}->{} {:?}", self.now, env.from, env.to, env.msg));
            return;
        }
        let copies = if self.rng.chance(self.cfg.duplicate) { 2 } else { 1 };
        for _ in 0..copies {
            let at = self.now + self.rng.range(self.cfg.delay.0, self.cfg.delay.1);
            self.seq += 1;
            self.inflight.insert((at, self.seq), env.clone());
        }
    }

    /// Runs the driver loop for one node until its `Ready` is empty.
    fn process(&mut self, id: NodeId) {
        loop {
            let ready = match self.nodes.get_mut(&id).unwrap().core.as_mut() {
                Some(c) => c.ready(),
                None => return,
            };
            if ready.is_empty() {
                break;
            }
            if self.cfg.send_before_persist {
                for m in ready.messages.clone() {
                    self.enqueue(m);
                }
            } else {
                self.nodes.get_mut(&id).unwrap().disk.persist(&ready);
            }
            // A crash between the two phases loses the second one.
            if self.cfg.crash_mid_step > 0 && self.rng.chance(self.cfg.crash_mid_step) {
                self.trace.push(format!("t{} crash {id} mid-step", self.now));
                self.crash(id);
                self.mid_step_crashes += 1;
                return;
            }
            if self.cfg.send_before_persist {
                self.nodes.get_mut(&id).unwrap().disk.persist(&ready);
            } else {
                for m in ready.messages.clone() {
                    self.enqueue(m);
                }
            }
            self.apply(id, &ready);
        }
        self.check(id);
    }

    fn apply(&mut self, id: NodeId, ready: &raft_core::Ready) {
        let n = self.nodes.get_mut(&id).unwrap();
        if let Some(s) = &ready.restore {
            n.kv = Kv::restore(&s.data).expect("snapshots we wrote decode");
            n.applied = s.last_index;
        }
        for e in &ready.committed {
            assert_eq!(e.index, n.applied + 1, "node {id} applied out of order");
            n.applied = e.index;
            match self.committed.get(&e.index) {
                None => {
                    self.committed.insert(e.index, (e.term, e.kind.clone()));
                }
                Some((t, k)) if *t == e.term && *k == e.kind => {}
                Some((t, _)) => self.violations.push(format!(
                    "state machine safety: node {id} applied (term {}) at index {} where term {t} was applied before",
                    e.term, e.index
                )),
            }
            let reply = match &e.kind {
                EntryKind::Noop => None,
                EntryKind::Command(data) => kv::Command::decode(data).ok().map(|c| n.kv.apply(&c)),
            };
            if let EntryKind::Command(data) = &e.kind {
                self.applied_log.push(Applied { node: id, index: e.index, term: e.term, data: data.clone(), reply });
            }
        }
        // Compaction: snapshot the state machine every `compact_every`.
        if self.cfg.compact_every > 0 {
            let snap_index = n.core.as_ref().map_or(0, |c| c.log().snapshot_index());
            if n.applied >= snap_index + self.cfg.compact_every {
                let (at, data) = (n.applied, n.kv.snapshot());
                if let Some(c) = n.core.as_mut() {
                    c.compact(at, data);
                }
            }
        }
    }

    fn check(&mut self, id: NodeId) {
        if let Some(c) = self.nodes[&id].core.as_ref() {
            if c.role() == Role::Leader {
                let (term, me) = (c.term(), id);
                match self.leaders.get(&term) {
                    None => {
                        self.leaders.insert(term, me);
                    }
                    Some(&other) if other != me => {
                        self.violations.push(format!("election safety: nodes {other} and {me} both led term {term}"));
                    }
                    _ => {}
                }
            }
        }
    }

    /// One tick: deliver what is due, then tick every live node.
    pub fn step(&mut self) {
        self.now += 1;
        while let Some((&(at, seq), _)) = self.inflight.iter().next() {
            if at > self.now {
                break;
            }
            let env = self.inflight.remove(&(at, seq)).unwrap();
            let deliverable = self.nodes[&env.to].alive() && self.reachable(env.from, env.to);
            self.trace.push(format!("t{} {} {}->{} {:?}", self.now, if deliverable { "deliver" } else { "lost" }, env.from, env.to, env.msg));
            if deliverable {
                let to = env.to;
                self.nodes.get_mut(&to).unwrap().core.as_mut().unwrap().step(env);
                self.process(to);
            }
        }
        for id in self.members() {
            if let Some(c) = self.nodes.get_mut(&id).unwrap().core.as_mut() {
                c.tick();
                self.process(id);
            }
        }
    }

    /// Steps until `done` holds or `max` ticks pass; returns whether it held.
    pub fn run_until(&mut self, max: u64, mut done: impl FnMut(&Sim) -> bool) -> bool {
        for _ in 0..max {
            if done(self) {
                return true;
            }
            self.step();
        }
        done(self)
    }

    pub fn run(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.step();
        }
    }

    /// Proposes `data` to the current leader; returns (leader, index, term).
    pub fn propose(&mut self, data: Vec<u8>) -> Option<(NodeId, Index, Term)> {
        let l = self.leader()?;
        let (index, term) = self.core_mut(l)?.propose(data).ok()?;
        self.trace.push(format!("t{} propose to {l} at {index}", self.now));
        self.process(l);
        Some((l, index, term))
    }

    /// Delivers a message straight away (for hand-built scenarios).
    pub fn inject(&mut self, env: Envelope) {
        let to = env.to;
        if let Some(c) = self.core_mut(to) {
            c.step(env);
            self.process(to);
        }
    }
}

#[cfg(test)]
mod tests;
