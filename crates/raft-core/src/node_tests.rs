//! Unit tests of the pure node for the classic Raft bugs (design §8).
//! Messages are handed in directly, so each test builds exactly the
//! situation it is about.

use super::*;
use crate::{Entry, EntryKind, HardState};

fn cfg(id: NodeId) -> Config {
    Config::new(id, vec![1, 2, 3], 7)
}

fn e(term: Term, index: Index) -> Entry {
    Entry { term, index, kind: EntryKind::Command(vec![index as u8]) }
}

fn node(id: NodeId, term: Term, entries: Vec<Entry>) -> RaftNode {
    RaftNode::new(cfg(id), HardState { term, voted_for: None }, None, entries)
}

fn env(from: NodeId, to: NodeId, msg: Message) -> Envelope {
    Envelope { from, to, msg }
}

/// Makes `n` the leader of `term + 1` with votes from 2 (3 is silent).
fn elect(n: &mut RaftNode) {
    n.force_election_timeout(1);
    n.tick();
    assert_eq!(n.role(), Role::Candidate);
    let term = n.term();
    n.step(env(2, n.id(), Message::RequestVoteResp { term, granted: true }));
    assert_eq!(n.role(), Role::Leader);
    n.ready();
}

fn ack(n: &mut RaftNode, from: NodeId, match_index: Index) {
    let term = n.term();
    n.step(env(from, n.id(), Message::AppendEntriesResp { term, success: true, match_index, hint: 0 }));
}

// Paper §5.4.2 / Figure 8: a leader must not commit an entry of an earlier
// term just because a majority stores it; it commits it indirectly, once an
// entry of its own term is on a majority.
#[test]
fn figure8_no_commit_of_old_term_by_count() {
    let mut l = node(1, 2, vec![e(1, 1), e(2, 2)]);
    elect(&mut l); // term 3; appends a no-op at index 3
    assert_eq!(l.term(), 3);
    assert_eq!(l.last_index(), 3);
    ack(&mut l, 2, 2); // follower 2 holds 1..2 (term 1 and 2 entries)
    assert_eq!(l.commit_index(), 0, "an old-term entry on a majority is not committed by counting");
    ack(&mut l, 2, 3); // now it holds the term-3 no-op too
    assert_eq!(l.commit_index(), 3, "committing the current-term no-op commits everything before it");
    let committed: Vec<Index> = l.ready().committed.iter().map(|e| e.index).collect();
    assert_eq!(committed, vec![1, 2, 3]);

    // The planted bug commits index 2 at the first ack.
    let mut bad = node(1, 2, vec![e(1, 1), e(2, 2)]);
    bad.set_bugs(Bugs { commit_old_term_by_count: true, ..Bugs::default() });
    elect(&mut bad);
    ack(&mut bad, 2, 2);
    assert_eq!(bad.commit_index(), 2, "the bug flag reproduces the Figure 8 mistake");
}

// A delayed or duplicated AppendEntries whose entries the follower already
// has must not delete the entries after them.
#[test]
fn reordered_append_does_not_truncate() {
    let mut f = node(2, 1, vec![e(1, 1), e(1, 2), e(1, 3)]);
    // An old AppendEntries carrying only entry 2 arrives late.
    f.step(env(1, 2, Message::AppendEntries { term: 1, prev_index: 1, prev_term: 1, entries: vec![e(1, 2)], leader_commit: 0 }));
    assert_eq!(f.last_index(), 3, "matching entries were kept");
    let r = f.ready();
    assert_eq!(r.truncate_from, None);
    assert!(matches!(r.messages[0].msg, Message::AppendEntriesResp { success: true, match_index: 2, .. }));

    let mut bad = node(2, 1, vec![e(1, 1), e(1, 2), e(1, 3)]);
    bad.set_bugs(Bugs { truncate_on_every_append: true, ..Bugs::default() });
    bad.step(env(1, 2, Message::AppendEntries { term: 1, prev_index: 1, prev_term: 1, entries: vec![e(1, 2)], leader_commit: 0 }));
    assert_eq!(bad.last_index(), 2, "the bug flag drops entry 3");
}

#[test]
fn conflicting_entries_are_replaced() {
    let mut f = node(2, 2, vec![e(1, 1), e(1, 2), e(1, 3)]);
    f.step(env(1, 2, Message::AppendEntries { term: 2, prev_index: 1, prev_term: 1, entries: vec![e(2, 2)], leader_commit: 0 }));
    assert_eq!(f.last_index(), 2);
    assert_eq!(f.log().term_at(2), Some(2));
    let r = f.ready();
    assert_eq!(r.truncate_from, Some(2));
    assert_eq!(r.entries, vec![e(2, 2)]);
}

#[test]
fn missing_prev_is_rejected_with_a_hint() {
    let mut f = node(2, 1, vec![e(1, 1)]);
    f.step(env(1, 2, Message::AppendEntries { term: 1, prev_index: 5, prev_term: 1, entries: vec![e(1, 6)], leader_commit: 0 }));
    let r = f.ready();
    assert!(matches!(r.messages[0].msg, Message::AppendEntriesResp { success: false, hint: 2, .. }), "{:?}", r.messages);
    assert_eq!(f.last_index(), 1);
}

#[test]
fn match_index_never_decreases_and_stale_response_ignored() {
    let mut l = node(1, 0, vec![]);
    elect(&mut l);
    for _ in 0..4 {
        l.propose(b"x".to_vec()).unwrap();
    }
    ack(&mut l, 2, 5);
    assert_eq!(l.match_index(2), 5);
    ack(&mut l, 2, 3); // a delayed, older ack
    assert_eq!(l.match_index(2), 5, "a stale ack can't move match_index back");
    assert!(l.next_index(2) >= 6);
    // A rejection with a low hint can't push next_index below what the
    // peer is known to hold.
    let term = l.term();
    l.step(env(2, 1, Message::AppendEntriesResp { term, success: false, match_index: 0, hint: 1 }));
    assert!(l.next_index(2) >= 6, "next_index {}", l.next_index(2));
}

#[test]
fn higher_term_in_response_steps_down() {
    let mut l = node(1, 0, vec![]);
    elect(&mut l);
    assert_eq!(l.role(), Role::Leader);
    l.step(env(3, 1, Message::AppendEntriesResp { term: 9, success: false, match_index: 0, hint: 0 }));
    assert_eq!(l.role(), Role::Follower);
    assert_eq!(l.term(), 9);
    assert_eq!(l.ready().hard_state, Some(HardState { term: 9, voted_for: None }), "the new term is persisted");
    assert!(matches!(l.propose(vec![]), Err(ProposeError::NotLeader { .. })));
}

#[test]
fn stale_term_messages_are_refused() {
    let mut f = node(2, 5, vec![]);
    f.step(env(1, 2, Message::AppendEntries { term: 3, prev_index: 0, prev_term: 0, entries: vec![e(3, 1)], leader_commit: 1 }));
    assert_eq!(f.last_index(), 0);
    let r = f.ready();
    assert!(matches!(r.messages[0].msg, Message::AppendEntriesResp { term: 5, success: false, .. }));
    // A stale response is dropped silently.
    let mut l = node(1, 0, vec![]);
    elect(&mut l);
    let t = l.term();
    l.step(env(2, 1, Message::RequestVoteResp { term: t - 1, granted: true }));
    assert!(l.ready().messages.is_empty());
}

#[test]
fn vote_rules() {
    // One vote per term.
    let mut f = node(3, 1, vec![e(1, 1)]);
    f.step(env(1, 3, Message::RequestVote { term: 2, last_index: 1, last_term: 1 }));
    assert_eq!(f.hard_state().voted_for, Some(1));
    f.step(env(2, 3, Message::RequestVote { term: 2, last_index: 9, last_term: 1 }));
    assert_eq!(f.hard_state().voted_for, Some(1), "no second vote in the same term");
    let r = f.ready();
    assert_eq!(r.hard_state, Some(HardState { term: 2, voted_for: Some(1) }), "the vote is in the same Ready as the grant");
    let grants: Vec<bool> = r.messages.iter().map(|m| matches!(m.msg, Message::RequestVoteResp { granted: true, .. })).collect();
    assert_eq!(grants, vec![true, false]);

    // §5.4.1: no vote for a candidate whose log is behind.
    let mut g = node(3, 1, vec![e(1, 1), e(1, 2)]);
    g.step(env(1, 3, Message::RequestVote { term: 2, last_index: 1, last_term: 1 }));
    assert_eq!(g.hard_state().voted_for, None, "shorter log, same last term");
    g.step(env(2, 3, Message::RequestVote { term: 3, last_index: 1, last_term: 2 }));
    assert_eq!(g.hard_state().voted_for, Some(2), "higher last term wins even if shorter");
}

#[test]
fn leader_commits_with_a_majority_and_followers_learn_it() {
    let mut l = node(1, 0, vec![]);
    elect(&mut l);
    let (idx, _) = l.propose(b"a".to_vec()).unwrap();
    assert_eq!(l.commit_index(), 0);
    ack(&mut l, 3, idx);
    assert_eq!(l.commit_index(), idx);
    let mut f = node(2, l.term(), vec![]);
    let entries = l.log().slice(1, 10);
    f.step(env(1, 2, Message::AppendEntries { term: l.term(), prev_index: 0, prev_term: 0, entries, leader_commit: idx }));
    assert_eq!(f.commit_index(), idx);
    assert_eq!(f.ready().committed.len(), idx as usize);
}

#[test]
fn single_node_cluster_commits_alone() {
    let mut n = RaftNode::new(Config::new(1, vec![1], 1), HardState::default(), None, vec![]);
    n.tick();
    assert_eq!(n.role(), Role::Leader);
    let (i, _) = n.propose(b"solo".to_vec()).unwrap();
    assert_eq!(n.commit_index(), i);
}

// Figure 2, AppendEntries rule 5: commitIndex = min(leaderCommit, index of
// the last new entry). A heartbeat verifies the log only up to prev_index;
// a stale, unverified tail beyond it must not be committed just because the
// leader's commit index is higher.
#[test]
fn follower_commits_only_what_the_append_verified() {
    // Index 3 is a stale entry from an old leader; the new leader's index 3
    // is different and committed.
    let mut f = node(2, 2, vec![e(1, 1), e(1, 2), e(1, 3)]);
    f.step(env(1, 2, Message::AppendEntries { term: 2, prev_index: 1, prev_term: 1, entries: vec![], leader_commit: 3 }));
    assert_eq!(f.commit_index(), 1, "only index 1 was verified by this append");
    assert_eq!(f.ready().committed.len(), 1);
}
