//! Three real `raftkv` processes on localhost (milestone R6). A kill is
//! SIGKILL: no shutdown code runs, so what survives is what the process
//! had written to the OS. The OS page cache survives a process kill, so
//! this proves the runtime writes before it acks, not that it syncs
//! (power loss is not simulated here; see design §9).

use kv::{Op, Reply};
use server::{parse_cluster, status, Client, Member, Status};
use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

struct Cluster {
    spec: String,
    members: Vec<Member>,
    dir: PathBuf,
    children: BTreeMap<u64, Child>,
}

impl Cluster {
    fn new(name: &str) -> Self {
        // Free ports: bind to port 0, note the port, release it.
        let port = || TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        let spec = (1..=3).map(|id| format!("{id}={}/{}", port(), port())).collect::<Vec<_>>().join(",");
        let dir = std::env::temp_dir().join(format!("raftkv-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut c = Cluster { members: parse_cluster(&spec).unwrap(), spec, dir, children: BTreeMap::new() };
        for id in 1..=3 {
            c.start(id);
        }
        c
    }

    fn start(&mut self, id: u64) {
        let log = std::fs::File::create(self.dir.join(format!("node{id}.log"))).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_raftkv"))
            .args(["node", "--id", &id.to_string(), "--cluster", &self.spec])
            .args(["--dir", self.dir.join(format!("n{id}")).to_str().unwrap()])
            // Fast ticks keep the test short; compacting often makes the
            // restarted node catch up through InstallSnapshot over TCP.
            .args(["--tick-ms", "20", "--compact-every", "10", "--sync", "fsync"])
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        self.children.insert(id, child);
    }

    fn kill(&mut self, id: u64) {
        let mut c = self.children.remove(&id).unwrap();
        c.kill().unwrap();
        c.wait().unwrap();
    }

    fn status(&self, id: u64) -> Option<Status> {
        status(self.members[(id - 1) as usize].client).ok()
    }

    fn client(&self) -> Client {
        Client::new(self.members.clone())
    }

    /// Polls until `f` holds, or fails with the cluster's state.
    fn wait_for(&self, what: &str, f: impl Fn(&Cluster) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if f(self) {
                return;
            }
            sleep(Duration::from_millis(50));
        }
        let states: Vec<String> = (1..=3).map(|id| format!("{:?}", self.status(id))).collect();
        panic!("timed out waiting for {what}:\n{}", states.join("\n"));
    }

    fn leader(&self) -> Option<u64> {
        // The leader with the highest term (an old one may not know yet).
        (1..=3).filter_map(|id| self.status(id)).filter(|s| s.role == raft_core::Role::Leader).max_by_key(|s| s.term).map(|s| s.id)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (_, mut c) in std::mem::take(&mut self.children) {
            let _ = c.kill();
            let _ = c.wait();
        }
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.dir);
        } else {
            eprintln!("node logs kept in {}", self.dir.display());
        }
    }
}

fn put(c: &mut Client, k: &str, v: &str) {
    assert_eq!(c.call(Op::Put(k.into(), v.into())), Ok(Reply::Written), "put {k}");
}

#[test]
fn three_processes_survive_a_leader_kill_and_the_restarted_node_catches_up() {
    let mut cl = Cluster::new("kill");
    cl.wait_for("a leader", |c| c.leader().is_some());
    let mut client = cl.client();
    for i in 0..30 {
        put(&mut client, &format!("k{i}"), &format!("v{i}"));
    }

    // Kill the leader: the other two elect a new one and keep serving.
    let first = cl.leader().unwrap();
    cl.kill(first);
    for i in 30..60 {
        put(&mut client, &format!("k{i}"), &format!("v{i}"));
    }
    let second = cl.leader().unwrap();
    assert_ne!(second, first);

    // Restart it from its disk. It is far behind the leader's snapshot.
    cl.start(first);
    cl.wait_for("the restarted node to catch up", |c| {
        let (Some(l), Some(r)) = (c.status(second), c.status(first)) else { return false };
        r.applied >= l.commit && r.keys == 60
    });
    let log = std::fs::read_to_string(cl.dir.join(format!("node{first}.log"))).unwrap();
    assert!(log.contains("installed the leader's snapshot"), "it caught up through InstallSnapshot over TCP:\n{log}");

    // Now kill the new leader too. The restarted node is part of the
    // remaining majority, so everything acknowledged must still be there.
    cl.kill(second);
    cl.wait_for("a third leader", |c| c.leader().is_some());
    for i in 0..60 {
        assert_eq!(client.call(Op::Get(format!("k{i}"))), Ok(Reply::Value(Some(format!("v{i}")))), "k{i} after two leader kills");
    }
    put(&mut client, "after", "both kills");
}

#[test]
fn a_whole_cluster_restart_keeps_every_acknowledged_write() {
    let mut cl = Cluster::new("restart");
    cl.wait_for("a leader", |c| c.leader().is_some());
    let mut client = cl.client();
    for i in 0..25 {
        put(&mut client, &format!("k{i}"), &format!("v{i}"));
    }
    assert_eq!(client.call(Op::Delete("k3".into())), Ok(Reply::Deleted(true)));
    for id in 1..=3 {
        cl.kill(id);
    }
    for id in 1..=3 {
        cl.start(id);
    }
    cl.wait_for("a leader after the restart", |c| c.leader().is_some());
    for i in 0..25 {
        let want = if i == 3 { None } else { Some(format!("v{i}")) };
        assert_eq!(client.call(Op::Get(format!("k{i}"))), Ok(Reply::Value(want)), "k{i}");
    }
}
