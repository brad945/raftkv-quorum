//! `raftkv`: run a node, or talk to a cluster.
//!
//! ```text
//! raftkv node --id 1 --cluster 1=127.0.0.1:7001/127.0.0.1:8001,2=...,3=... --dir data/1
//!             [--tick-ms 50] [--compact-every 1000] [--sync full|fsync|none]
//! raftkv client --cluster ... put <key> <value> | get <key> | del <key> | status
//! ```
//! In `--cluster`, each member is `id=raft_addr/client_addr`.

use kv::{Op, Reply, SyncMode};
use server::{parse_cluster, run_node, status, Client, NodeOptions};
use std::collections::BTreeMap;
use std::process::ExitCode;
use std::time::Duration;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("raftkv: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Splits `--flag value` pairs from positional arguments.
fn flags(args: &[String]) -> Result<(BTreeMap<String, String>, Vec<String>), String> {
    let mut f = BTreeMap::new();
    let mut pos = Vec::new();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if let Some(name) = a.strip_prefix("--") {
            let v = it.next().ok_or(format!("--{name} needs a value"))?;
            f.insert(name.to_string(), v.clone());
        } else {
            pos.push(a.clone());
        }
    }
    Ok((f, pos))
}

fn run(args: &[String]) -> Result<(), String> {
    let (cmd, rest) = args.split_first().ok_or("usage: raftkv node|client ... (see --help in the source header)")?;
    let (f, pos) = flags(rest)?;
    let cluster = parse_cluster(f.get("cluster").ok_or("--cluster is required")?)?;
    match cmd.as_str() {
        "node" => {
            let num = |name: &str, default: u64| f.get(name).map_or(Ok(default), |v| v.parse::<u64>().map_err(|_| format!("--{name}: not a number")));
            let sync = match f.get("sync").map(String::as_str) {
                None | Some("full") => SyncMode::Full,
                Some("fsync") => SyncMode::Fsync,
                Some("none") => SyncMode::None,
                Some(s) => return Err(format!("--sync {s}: expected full, fsync or none")),
            };
            run_node(NodeOptions {
                id: f.get("id").ok_or("--id is required")?.parse().map_err(|_| "--id: not a number")?,
                cluster,
                dir: f.get("dir").ok_or("--dir is required")?.into(),
                sync,
                tick: Duration::from_millis(num("tick-ms", 50)?),
                compact_every: num("compact-every", 1000)?,
            })
        }
        "client" => {
            let op = match pos.iter().map(String::as_str).collect::<Vec<_>>()[..] {
                ["put", k, ref v @ ..] if !v.is_empty() => Op::Put(k.into(), v.join(" ")),
                ["get", k] => Op::Get(k.into()),
                ["del", k] => Op::Delete(k.into()),
                ["status"] => {
                    for m in &cluster {
                        match status(m.client) {
                            Ok(s) => println!(
                                "node {}: {:?} term {} leader {:?} commit {} applied {} log to {} snapshot at {} keys {}",
                                s.id, s.role, s.term, s.leader, s.commit, s.applied, s.last_index, s.snapshot_index, s.keys
                            ),
                            Err(e) => println!("node {}: down ({e})", m.id),
                        }
                    }
                    return Ok(());
                }
                _ => return Err("client: put <key> <value> | get <key> | del <key> | status".into()),
            };
            match Client::new(cluster).call(op)? {
                Reply::Value(Some(v)) => println!("{v}"),
                Reply::Value(None) => println!("(not found)"),
                Reply::Written => println!("OK"),
                Reply::Deleted(existed) => println!("{}", if existed { "deleted" } else { "(not found)" }),
                Reply::Stale => return Err("stale request".into()),
            }
            Ok(())
        }
        other => Err(format!("unknown command {other:?}: expected node or client")),
    }
}
