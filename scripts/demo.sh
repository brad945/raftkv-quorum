#!/usr/bin/env bash
# Three raftkv processes on localhost: write, kill the leader, keep
# writing, restart it, watch it catch up. Run from the repo root:
#   scripts/demo.sh
set -euo pipefail

CARGO="${CARGO:-$(command -v cargo || echo "$HOME/.cargo/bin/cargo")}"
"$CARGO" build --release -q -p server
BIN=target/release/raftkv
CLUSTER="1=127.0.0.1:7001/127.0.0.1:8001,2=127.0.0.1:7002/127.0.0.1:8002,3=127.0.0.1:7003/127.0.0.1:8003"
DATA=demo-data
rm -rf "$DATA" && mkdir -p "$DATA"
PID=()

start() {
  "$BIN" node --id "$1" --cluster "$CLUSTER" --dir "$DATA/n$1" --compact-every 20 2>>"$DATA/node$1.log" &
  PID[$1]=$!; disown
}
cleanup() { for p in ${PID[@]+"${PID[@]}"}; do kill -9 "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

kv() { "$BIN" client --cluster "$CLUSTER" "$@"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
leader() { kv status | awk '/Leader/ { sub(":", "", $2); print $2; exit }'; }
wait_leader() { for _ in $(seq 100); do l=$(leader); [ -n "$l" ] && { echo "$l"; return; }; sleep 0.1; done; echo "no leader" >&2; exit 1; }

step "Start 3 nodes"
for id in 1 2 3; do start "$id"; done
L=$(wait_leader)
kv status

step "Write through the leader (node $L); every write is fsynced on a majority first"
for i in $(seq 1 30); do kv put "user$i" "balance=$((i * 10))" >/dev/null; done
echo "30 writes OK. user7 = $(kv get user7)"

step "kill -9 the leader (node $L): no clean shutdown"
kill -9 "${PID[$L]}"; unset "PID[$L]"
NEW=$(wait_leader)
echo "node $NEW took over"
for i in $(seq 31 60); do kv put "user$i" "balance=$((i * 10))" >/dev/null; done
echo "30 more writes OK with one node down. user45 = $(kv get user45)"
kv status

step "Restart node $L from its disk; it is behind the leader's snapshot"
start "$L"
for _ in $(seq 100); do
  line=$(kv status | grep "^node $L:" || true)
  case "$line" in *"keys 60"*) break ;; esac
  sleep 0.1
done
kv status
grep -h "installed the leader's snapshot" "$DATA/node$L.log" || true

step "Kill the current leader (node $NEW) too; the restarted node is now part of the majority"
kill -9 "${PID[$NEW]}"; unset "PID[$NEW]"
wait_leader >/dev/null
echo "user1 = $(kv get user1)   user60 = $(kv get user60)"
kv status
step "Done. Node logs: $DATA/node*.log"
