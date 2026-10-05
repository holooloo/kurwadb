#!/usr/bin/env bash
# Is the load generator the ceiling? The same 64 connections in total, split
# across 1, 2, 4 and 8 ab processes against one node. If the sum keeps growing,
# the figure was the client's. It was: see "HTTP gateway" in PERFORMANCE.md.
#
#     bench/client_ceiling.sh
set -euo pipefail
PORT=4062; DIR=$(mktemp -d)
ENGINE=${ENGINE:-ets}
cleanup() { kill "${NODE_PID:-}" 2>/dev/null || true; rm -rf "$DIR"; }
trap cleanup EXIT
KURWA_ENGINE=$ENGINE KURWA_DATA_DIR="$DIR/data" KURWA_N=1 KURWA_R=1 KURWA_W=1 KURWA_HTTP_PORT=$PORT \
  elixir -S mix run --no-halt >"$DIR/node.log" 2>&1 &
NODE_PID=$!
for _ in $(seq 1 60); do curl -fsS -o /dev/null "http://127.0.0.1:$PORT/health" 2>/dev/null && break; sleep 0.5; done
curl -fsS -X PUT -o /dev/null "http://127.0.0.1:$PORT/k/benchkey"
ab -n 20000 -c 32 -k -q "http://127.0.0.1:$PORT/k/benchkey" >/dev/null 2>&1; sleep 2
for procs in 1 2 4 8; do
  c=$((64 / procs)); n=$((240000 / procs))
  for _ in $(seq 1 $procs); do
    ab -n $n -c $c -k -q "http://127.0.0.1:$PORT/k/benchkey" 2>/dev/null | awk '/Requests per second/ {print $4}' >> "$DIR/r.$procs" &
  done
  wait $(jobs -p | grep -v "^$NODE_PID$") 2>/dev/null || true
  sleep 1
  printf "  %d x ab -c %-3d  total %8.0f req/sec\n" $procs $c "$(paste -sd+ "$DIR/r.$procs" | bc)"
  sleep 2
done
