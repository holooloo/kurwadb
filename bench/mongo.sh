#!/usr/bin/env bash
# The MongoDB frontend under the official Node.js driver: findOne by _id,
# 64 operations in flight, one node.
#
#     NODE_MONGODB=/path/to/node_modules/mongodb bench/mongo.sh
#
# There is no pgbench for MongoDB, so this is bench/mongo_client.js, several
# processes of it: one Node process was its own ceiling, as one ab was.
set -euo pipefail

PORT=${MONGO_PORT:-27098}
HTTP=${HTTP:-4096}
PROCS=${PROCS:-4}
CONCURRENCY=${CONCURRENCY:-16}
SECONDS_EACH=${SECONDS_EACH:-10}
KEYS=100000
WORK=$(mktemp -d)
HERE=$(cd "$(dirname "$0")" && pwd)

cleanup() { kill "${NODE_PID:-}" 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT

if pgrep -f "beam.smp" >/dev/null 2>&1; then
  echo "note: another BEAM is already running - let the machine settle first"
fi

mix compile >/dev/null

KURWA_MONGO=1 KURWA_MONGO_PORT="$PORT" KURWA_HTTP_PORT="$HTTP" KURWA_DATA_DIR="$WORK/data" \
  KURWA_N=1 KURWA_R=1 KURWA_W=1 elixir -S mix run --no-halt >"$WORK/node.log" 2>&1 &
NODE_PID=$!
for _ in $(seq 1 60); do
  curl -fsS -o /dev/null "http://127.0.0.1:$HTTP/health" 2>/dev/null && break
  sleep 0.5
done

python3 - "$HTTP" "$KEYS" <<'PY'
import http.client, json, sys
port, keys = int(sys.argv[1]), int(sys.argv[2])
conn = http.client.HTTPConnection("127.0.0.1", port)
for start in range(1, keys + 1, 1000):
    body = json.dumps({"op": "add", "keys": [str(i) for i in range(start, min(start + 1000, keys + 1))]})
    conn.request("POST", "/batch", body, {"Content-Type": "application/json"})
    r = conn.getresponse(); r.read()
    assert r.status == 200, r.status
PY

run() {
  local total=0 pids=()
  for i in $(seq 1 "$PROCS"); do
    node "$HERE/mongo_client.js" "mongodb://127.0.0.1:$PORT/" "$SECONDS_EACH" "$CONCURRENCY" "$1" "$KEYS" >"$WORK/$1.$i" &
    pids+=($!)
  done
  wait "${pids[@]}"
  for i in $(seq 1 "$PROCS"); do total=$((total + $(cat "$WORK/$1.$i"))); done
  printf "  %-5s %8d ops/sec\n" "$1" $((total / SECONDS_EACH))
}

node "$HERE/mongo_client.js" "mongodb://127.0.0.1:$PORT/" 2 8 hit "$KEYS" >/dev/null
sleep 1

echo
echo "findOne({_id}), $PROCS node processes x $CONCURRENCY in flight, ${SECONDS_EACH}s each"
echo
run hit
run miss
echo
