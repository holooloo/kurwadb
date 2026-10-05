#!/usr/bin/env bash
# The HTTP gateway, measured with ApacheBench (ships with macOS).
#
#     bench/http.sh
#
# One node, n=1, so this measures the gateway and the store, not replication.
set -euo pipefail

. "$(dirname "$0")/ab_parallel.sh"

PORT=${KURWA_BENCH_PORT:-4060}
DIR=$(mktemp -d)
BODY="$DIR/batch.json"

cleanup() { kill "${NODE_PID:-}" 2>/dev/null || true; rm -rf "$DIR"; }
trap cleanup EXIT

# Two published numbers have already been wrong because a run measured the tail
# of whatever ran before it. This one is especially sensitive: measured within a
# minute of the cluster benchmark it reads ~30% low.
# No command substitution here on purpose: pgrep exits 1 when it finds nothing,
# and with `set -o pipefail` that took the whole script down without a word.
if pgrep -f "beam.smp" >/dev/null 2>&1; then
  echo "note: another BEAM is already running - let the machine settle first"
fi

mix compile >/dev/null

KURWA_DATA_DIR="$DIR/data" KURWA_N=1 KURWA_R=1 KURWA_W=1 KURWA_HTTP_PORT="$PORT" \
  elixir -S mix run --no-halt >"$DIR/node.log" 2>&1 &
NODE_PID=$!

for _ in $(seq 1 40); do
  curl -fsS -o /dev/null "http://127.0.0.1:$PORT/health" 2>/dev/null && break
  sleep 0.5
done

curl -fsS -X PUT -o /dev/null "http://127.0.0.1:$PORT/k/benchkey"

# Warm up and discard. Two published numbers have already been wrong because a
# run measured the tail of whatever ran before it - sockets in TIME_WAIT once,
# a cluster still shutting down the next time.
ab -n 3000 -c 32 -k -q "http://127.0.0.1:$PORT/k/benchkey" >/dev/null 2>&1
sleep 3
python3 - "$BODY" <<'PY'
import json, sys
json.dump({"op": "member", "keys": [f"bench:{i}" for i in range(100)]}, open(sys.argv[1], "w"))
PY

echo
echo "GET /k/:key  — keep-alive, 64 concurrent"
ab_parallel 120000 64 -k "http://127.0.0.1:$PORT/k/benchkey"

echo
echo "GET /k/:key  — new connection per request"
# One client on purpose: this one measures TCP setup in the kernel, and more
# clients only add contention for it (four ab read 25 000 here, one 30 000).
CLIENTS=1 ab_parallel 10000 64 "http://127.0.0.1:$PORT/k/benchkey"

echo
echo "POST /batch  — 100 keys per request, keep-alive"
ab_parallel 8000 16 -k -p "$BODY" -T application/json "http://127.0.0.1:$PORT/batch"
echo
