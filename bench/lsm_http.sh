#!/usr/bin/env bash
# Both engines over HTTP, with a million keys loaded and the ones asked about
# actually on disk.
#
#     bench/lsm_http.sh
#
# bench/versus.sh writes one key into the default engine, so it never measured
# the on-disk engine over the wire. This does: each engine gets the same million
# keys through POST /batch, and then answers for a key that is present and one
# that is not. Eight shards flush at 100 000 keys each, so the keys loaded first
# are in a table on disk by the end, not in a memtable - the script checks the
# log for the flushes rather than assuming them.
#
# The data set is ~70 MB on disk, which the OS keeps in its page cache. So a
# "present" read here is a pread that the kernel answers from memory, not a seek
# on the SSD. That is the steady state for a set that fits in page cache, and
# the best case for one that does not.
set -euo pipefail

PORT=${KURWA_BENCH_PORT:-4061}
KEYS=${KEYS:-1000000}
REQUESTS=${REQUESTS:-200000}
CONCURRENCY=${CONCURRENCY:-64}
WORK=$(mktemp -d)

cleanup() { kill "${NODE_PID:-}" 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT

if pgrep -f "beam.smp" >/dev/null 2>&1; then
  echo "note: another BEAM is already running - let the machine settle first"
fi

mix compile >/dev/null

# order:1000001 goes in with the first batch, so it is on disk by the end;
# absent:1 never goes in at all.
PRESENT="order:1000001"
ABSENT="absent:1"

run() {
  local engine=$1
  KURWA_ENGINE="$engine" KURWA_DATA_DIR="$WORK/$engine" KURWA_N=1 KURWA_R=1 KURWA_W=1 \
    KURWA_HTTP_PORT="$PORT" elixir -S mix run --no-halt >"$WORK/$engine.log" 2>&1 &
  NODE_PID=$!

  for _ in $(seq 1 60); do
    curl -fsS -o /dev/null "http://127.0.0.1:$PORT/health" 2>/dev/null && break
    sleep 0.5
  done

  python3 - "$PORT" "$KEYS" <<'PY'
import http.client, json, sys
port, keys = int(sys.argv[1]), int(sys.argv[2])
conn = http.client.HTTPConnection("127.0.0.1", port)
batch = 1000
for start in range(0, keys, batch):
    body = json.dumps({"op": "add", "keys": [f"order:{1000001 + i}" for i in range(start, min(start + batch, keys))]})
    conn.request("POST", "/batch", body, {"Content-Type": "application/json"})
    r = conn.getresponse(); r.read()
    if r.status != 200:
        sys.exit(f"batch at {start} answered {r.status}")
PY

  local flushes
  flushes=$(grep -c "flushed" "$WORK/$engine.log" || true)

  # warm up and discard
  ab -n 5000 -c 32 -k -q "http://127.0.0.1:$PORT/k/$PRESENT" >/dev/null 2>&1
  sleep 2

  local hit miss
  hit=$(ab -n "$REQUESTS" -c "$CONCURRENCY" -k -q "http://127.0.0.1:$PORT/k/$PRESENT" 2>/dev/null |
    awk '/Requests per second/ {print $4}')
  miss=$(ab -n "$REQUESTS" -c "$CONCURRENCY" -k -q "http://127.0.0.1:$PORT/k/$ABSENT" 2>/dev/null |
    awk '/Requests per second/ {print $4}')

  printf "  %-4s  present %9s req/sec   absent %9s req/sec   tables flushed %2s\n" \
    "$engine" "$hit" "$miss" "$flushes"

  kill "$NODE_PID" 2>/dev/null || true
  wait "$NODE_PID" 2>/dev/null || true
  unset NODE_PID
  sleep 3
}

echo
echo "GET /k/:key, $KEYS keys loaded, $CONCURRENCY clients, keep-alive"
echo
run ets
run lsm
echo
echo "  An absent key answers 404, which ab reports as non-2xx; that is the"
echo "  correct answer, not a failure. Memory per key is measured precisely by"
echo "  bench/engines.exs; the whole node's RSS after a bulk load is mostly garbage"
echo "  not yet returned to the OS, and says little about either engine."
echo
