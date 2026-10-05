#!/usr/bin/env bash
# The PostgreSQL frontend under pgbench: membership checks over the wire, in
# each of pgbench's three protocol modes, against one node.
#
#     bench/pg.sh
#
# 100 000 keys are loaded first; a "hit" run asks for keys that are there and
# a "miss" run for keys that are not. pgbench is multi-threaded (-j), so unlike
# a single ab it is not the ceiling - see bench/client_ceiling.sh.
set -euo pipefail

PGPORT=${PGPORT:-5498}
HTTP=${HTTP:-4098}
CLIENTS=${CLIENTS:-64}
THREADS=${THREADS:-4}
SECONDS_EACH=${SECONDS_EACH:-10}
KEYS=100000
WORK=$(mktemp -d)

cleanup() { kill "${NODE_PID:-}" 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT

if pgrep -f "beam.smp" >/dev/null 2>&1; then
  echo "note: another BEAM is already running - let the machine settle first"
fi

mix compile >/dev/null

KURWA_PG=1 KURWA_PG_PORT="$PGPORT" KURWA_HTTP_PORT="$HTTP" KURWA_DATA_DIR="$WORK/data" \
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

printf '\\set k random(1, %d)\nSELECT key FROM kurwa WHERE key = :k;\n' "$KEYS" >"$WORK/hit.sql"
printf '\\set k random(%d, %d)\nSELECT key FROM kurwa WHERE key = :k;\n' "$((KEYS + 1))" "$((KEYS * 2))" >"$WORK/miss.sql"

run() {
  pgbench -n -h 127.0.0.1 -p "$PGPORT" -U bench kurwadb -M "$1" -f "$WORK/$2.sql" \
    -c "$CLIENTS" -j "$THREADS" -T "$SECONDS_EACH" 2>/dev/null |
    awk -v mode="$1" -v kind="$2" '/latency average/ {latency = $4}
                                   /^tps/ {printf "  %-9s %-4s %8.0f tps   latency %s ms\n", mode, kind, $3, latency}'
}

# warm up and discard
pgbench -n -h 127.0.0.1 -p "$PGPORT" -U bench kurwadb -M prepared -f "$WORK/hit.sql" -c 8 -T 2 >/dev/null 2>&1
sleep 1

echo
echo "SELECT key FROM kurwa WHERE key = ..., $CLIENTS clients, $THREADS pgbench threads, ${SECONDS_EACH}s each"
echo
for mode in simple extended prepared; do
  run "$mode" hit
  run "$mode" miss
done
echo
