#!/usr/bin/env bash
# The MySQL frontend under mysqlslap: membership checks over the wire, one node.
#
#     bench/mysql.sh          (MYSQLSLAP=/path/to/mysqlslap if it is not on the path)
#
# 100 000 keys are loaded first; a hit asks for a key that is there, a miss for
# one that is not. mysqlslap runs each client in its own thread, so it is not
# a single-threaded ceiling the way one ab was - see bench/client_ceiling.sh.
set -euo pipefail

MYPORT=${MYPORT:-3398}
HTTP=${HTTP:-4097}
CLIENTS=${CLIENTS:-64}
QUERIES=${QUERIES:-640000}
KEYS=100000
WORK=$(mktemp -d)
SLAP=${MYSQLSLAP:-$(command -v mysqlslap || command -v mariadb-slap || echo /opt/homebrew/opt/mysql-client/bin/mysqlslap)}

cleanup() { kill "${NODE_PID:-}" 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT

if pgrep -f "beam.smp" >/dev/null 2>&1; then
  echo "note: another BEAM is already running - let the machine settle first"
fi

mix compile >/dev/null

KURWA_MYSQL=1 KURWA_MYSQL_PORT="$MYPORT" KURWA_HTTP_PORT="$HTTP" KURWA_DATA_DIR="$WORK/data" \
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

slap() {
  "$SLAP" -h 127.0.0.1 -P "$MYPORT" -u bench --create-schema=kurwadb --no-drop \
    --concurrency="$CLIENTS" --iterations=1 --number-of-queries="$QUERIES" --query="$1" 2>/dev/null |
    awk -v q="$QUERIES" -v label="$2" '/Average number of seconds/ {printf "  %-5s %8.0f queries/sec\n", label, q / $(NF - 1)}'
}

# warm up and discard
"$SLAP" -h 127.0.0.1 -P "$MYPORT" -u bench --create-schema=kurwadb --no-drop --concurrency=8 \
  --iterations=1 --number-of-queries=20000 --query="SELECT \`key\` FROM kurwa WHERE \`key\` = '1'" >/dev/null 2>&1
sleep 1

echo
echo "SELECT \`key\` FROM kurwa WHERE \`key\` = ..., $CLIENTS clients, mysqlslap"
echo
slap "SELECT \`key\` FROM kurwa WHERE \`key\` = '50000'" hit
slap "SELECT \`key\` FROM kurwa WHERE \`key\` = 'absent'" miss
echo
