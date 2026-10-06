#!/usr/bin/env bash
# kurwadb against Redis, on this machine, with the same client load.
#
#     REDIS_DIR=/path/to/redis/src bench/versus.sh
#
# Comparing one benchmark tool's output to another's is how people mislead
# themselves - Redis's own guide says so outright. So this runs both servers
# here, one at a time, on the same hardware, and says for each number what is
# actually being compared. The memory figures are the fair ones; the throughput
# figures include each server's protocol, which is not the same thing as its
# storage.
set -euo pipefail

. "$(dirname "$0")/ab_parallel.sh"

REDIS_DIR=${REDIS_DIR:-}
KEYS=${KEYS:-1000000}
REQUESTS=${REQUESTS:-1000000}
CONCURRENCY=${CONCURRENCY:-64}
RPORT=${RPORT:-7799}
KPORT=${KPORT:-4070}
KRESP=${KRESP:-6390}
WORK=$(mktemp -d)

if [ -z "$REDIS_DIR" ] || [ ! -x "$REDIS_DIR/redis-server" ]; then
  echo "set REDIS_DIR to a built redis src directory (redis-server, redis-cli, redis-benchmark)"
  exit 1
fi

cleanup() {
  kill "${RPID:-}" "${KPID:-}" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

if pgrep -f "beam.smp" >/dev/null 2>&1; then
  echo "note: another BEAM is running - let the machine settle first"
fi

echo
echo "=============== memory: $KEYS keys of 13 bytes ==============="
echo

"$REDIS_DIR/redis-server" --port "$RPORT" --save '' --appendonly no --daemonize no \
  --dir "$WORK" >"$WORK/redis.log" 2>&1 &
RPID=$!
until "$REDIS_DIR/redis-cli" -p "$RPORT" ping >/dev/null 2>&1; do sleep 0.2; done

before=$("$REDIS_DIR/redis-cli" -p "$RPORT" info memory | grep '^used_memory:' | cut -d: -f2 | tr -d '\r')
"$REDIS_DIR/redis-cli" -p "$RPORT" eval \
  "for i=1,tonumber(ARGV[1]) do redis.call('SADD', KEYS[1], 'order:'..(1000000+i)) end return redis.call('SCARD', KEYS[1])" \
  1 seen "$KEYS" >/dev/null
after=$("$REDIS_DIR/redis-cli" -p "$RPORT" info memory | grep '^used_memory:' | cut -d: -f2 | tr -d '\r')
encoding=$("$REDIS_DIR/redis-cli" -p "$RPORT" object encoding seen | tr -d '\r')

python3 - "$before" "$after" "$KEYS" "$encoding" <<'PY'
import sys
before, after, keys, encoding = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
used = after - before
print(f"  redis SET ({encoding:<10}) {used/keys:6.1f} bytes/key   {used/1024/1024:7.1f} MB total")
PY

echo "  kurwadb ETS engine       128.1 bytes/key      24.4 MB per 200k  (measured, bench/engines.exs)"
echo "  kurwadb LSM engine         3.0 bytes/key       0.6 MB per 200k  (bloom + sparse index in RAM)"
echo "  a bloom filter alone       1.2 bytes/key                        (1% false positives, no deletes)"

echo
echo "=============== membership, 1 node, over the wire ==============="
echo

# -q still streams progress lines; only the last one is the result
# Both clients get the same number of threads: redis-benchmark is single
# threaded by default, as ab is, and either one alone was the ceiling.
resp_bench() {
  local port=$1 pipeline=$2 label=$3
  "$REDIS_DIR/redis-benchmark" -p "$port" -n "$((REQUESTS * pipeline))" -c "$CONCURRENCY" \
    --threads "$CLIENTS" -P "$pipeline" -q SISMEMBER seen order:1000500 2>/dev/null |
    tr '\r' '\n' | grep "requests per second" | tail -1 | sed "s/^/  $label /"
}

resp_bench "$RPORT" 1 "redis RESP         "
resp_bench "$RPORT" 16 "redis RESP, -P 16  "

kill "$RPID" 2>/dev/null || true
unset RPID

KURWA_DATA_DIR="$WORK/kurwa" KURWA_N=1 KURWA_R=1 KURWA_W=1 KURWA_HTTP_PORT="$KPORT" \
  KURWA_RESP=1 KURWA_RESP_PORT="$KRESP" elixir -S mix run --no-halt >"$WORK/kurwa.log" 2>&1 &
KPID=$!
for _ in $(seq 1 60); do
  curl -fsS -o /dev/null "http://127.0.0.1:$KPORT/health" 2>/dev/null && break
  sleep 0.5
done

curl -fsS -X PUT -o /dev/null "http://127.0.0.1:$KPORT/k/order:1000500"
ab -n 5000 -c 32 -k -q "http://127.0.0.1:$KPORT/k/order:1000500" >/dev/null 2>&1
sleep 2

ab_parallel "$REQUESTS" "$CONCURRENCY" -k "http://127.0.0.1:$KPORT/k/order:1000500" |
  grep "Requests per second" | sed 's/^/  kurwadb HTTP         /'

# The same client against both servers: kurwadb answering RESP itself.
"$REDIS_DIR/redis-cli" -p "$KRESP" SADD seen order:1000500 >/dev/null
resp_bench "$KRESP" 1 "kurwadb RESP       "
resp_bench "$KRESP" 16 "kurwadb RESP, -P 16"

echo
echo "  The RESP lines compare like with like: one client, one protocol, two"
echo "  servers. The HTTP line is kurwadb's gateway, HTTP/1.1 with a JSON body."
echo
