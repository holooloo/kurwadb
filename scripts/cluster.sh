#!/usr/bin/env bash
# Boots a local kurwadb cluster: N nodes, one OS process each, all seeded at
# each other. Ctrl-C stops the lot.
#
#   scripts/cluster.sh          3 nodes on 4040, 4041, 4042
#   scripts/cluster.sh 5        5 nodes
#
# KURWA_MONGO_BASE_PORT=27101 also opens the MongoDB protocol on 27101, 27102,
# ... - give a driver all of them and it treats each node as a mongos router.
#
# Every node keeps its own WAL under data/cluster/<node>/ (Kurwa.Store scopes the
# data directory by node name), so one data dir for the whole cluster is fine.
set -euo pipefail

NODES=${1:-3}
HOST=${KURWA_HOST:-127.0.0.1}
COOKIE=${KURWA_COOKIE:-kurwa}
BASE_PORT=${KURWA_BASE_PORT:-4040}
export KURWA_DATA_DIR=${KURWA_DATA_DIR:-data/cluster}
export KURWA_N=${KURWA_N:-3}
export KURWA_R=${KURWA_R:-2}
export KURWA_W=${KURWA_W:-2}

SEEDS=""
for i in $(seq 1 "$NODES"); do
  SEEDS="${SEEDS}kurwa${i}@${HOST},"
done
export KURWA_SEEDS="${SEEDS%,}"

# Compile once, so the nodes do not race each other over _build.
mix compile

pids=()
cleanup() {
  trap - INT TERM EXIT
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup INT TERM EXIT

for i in $(seq 1 "$NODES"); do
  port=$((BASE_PORT + i - 1))
  echo "starting kurwa${i}@${HOST} on http port ${port}"
  mongo=()
  if [ -n "${KURWA_MONGO_BASE_PORT:-}" ]; then
    mongo=(KURWA_MONGO=1 KURWA_MONGO_PORT=$((KURWA_MONGO_BASE_PORT + i - 1)))
  fi
  env "${mongo[@]}" KURWA_HTTP_PORT=$port elixir --name "kurwa${i}@${HOST}" --cookie "$COOKIE" \
    -S mix run --no-halt &
  pids+=($!)
done

wait
