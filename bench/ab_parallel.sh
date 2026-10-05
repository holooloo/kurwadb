# Sourced by the HTTP benchmarks.
#
# One ab is one thread, and it was the ceiling: against the same node, at the
# same 64 connections in total, one ab read 96 900 req/sec and four read
# 133 900. So the load is split across CLIENTS processes and their rates are
# summed. Past four the sum stops growing on a 10-core machine that is also
# running the server, which is where the measurement becomes the server's.
#
#     ab_parallel <requests> <concurrency> <ab args...>
CLIENTS=${CLIENTS:-4}

ab_parallel() {
  local requests=$1 concurrency=$2
  shift 2
  local out
  out=$(mktemp -d)
  local pids=()
  for i in $(seq 1 "$CLIENTS"); do
    ab -n $((requests / CLIENTS)) -c $((concurrency / CLIENTS)) -q "$@" >"$out/$i" 2>/dev/null &
    pids+=($!)
  done
  # Only the ab processes: a bare `wait` would also wait for the node under test.
  wait "${pids[@]}" || true
  local failed
  failed=$(cat "$out"/* | awk '/Failed requests/ {s += $3} END {print s + 0}')
  cat "$out"/* | awk -v failed="$failed" -v clients="$CLIENTS" \
    '/Requests per second/ {s += $4} END {printf "Requests per second:    %.0f [#/sec] (sum of %d ab)\nFailed requests:        %d\n", s, clients, failed}'
  rm -rf "$out"
}
