# Performance

## The rule

Speed is part of the contract, not a side effect of it.

**A release that is slower than the one before it is a broken release.** The
regression is the bug; it gets fixed before the release ships, rather than
recorded as the new baseline.

A promise like that is only worth making if it can be checked, so:

* every number here comes from a script in [`bench/`](bench), committed to the
  repository — no figure in the docs or on the site is hand-written;
* every version gets its own column, measured the same way on the same machine;
* [`docs/index.html`](docs/index.html) (the site) carries the current numbers,
  and is updated in the same commit as this file.

## Reproducing

```sh
KURWA_DATA_DIR=tmp/bench mix run bench/local.exs   # footprint, layer costs, single node
MIX_ENV=test mix run --no-start bench/cluster.exs  # three real nodes, quorum
bench/http.sh                                      # the HTTP gateway, via ApacheBench
```

Measured on Apple M4, 10 cores, Elixir 1.20.4 / OTP 29. All three cluster nodes
share that one machine, so the cluster figures are pessimistic: on separate
hosts they trade CPU contention for real network latency. Each figure is a
single run unless stated. Measured variance, so that "noise" is a number rather
than a shrug: `Store.put` moves ±10% between runs and `Kurwa.add` ±3%, while the
HTTP figures move ±7% - the 0.3.0 HTTP column is the midpoint of two runs. Treat
a change inside those bands as noise.

**Run them on a quiet machine, one at a time.** The HTTP benchmark reads about
30% low when it starts within a minute of the cluster benchmark, which is how
the 0.1.0 reconnect figure came to be wrong. Both scripts now warm up and
discard first, and `bench/http.sh` warns if another BEAM is already running.

## Storage footprint

A key carries four fixed metadata fields and no value, so the cost per key does
not grow with the cluster or with write history.

| | 0.1.0 | 0.2.0 | 0.3.0 | 0.4.0 |
|---|---|---|---|---|
| 13-byte key | 120 B | 128 B | 128 B | 128 B |
| 36-byte key | 144 B | 152 B | 152 B | 152 B |
| 100M keys, projected | 11.2 – 13.4 GB | 12.0 – 14.2 GB | 12.0 – 14.2 GB | 12.0 – 14.2 GB |

0.2.0 gave every record an expiry field, which is one machine word per key
whether or not the key uses it. That is the price of per-key TTL; it buys back
the memory of every key that now deletes itself instead of being swept by hand.

## Single node, in-process

| | 0.1.0 | 0.2.0 | 0.3.0 | 0.4.0 | 0.5.0 | 0.6.0 |
|---|---|---|---|---|---|---|
| `Clock.tick` | 26 ns | 26 ns | 26 ns | 26 ns | 26 ns | 26 ns |
| `Placement.targets` | 322 ns | 316 ns | 311 ns | 301 ns | 304 ns | 320 ns |
| `Store.get` (ETS only) | 382 ns | 361 ns | 366 ns | 369 ns | 349 ns | 364 ns |
| `Store.put` (shard + WAL) | 1.51 µs | 1.55 µs | 1.59 µs | 1.53 µs | 1.62 µs | 1.54 µs |
| `Quorum.run`, one target | 1.86 µs | 1.84 µs | 1.87 µs | 1.80 µs | 1.83 µs | 1.88 µs |
| `Kurwa.add` | 5.25 µs | 5.39 µs | 5.44 µs | 5.07 µs | 5.21 µs | 5.31 µs |
| `Kurwa.member?` | 3.15 µs | 3.08 µs | 3.12 µs | 2.94 µs | 2.87 µs | 3.04 µs |

A key with no expiry answers `member?` without reading the clock at all - the
`:never` case is a separate function head - so TTL costs the keys that do not
use it nothing. Every difference in this table is inside run-to-run variance.

## Three nodes, quorum (n=3 r=2 w=2)

| | 0.1.0 | 0.2.0 | 0.3.0 | 0.4.0 |
|---|---|---|---|---|
| `add`, single client | 46.6 µs | 45.7 µs | 45.3 µs | 41.5 µs |
| `member?`, single client | 46.8 µs | 46.4 µs | 47.3 µs | 45.7 µs |
| `add`, 64 clients | 54 300 ops/sec | 56 000 ops/sec | 55 200 ops/sec | 56 900 ops/sec |
| `member?`, 64 clients | 59 100 ops/sec | 58 400 ops/sec | 59 000 ops/sec | 60 900 ops/sec |
| local ETS read, 64 clients | 1 974 000 ops/sec | 1 962 000 ops/sec | 1 865 000 ops/sec | 1 959 000 ops/sec |

## HTTP gateway, one node

| | 0.1.0 | 0.2.0 | 0.3.0 | 0.4.0 |
|---|---|---|---|---|
| `GET /k/:key`, keep-alive, 64 conn | 113 000 req/sec | 126 000 req/sec | 123 000 req/sec | 119 000 req/sec |
| `GET /k/:key`, new connection each | ~~5 900~~ req/sec | 32 500 req/sec | 31 700 req/sec | 33 400 req/sec |
| `POST /batch`, 100 keys per request | 3 891 req/sec — 389 100 keys/sec | 4 084 req/sec — 408 400 keys/sec | 3 956 req/sec — 395 600 keys/sec | 3 967 req/sec — 396 700 keys/sec |

The struck-through figure is a bad measurement, not a slow release. It was taken
by hand before `bench/http.sh` existed, immediately after a 30 000-request run,
so it was measuring sockets stuck in TIME_WAIT rather than the gateway. Corrected,
connection reuse is worth about 4×, not 19× - still enough that a client which
reconnects per request is benchmarking TCP.

`POST /batch` is the pipelining equivalent and remains the right tool above a few
thousand keys per second.

## The native Bloom filter

0.7.0 moved one function into Rust: Bloom membership, which is the read path of
the on-disk engine. 100 000 keys, 7 probes over a 117 KB filter:

| | Elixir | Rust |
|---|---|---|
| key present | 2 728 ns | **45 ns** |
| key absent | 789 ns | **62 ns** |

Sixty times faster in the case that has to check every probe. What that bought
end to end is the more useful number, and it is smaller: an LSM `get` of an
absent key went from 380 ns to 280 ns, about 26%, because the filter was no
longer most of the cost once it stopped being slow. A present key barely moved,
at 11.8 µs to 11.0 µs - that path is a `pread` and a term decode per record in
the block, and neither is in Rust.

Which is the point of measuring rather than asserting: a 60× improvement to a
function is a 26% improvement to the operation, and the next thing worth
touching is now somewhere else.

`cargo` is optional. Without it the project compiles, the Elixir implementation
is used, and the suite passes - and a test asserts the two implementations agree
bit for bit, since a filter that disagrees with itself is a filter that says no
to a key it holds.

## What 0.6.0 changed

Durable hints. A hint is now written to the local store as it is taken, in the
*caller's* process rather than in the handoff process - a replica that is away
during heavy writing would otherwise make that one process the bottleneck for
every write that misses it. Nothing on the path of a write that reaches all its
replicas, and the column above is unchanged within variance.

## The two engines

0.5.0 added `Kurwa.Store.Lsm`, for when the keys stop fitting in memory. Both
engines answer the same contract - the whole test suite runs against either:

```sh
mix test                        # the ETS engine
KURWA_TEST_ENGINE=lsm mix test  # the on-disk one
```

200 000 keys with 13-byte names, everything flushed to disk
(`MIX_ENV=test mix run --no-start bench/engines.exs`):

| | ETS | LSM |
|---|---|---|
| resident, per key | 128.1 B | **3.01 B** |
| resident, total | 24.4 MB | 0.57 MB |
| on disk | — | 14.0 MB |
| `get`, key present | 1.14 µs | 11.0 µs |
| `get`, key absent | 1.14 µs | **0.28 µs** |

Two things to read out of that table. The same machine holds **43× more keys**,
because what stays in RAM is a Bloom filter and a sparse index rather than the
keys. And a miss is *faster* than the ETS engine, because the filter answers it
from memory without a seek - which is the common case for the jobs this store is
for, where the question is usually "no, not seen".

The `get` figures here are random keys over a 200k table, so they include cache
misses; the 349 ns in the table above is the same key read repeatedly, which is
the hot path, not the whole story.

## What 0.5.0 changed

The LSM engine, and nothing on the ETS path: the shard now passes two more
options that the ETS engine ignores. The column above is unchanged within
variance.

## What 0.4.0 changed

The set registry. System keys now route to a shard of their own, which adds one
prefix check per operation and one more shard to the supervision tree - both
free at this resolution, and the column above is unchanged or slightly better.
Registering a set name is a second quorum write, so it happens off the caller's
path and only once per set per node.

## What 0.3.0 changed

Active anti-entropy (`Kurwa.Repair`). It costs nothing on the request path by
construction: digests are computed by folding the ETS tables from a background
process, never maintained incrementally on write, and the tables are
`:protected` so reading them does not touch the shards. Every figure above is
unchanged within variance, which is the point.

A round is real work, though: it folds the whole local store once per peer. The
default interval is ten minutes and one peer per tick.

## Notes on comparing this to other stores

Redis's own benchmarking guide is blunt about it: *"It is absolutely pointless
to compare the result of redis-benchmark to the result of another benchmark
program and extrapolate."* The same applies in reverse to the table above.

Two differences matter more than the numbers:

* **Protocol.** The HTTP figures are HTTP/1.1 with JSON bodies against a binary
  protocol's figures. Most of any gap is framing, not storage — the store itself
  answers in 382 ns.
* **What is being bought.** The three-node figures include a quorum across three
  replicas with read repair. A single-node store of any kind is not doing that
  work and should be faster.
