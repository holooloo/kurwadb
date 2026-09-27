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

| | 0.1.0 | 0.2.0 | 0.3.0 |
|---|---|---|---|
| 13-byte key | 120 B | 128 B | 128 B |
| 36-byte key | 144 B | 152 B | 152 B |
| 100M keys, projected | 11.2 – 13.4 GB | 12.0 – 14.2 GB | 12.0 – 14.2 GB |

0.2.0 gave every record an expiry field, which is one machine word per key
whether or not the key uses it. That is the price of per-key TTL; it buys back
the memory of every key that now deletes itself instead of being swept by hand.

## Single node, in-process

| | 0.1.0 | 0.2.0 | 0.3.0 |
|---|---|---|---|
| `Clock.tick` | 26 ns | 26 ns | 26 ns |
| `Placement.targets` | 322 ns | 316 ns | 311 ns |
| `Store.get` (ETS only) | 382 ns | 361 ns | 366 ns |
| `Store.put` (shard + WAL) | 1.51 µs | 1.55 µs | 1.59 µs |
| `Quorum.run`, one target | 1.86 µs | 1.84 µs | 1.87 µs |
| `Kurwa.add` | 5.25 µs | 5.39 µs | 5.44 µs |
| `Kurwa.member?` | 3.15 µs | 3.08 µs | 3.12 µs |

A key with no expiry answers `member?` without reading the clock at all - the
`:never` case is a separate function head - so TTL costs the keys that do not
use it nothing. Every difference in this table is inside run-to-run variance.

## Three nodes, quorum (n=3 r=2 w=2)

| | 0.1.0 | 0.2.0 | 0.3.0 |
|---|---|---|---|
| `add`, single client | 46.6 µs | 45.7 µs | 45.3 µs |
| `member?`, single client | 46.8 µs | 46.4 µs | 47.3 µs |
| `add`, 64 clients | 54 300 ops/sec | 56 000 ops/sec | 55 200 ops/sec |
| `member?`, 64 clients | 59 100 ops/sec | 58 400 ops/sec | 59 000 ops/sec |
| local ETS read, 64 clients | 1 974 000 ops/sec | 1 962 000 ops/sec | 1 865 000 ops/sec |

## HTTP gateway, one node

| | 0.1.0 | 0.2.0 | 0.3.0 |
|---|---|---|---|
| `GET /k/:key`, keep-alive, 64 conn | 113 000 req/sec | 126 000 req/sec | 123 000 req/sec |
| `GET /k/:key`, new connection each | ~~5 900~~ req/sec | 32 500 req/sec | 31 700 req/sec |
| `POST /batch`, 100 keys per request | 3 891 req/sec — 389 100 keys/sec | 4 084 req/sec — 408 400 keys/sec | 3 956 req/sec — 395 600 keys/sec |

The struck-through figure is a bad measurement, not a slow release. It was taken
by hand before `bench/http.sh` existed, immediately after a 30 000-request run,
so it was measuring sockets stuck in TIME_WAIT rather than the gateway. Corrected,
connection reuse is worth about 4×, not 19× - still enough that a client which
reconnects per request is benchmarking TCP.

`POST /batch` is the pipelining equivalent and remains the right tool above a few
thousand keys per second.

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
