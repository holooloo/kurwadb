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
single run; expect a few percent of run-to-run variance, and treat a change
smaller than that as noise rather than as a regression.

## Storage footprint

A key carries four fixed metadata fields and no value, so the cost per key does
not grow with the cluster or with write history.

| | 0.1.0 |
|---|---|
| 13-byte key | 120 B |
| 36-byte key | 144 B |
| 100M keys, projected | 11.2 – 13.4 GB |

## Single node, in-process

| | 0.1.0 |
|---|---|
| `Clock.tick` | 26 ns |
| `Placement.targets` | 322 ns |
| `Store.get` (ETS only) | 382 ns |
| `Store.put` (shard + WAL) | 1.51 µs |
| `Quorum.run`, one target | 1.86 µs |
| `Kurwa.add` | 5.25 µs |
| `Kurwa.member?` | 3.15 µs |

## Three nodes, quorum (n=3 r=2 w=2)

| | 0.1.0 |
|---|---|
| `add`, single client | 46.6 µs |
| `member?`, single client | 46.8 µs |
| `add`, 64 clients | 54 300 ops/sec |
| `member?`, 64 clients | 59 100 ops/sec |
| local ETS read, 64 clients | 1 974 000 ops/sec |

## HTTP gateway, one node

| | 0.1.0 |
|---|---|
| `GET /k/:key`, keep-alive, 64 conn | 113 000 req/sec |
| `GET /k/:key`, new connection each | 5 900 req/sec |
| `POST /batch`, 100 keys per request | 3 891 req/sec — 389 100 keys/sec |

Connection reuse is worth 19× here, so a client that opens a connection per
request is measuring TCP, not kurwadb. `POST /batch` is the equivalent of
pipelining and is the right tool above a few thousand keys per second.

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
