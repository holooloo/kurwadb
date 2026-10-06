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
bench/http.sh                                      # the HTTP gateway, via ApacheBench (CLIENTS=4 by default)
bench/lsm_http.sh                                  # both engines over HTTP, 1M keys, hits on disk
MIX_ENV=test mix run --no-start bench/quorum_parts.exs  # a quorum read, timed in parts
bench/client_ceiling.sh                            # is the load generator the ceiling?
bench/pg.sh                                        # the PostgreSQL frontend under pgbench
bench/mysql.sh                                     # the MySQL frontend under mysqlslap
REDIS_DIR=... bench/versus.sh                      # against Redis: HTTP, and RESP with RESP
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

| | 0.1.0 | 0.2.0 | 0.3.0 | 0.4.0 | 0.5.0 | 0.6.0 | 0.8.0 | 0.9.0 | 0.11.0 |
|---|---|---|---|---|---|---|---|---|---|
| `Clock.tick` | 26 ns | 26 ns | 26 ns | 26 ns | 26 ns | 26 ns | 33 ns | 31 ns | 37 ns |
| `Placement.targets` | 322 ns | 316 ns | 311 ns | 301 ns | 304 ns | 320 ns | 292 ns | 320 ns | 329 ns |
| `Store.get` (ETS only) | 382 ns | 361 ns | 366 ns | 369 ns | 349 ns | 364 ns | 339 ns | 361 ns | 374 ns |
| `Store.put` (shard + WAL) | 1.51 µs | 1.55 µs | 1.59 µs | 1.53 µs | 1.62 µs | 1.54 µs | 1.60 µs | 1.59 µs | 1.74 µs |
| `Quorum.run`, one target | 1.86 µs | 1.84 µs | 1.87 µs | 1.80 µs | 1.83 µs | 1.88 µs | 1.78 µs | 1.90 µs | 1.93 µs |
| `Kurwa.add` | 5.25 µs | 5.39 µs | 5.44 µs | 5.07 µs | 5.21 µs | 5.31 µs | 5.08 µs | **4.52 µs** | **3.28 µs** |
| `Kurwa.member?` | 3.15 µs | 3.08 µs | 3.12 µs | 2.94 µs | 2.87 µs | 3.04 µs | 2.86 µs | **2.23 µs** | **1.24 µs** |

A key with no expiry answers `member?` without reading the clock at all - the
`:never` case is a separate function head - so TTL costs the keys that do not
use it nothing. Every difference in this table up to 0.8.0 is inside run-to-run
variance. 0.9.0 is not: `Kurwa.add` and `Kurwa.member?` now go through
`Quorum.request`, which starts one process instead of two. The `Quorum.run` row
is the old function, still used for `count` and the set registry. Mean of two
runs.

0.11.0 again, and for a smaller reason: when the only replica is the node
itself - one node, or `n=1` - `Quorum.request` answers inline instead of
starting a collector to wait for nobody. In a cluster nothing changes, because
there is always a remote replica to wait for. `Store.put` reads 9% higher, and
is inside its ±10%: nothing on that path changed.

## Three nodes, quorum (n=3 r=2 w=2)

| | 0.1.0 | 0.2.0 | 0.3.0 | 0.4.0 | 0.4.0, re-run | 0.8.0 | 0.9.0 | 0.11.0 |
|---|---|---|---|---|---|---|---|---|
| `add`, single client | 46.6 µs | 45.7 µs | 45.3 µs | 41.5 µs | — | 48.4 µs | **38.0 µs** | 39.6 µs |
| `member?`, single client | 46.8 µs | 46.4 µs | 47.3 µs | 45.7 µs | — | 50.9 µs | **40.8 µs** | 40.6 µs |
| `add`, 64 clients | 54 300 ops/sec | 56 000 ops/sec | 55 200 ops/sec | 56 900 ops/sec | 46 500 ops/sec | 46 200 ops/sec | **64 600 ops/sec** | 63 800 ops/sec |
| `member?`, 64 clients | 59 100 ops/sec | 58 400 ops/sec | 59 000 ops/sec | 60 900 ops/sec | 44 300 ops/sec | 48 000 ops/sec | **68 600 ops/sec** | 67 700 ops/sec |
| local ETS read, 64 clients | 1 974 000 ops/sec | 1 962 000 ops/sec | 1 865 000 ops/sec | 1 959 000 ops/sec | 1 710 000 ops/sec | 1 859 000 ops/sec | 1 645 000 ops/sec | 1 543 000 ops/sec |

**Read the last two columns together, not against the ones before them.** The
cluster and HTTP benchmarks were not re-run for 0.5.0 – 0.7.0, which broke this
file's own rule, and when they were run again on 2026-10-05 every concurrent
figure came out 10–20% below the 0.4.0 column. Before calling that a regression,
0.4.0, 0.5.0, 0.6.0 and 0.7.0 were each checked out and measured the same day:
all of them landed at 44 000 – 51 000 ops/sec, 0.4.0 included. The code did not
get slower; the machine did, for multi-core work (the OS was updated in between,
and three nodes on one CPU feel that first). Single-threaded figures reproduce to
within a few percent, so the table above is unaffected.

0.8.0 is the median of three runs; on the same day the same three runs spread
from 43 400 to 49 600 for `add`, wider than the ±7% quoted above. The re-run
0.4.0 column is a single run. 0.9.0 is the median of three, measured on
2026-10-06 on the same machine - see [What 0.9.0 changed](#what-090-changed).
0.11.0 is the median of three the same day; every row is within 4% of 0.9.0,
and the local ETS read - which no change since 0.8 touches - is the one that
moved most, 6%, which is a fair measure of the noise.

## HTTP gateway, one node

| | 0.1.0 | 0.2.0 | 0.3.0 | 0.4.0 | 0.4.0, re-run | 0.8.0 | 0.9.0, four clients |
|---|---|---|---|---|---|---|---|
| `GET /k/:key`, keep-alive, 64 conn | 113 000 req/sec | 126 000 req/sec | 123 000 req/sec | 119 000 req/sec | 103 000 req/sec | 106 000 req/sec | **130 700 req/sec** |
| `GET /k/:key`, new connection each | ~~5 900~~ req/sec | 32 500 req/sec | 31 700 req/sec | 33 400 req/sec | 28 200 req/sec | 30 000 req/sec | 28 500 req/sec (one client) |
| `POST /batch`, 100 keys per request | 3 891 req/sec — 389 100 keys/sec | 4 084 req/sec — 408 400 keys/sec | 3 956 req/sec — 395 600 keys/sec | 3 967 req/sec — 396 700 keys/sec | 3 782 req/sec — 378 200 keys/sec | 3 962 req/sec — 396 200 keys/sec | **4 460 req/sec — 446 000 keys/sec** |

Same story as the cluster: 0.4.0 measured on 2026-10-05 is slower than 0.4.0
measured in September, and 0.8.0 is at or above it on every row. Single runs.

**The last column is measured differently, and most of its gain is that.** Up to
0.8.0 a single `ab` produced the load, and `ab` is one thread. Split across
processes at the same 64 connections in total, against the same 0.8.1 node:

| `ab` processes | req/sec |
|---|---|
| 1 | 96 900 |
| 2 | 119 200 |
| 4 | **133 900** |
| 8 | 131 300 |

So every keep-alive figure before 0.9.0 was the client's ceiling, not the
server's. `bench/http.sh` now runs four (`CLIENTS`, in `bench/ab_parallel.sh`)
and sums them; past four the sum stops growing because the clients and the
server share ten cores. The new-connection row stays on one client on purpose:
it measures TCP setup in the kernel, and more clients only contend for that
(four read 25 000). 0.9.0 is the median of three runs for keep-alive, two for
the others.

The struck-through figure is a bad measurement, not a slow release. It was taken
by hand before `bench/http.sh` existed, immediately after a 30 000-request run,
so it was measuring sockets stuck in TIME_WAIT rather than the gateway. Corrected,
connection reuse is worth about 4×, not 19× - still enough that a client which
reconnects per request is benchmarking TCP.

`POST /batch` is the pipelining equivalent and remains the right tool above a few
thousand keys per second.

## The PostgreSQL frontend

`bench/pg.sh`: one node, 100 000 keys, `SELECT key FROM kurwa WHERE key = ...`
with a random key that is present (hit) or not (miss), 64 clients over four
pgbench threads, ten seconds per run, two runs, 0.10.0:

| pgbench mode | hit | miss |
|---|---|---|
| `simple` - one Query message | 136 500 – 146 600 tps | 135 400 – 143 500 tps |
| `extended` - Parse, Bind, Describe, Execute, Sync each time | 126 800 – 133 700 tps | 125 300 – 131 900 tps |
| `prepared` - Bind, Execute, Sync | **135 700 – 142 600 tps** | **138 600 – 142 600 tps** |

The first version answered prepared statements at 70 000 and the extended mode
at 62 000, half of the simple protocol. Nothing in the SQL was slow: the server
wrote each reply message with its own syscall, so a prepared statement's three
replies were three writes. Collecting the replies and writing them once the
input in hand is handled - which is when PostgreSQL itself flushes - took both
to the simple protocol's rate.

An earlier version of this section noted that these were within a few percent
of Redis over RESP and suggested the gap Redis held over HTTP was HTTP and
JSON. The RESP frontend measured that properly, one client against both
servers, and the suggestion was wrong: see the next section.

## The MySQL frontend

`bench/mysql.sh`: one node, 100 000 keys, 64 clients in mysqlslap's threads,
640 000 `SELECT \`key\` FROM kurwa WHERE \`key\` = ...` per run, text protocol,
two runs, 0.13.0:

| | queries/sec |
|---|---|
| key present | 128 400 – 133 200 |
| key absent | 127 000 – 130 500 |

The same as the other three protocols, within their noise: HTTP 124-132k,
RESP 125-129k, PostgreSQL 136-147k. Which is the result of the RESP section
again from another side - none of these wire formats is what a request costs.

## SET NX: one winner, two round trips

`Kurwa.add_new/2` - `SET NX`, `INSERT ... ON CONFLICT DO NOTHING` - asks the
first replica in a key's preference list alone, then the rest (see
ARCHITECTURE.md, "One winner without a leader"). `bench/cluster.exs`, three
nodes, 0.12.0, two runs:

| | `add` | `add_new` |
|---|---|---|
| single client | 40.5 – 41.3 µs | 47.1 – 47.8 µs |
| 64 clients | 61 900 – 66 400 ops/sec | 58 200 – 59 900 ops/sec |

The second round trip costs less than a whole one because a third of the time
the coordinator is itself the first replica and phase one is local.

## The Redis frontend, against Redis

0.11.0 answers RESP, so `redis-benchmark` can drive both servers the same way:
the same client binary, the same command, four client threads, 64 connections,
one node each. `bench/versus.sh`, two runs on 2026-10-06:

| `SISMEMBER` | Redis 8.0.3 | kurwadb, RESP | kurwadb, HTTP |
|---|---|---|---|
| one request at a time | 142 800 – 159 900 /sec | 124 900 – 128 800 /sec | 123 600 – 131 800 /sec |
| pipelined, 16 per round trip | **1 640 000 – 1 830 000 /sec** | 735 000 – 820 000 /sec | — |

Three things in that table.

**The protocol was not the gap.** kurwadb answers RESP at the same rate it
answers HTTP with a JSON body, and pgbench saw the same again. What Redis is
ahead by, 15-20% one request at a time, is the cost of a request inside the
server - a BEAM process per connection, a placement lookup, a quorum call even
when the quorum is one - not the bytes on the wire.

**Pipelined, Redis is 2.2 times faster.** A pipeline is where an event loop
over an in-memory dictionary is at its best: sixteen lookups and one write,
all on one thread. kurwadb runs the sixteen in order through the same path as
one, at about 1 µs each. That is the price of the path being the same one a
replicated read takes, and the honest summary is that Redis is the faster
single server by a clear margin.

**Latency is level.** p50 is 0.30 ms for kurwadb and 0.28-0.32 ms for Redis
one request at a time.

## What 0.9.0 changed

A quorum read took 52 µs on three nodes. Timed in parts, from inside a node of a
real three-node cluster (`n=3 r=2`, one client):

| | |
|---|---|
| `Kurwa.fetch`, whole | **52.5 µs** |
| a bare send and receive to a process on another node | **29.9 µs** |
| `:erpc.call` to another node, doing nothing | 40.3 µs |
| `:erpc.call` to another node, `Replica.get` | 43.3 µs |
| `Quorum.run`, three local targets doing nothing | 4.4 µs |
| `Placement.targets` / local `Store.get` | 2.1 / 1.7 µs |

Thirty of the fifty-two are one distribution round trip: two trips through the
kernel's TCP stack on loopback. Nothing in this repository can shorten that, and
on separate hosts it becomes the network instead. Of the rest, 10-13 µs was
`:erpc`, which spawns a process on the remote node for every call, and 4-5 µs
was the quorum, which started a collector and then a worker per replica.

So the hot path no longer uses either. `Kurwa.Replica.Endpoint` is sixteen
long-lived processes per node, registered by name; `Quorum.request/4` sends a
read or a write straight to the endpoint on each remote node, runs the local one
inline, and starts only the collector - sending is asynchronous, and the workers
existed only to wait. Late replies still land in the collector and die with it.

The first version was **slower** than what it replaced, 56 µs and 36 000 reads a
second: it put a process monitor on each remote endpoint, and monitoring a remote
name is a signal over the wire to set up and another to take down, on top of the
request and the reply. Without them it was 40 µs. What the monitors were for -
answering at once when a node drops mid-request, rather than at the deadline -
now comes from `:erlang.monitor_node/2`, which is bookkeeping in the local
distribution layer and sends nothing. It costs about 1 µs. A cluster test kills
two replicas and asserts a request needing them returns in under three seconds
against a ten-second timeout; with `monitor_node` removed, it fails.

| 3 nodes, `n=3 r=2 w=2` | 0.8.1 | 0.9.0 |
|---|---|---|
| `add`, single client | 48.4 µs | **38.0 µs** |
| `member?`, single client | 50.9 µs | **40.8 µs** |
| `add`, 64 clients | 46 200 ops/sec | **64 600 ops/sec** (+40%) |
| `member?`, 64 clients | 48 000 ops/sec | **68 600 ops/sec** (+43%) |

What is left above the round trip is about 10 µs, and most of it is the
remote endpoint and the collector waking up from sleep.

### A write that said ok and did nothing

The faster quorum made a flaky cluster test fail more often, and the test was
right. A write is stamped by its coordinator's Lamport clock. If that clock is
behind a version some replica already holds - a delete another node just
repaired onto it, say - the write loses the merge on every replica, and every
replica still answers `{:ok, winner}`. The coordinator counted those as
acknowledgements and told the client `:ok`. The key stayed deleted.

The coordinator now compares each winner with the record it sent. If any
replica kept something else, the clock is raised past it and the write goes
again, once - the stamp it would have had if this node had heard of that version
first. It costs nothing on the common path, where every winner is the record
just sent. A deterministic cluster test sets up exactly this; it failed before
the change, and the flaky one has passed twenty runs in a row since, on both
engines.

## Where an on-disk read actually goes

0.8.0 is the result of profiling the parts instead of the whole, and it is the
most useful thing in this file.

After the Bloom filter went native, an on-disk read of a key that *is* present
still cost 11 µs. Timing the pieces of that read:

| | |
|---|---|
| `:file.pread` of the block | 7.2 µs — **83%** |
| everything else (decode, index search, calls) | 1.4 µs — 16% |
| the Bloom filter | 0.05 µs — **1%** |

The filter that had just been made sixty times faster was one percent of the
read. What cost everything was the file handle: it was opened without `:raw`,
so that any process could use it, and a non-raw handle is a message round trip
to the process that owns the file. Same 1 KB read, measured both ways: **6.41 µs
shared, 1.46 µs raw.**

So a table now holds no handle at all - it is plain data, which is also what you
want in `:persistent_term` - and each reading process opens its own raw handle
on first use and keeps it (`Kurwa.Store.SSTable.Fd`, capped, so a compacted-away
table cannot pin inodes forever).

The other change is the frame format: the key moved out of the `term_to_binary`
payload into the frame header, so scanning a block for a key compares bytes and
decodes exactly one record instead of every record on the way past. Worth 9% and
a slightly smaller file, since the key is no longer stored twice.

| on-disk `get`, key present | |
|---|---|
| 0.6.0 | 11.8 µs |
| 0.7.0, Bloom in Rust | 11.0 µs |
| 0.8.0, key out of the payload | 10.0 µs |
| 0.8.0, raw file handles | ~~3.6 µs~~ — **28.7 µs** as a request saw it |
| 0.8.1, a pool of readers that own the handles | **3.4 µs** |

0.8.0 was meant to be three times faster and was, in the benchmark. On the path
a request actually takes it was **two and a half times slower** than 0.7.0, which
makes it a broken release by the rule at the top of this file.

## What 0.8.1 fixed

A raw handle belongs to the process that opened it, and 0.8.0 cached one per
process. `bench/engines.exs` reads from a single process, so it opened each file
once and measured 3.6 µs. A real read never runs in a process that lives:
`Kurwa.Quorum` gives every replica call a fresh worker, and a call from another
node arrives through `:erpc`, which spawns one too. So every read opened the
file, read one block, and closed it as the worker died:

| on-disk `get`, key present, 0.8.0 | |
|---|---|
| from a process that already holds the handle | 2.55 µs |
| from a fresh process - the real path | **28.7 µs** |
| of which the spawn itself | 0.7 µs |

Nothing caught it because nothing measured the real path. `bench/versus.sh`
writes one key into the default engine, and the HTTP benchmark never touched the
on-disk engine at all. `bench/lsm_http.sh` now does: a million keys into each
engine through `POST /batch`, eight tables flushed (the script checks the log
rather than assuming it), then a key from the first batch and a key that was
never written:

| `GET /k/:key`, 1M keys, 64 clients | ETS | LSM, 0.8.0 | LSM, 0.8.1 |
|---|---|---|---|
| key present (on disk for LSM) | 96 500 – 104 400 req/sec | 47 800 req/sec | **69 700 – 71 500 req/sec** |
| key absent | 96 600 – 102 000 req/sec | 81 500 req/sec | **101 400 – 105 500 req/sec** |

The same script on 0.9.0 with four clients instead of one, two runs - so these
are the server's figures rather than `ab`'s:

| `GET /k/:key`, 1M keys, 64 clients, 0.9.0 | ETS | LSM |
|---|---|---|
| key present (on disk for LSM) | 124 300 – 127 700 req/sec | **92 900 – 93 000 req/sec** |
| key absent | 120 400 – 124 200 req/sec | **118 300 – 120 200 req/sec** |

A hit from disk is 75% of the in-memory engine's rate; a miss is level.

These replace figures 2-3% lower that this table first carried for 0.9.0. Those
were measured with the Elixir Bloom filter, not the Rust one: `cargo` was not on
the path when the project recompiled, and `Kurwa.Native` falls back silently by
design - which is right for a build and wrong for a benchmark. The four
`BloomTest` cases that compare the two paths failed and said so; the benchmark
did not. Check `Kurwa.Native.available?()` before measuring the on-disk engine.

The fix: `Kurwa.Store.SSTable.Readers`, one long-lived process per scheduler that
owns the raw handles. A read sends the path and the range to the reader for the
caller's scheduler and gets the block back - a reference-counted binary, so no
copy. One message each way instead of an `open` and a `close`. The handle cap
went from 16 to 256 so a reader can hold every table of every shard, and a table
removed by compaction is released in every reader.

`bench/engines.exs` now starts the pool and also reports the read from a fresh
process, so the number in this file is the one a request pays. The ~30% left
between the engines on a hit is the `pread` itself; on a miss there is nothing
left - the filter answers from memory and the two engines are level.

The dataset is ~70 MB on disk and sits in the OS page cache, so a "present" read
here is a syscall answered from memory, not an SSD seek. That is the steady state
for a set that fits in page cache and the best case for one that does not.

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
| on disk | — | 13.5 MB |
| `get`, key present, from a fresh process | 1.14 µs | **3.4 µs** |
| `get`, key absent | 1.14 µs | **0.20 µs** |

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

## Against Redis, measured here

Redis's own guide is blunt that comparing one benchmark tool's output to
another's and extrapolating is pointless. So this is not that: both servers were
built and run on this machine, one at a time, asked the same question by a
client with the same concurrency. `REDIS_DIR=... bench/versus.sh` reproduces it.

Redis 8.0.3, one million 13-byte members in a `SET`. Re-measured 2026-10-05, three
runs of each server, medians:

| | bytes per key |
|---|---|
| redis `SET`, hashtable encoding | **30.2 B** |
| kurwadb, ETS engine | 128.1 B |
| kurwadb, LSM engine | **3.0 B** |
| a Bloom filter on its own | 1.2 B (false positives, no deletes) |

Membership over the wire, one node, no replication on either side, 64 clients:

| | 2026-09-30 | 2026-10-05 | 2026-10-06, four client threads |
|---|---|---|---|
| redis `SISMEMBER` (RESP) | 142 600 req/sec, p50 0.24 ms | 102 400 req/sec, p50 0.33 ms | **142 800 req/sec**, p50 0.31 ms |
| kurwadb `GET /k/:key` (HTTP/1.1 + JSON) | 115 000 – 123 000 req/sec | 103 300 req/sec | **118 900 req/sec** |
| kurwadb, same question across a 3-node quorum | 59 000 /sec | 48 000 /sec | 68 600 /sec |

The same Redis binary lost 28% between the first two dates, which is the clearest
evidence that the drop in the cluster and HTTP tables above is the machine: Redis
did not change. On the second date the two servers looked tied, within 2% on
every run - **and that tie was the clients, not the servers.** `redis-benchmark`
is single-threaded by default exactly as `ab` is, and both had hit their own
ceiling. On the third date each side gets four client threads
(`redis-benchmark --threads 4`, four `ab` summed) and a million requests rather
than 200 000 - at 200 000 a Redis run lasts about a second and its rate comes
out quantised (160 000, 133 333). Medians of three: **Redis is about 20%
faster.** The range was 137 900 – 153 800 for Redis and 117 600 – 121 300 for
kurwadb.

`bench/versus.sh` measures kurwadb with one key in the default engine, so its
throughput line is the in-memory engine only. The site used to show the same
figure against the on-disk engine; that was never measured, and is now marked so.

### Reading that honestly

**Redis is four times more memory-efficient than our in-memory engine**, and an
earlier version of these docs guessed they were in the same league. They are
not. Some of the gap is what we store: a kurwadb record carries a Lamport stamp,
the node that issued it, a tombstone flag, a wall clock and an expiry - about 40
bytes of logical content against Redis's 13 - because those five fields are what
make it replicate and expire. The rest is BEAM term overhead, and Rust would
recover maybe half of it.

**The LSM engine uses ten times less memory than Redis**, which is the more
interesting direction and is not a language question at all: what is in RAM is a
Bloom filter, not the keys.

**Redis is about 20% faster** on throughput, measured with clients that are not
the bottleneck. This paragraph used to call that the cost of HTTP and JSON and
say a binary protocol would close it; with RESP built, kurwadb answers RESP no
faster than HTTP, so it is the cost of a request inside the server. See
"The Redis frontend, against Redis" above. An earlier version still called it a
tie; it was a tie between two clients.

Two caveats on the Redis side: it was built here without jemalloc (`malloc=libc`),
which usually costs it a little memory, and a single Redis node is not doing the
replication the three-node row is.
