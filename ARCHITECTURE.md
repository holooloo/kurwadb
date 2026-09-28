# Architecture

kurwadb is a distributed set: it stores keys and no values. This document is the
reasoning behind that, what it makes easy, what it makes impossible, and what is
deliberately still missing.

## Shape

```
  clients                HTTP            9P2000
                           |               |
  frontends          Kurwa.Gateway   Kurwa.NineP        thin adapters, no logic
                           \             /
  api                  Kurwa / Kurwa.Namespace          keys and named sets
                                 |
  read path          Kurwa.Extractor                    cache + single-flight
                                 |
  replication        Kurwa.Coordinator                  quorum, read repair
                       /         |        \
              Placement       Quorum      Handoff
                                 |
  node-local           Kurwa.Store                      shards
                                 |
  storage            Kurwa.Store.Engine                 ETS + WAL today
```

Nothing above `Coordinator` knows about replication, and nothing below it knows
about protocols. That is what makes a new frontend cheap - 9P was added without
touching the core - and what will let a disk engine be swapped in without
touching the cluster.

## The data model

Everything kurwadb stores is this:

```elixir
{key, lamport, node, alive?, wall, expires_at}
```

`alive?` is membership; `false` is a tombstone. `wall` is used only to age
tombstones out and never to order anything. `expires_at` is an absolute instant
or `:never`. Merge is last-writer-wins on
`{lamport, node}`:

```elixir
def merge(a, b), do: if newer?(a, b), do: a, else: b
```

That is a total order, so the merge is commutative, associative and idempotent -
a convergent LWW-Set. Two replicas that have seen the same writes agree, in any
order, with no coordination. A coordinator stamps a write once and sends the same
record to every replica, so replicas hold byte-identical data rather than each
inventing a version.

**What no values buys.** In a key-value store, two concurrent writes to one key
are two different values and somebody has to choose: vector clocks and siblings
(Riak), a CRDT per value type, or a silent LWW that loses data. With no values
there is nothing to lose - the only question is whether the key is in the set, and
`{lamport, node}` answers it.

**What LWW costs.** A delete concurrent with an add can win. For dedup and
idempotency that is the right shape (`add` then `add` is the same as `add`), but
kurwadb is not an add-wins OR-Set, and a workload that deletes and re-adds the
same key from several coordinators at once will see whichever stamp is higher.

**Clock skew cannot lose a write.** Ordering is Lamport, not wall-clock. The
clock lives in `:atomics`, is raised by every record we accept from a peer and by
every record replayed from the WAL, so a restarted node never reissues a stamp.

**Expiry.** A key can be given a TTL, and the coordinator turns it into an
absolute deadline once, so every replica stores the same instant rather than
each starting its own countdown when the write arrives. Expiry is then a *pure
function of the record*: every replica reaches the same verdict at the same
moment with nothing exchanged, which is why an expired key needs no tombstone to
stay gone. Two consequences worth stating. It is the one place wall-clock time
decides an answer, so unlike ordering it is exposed to clock skew. And `:never`
is the sentinel rather than `nil` or `0` because in Erlang term order every
number sorts before every atom, so `expires_at < now` is already false without a
special case - in the guards and in the sweeper's match specs alike.

**Tombstones.** A delete writes a tombstone, because a replica that never heard
about the delete would otherwise resurrect the key on the next merge. Tombstones
are dropped after `tombstone_ttl`. This is a real tradeoff, not a detail: the TTL
must exceed the longest time a replica can be away, or that replica can come back
holding a live key whose tombstone has been collected, and resurrect it. Expired
keys are swept on the same grace period and for the same reason.

## Placement

Consistent hashing, `vnodes` (128) ring points per node, `preflist/3` is a binary
search over a tuple - O(log P), no process hop.

The ring is built from every node the cluster has ever **verified**, not from the
nodes that are reachable right now. `Kurwa.Placement` then splits a key's
replicas into `up` and `down`.

This split is load-bearing. A ring built from reachable nodes only would move
ownership every time a node blinks - keys shift to new replicas on the way down
and shift back on the way up, and nobody is responsible for the writes the absent
node missed. With a stable ring, a downed node keeps its share of the keyspace
and its missed writes become a hint list. A node leaves the ring only when an
operator says so (`Kurwa.Cluster.forget/1`).

Membership comes from Erlang distribution: `:net_kernel.monitor_nodes` plus a
`ping` check, so unrelated BEAM nodes cannot join the ring. No gossip protocol,
because distribution already gives a full mesh and up/down events. The ring lives
in `:persistent_term`, which is exactly the right structure for something read on
every request and written on membership change.

## Quorum

Any node coordinates any key; there is no leader. `Kurwa.Quorum` fans a request
out to the replicas and returns as soon as `need` of them answer.

The fan-out runs in a short-lived collector process rather than in the caller.
Once the quorum is reached we walk away from the slow replicas, and their late
replies must not accumulate in the mailbox of a long-lived process such as a
gateway connection - they land in the collector's mailbox and die with it. There
is a test for exactly that.

`r + w > n` gives read-your-writes per key. Weaker settings are allowed and
warned about. When fewer than `w` replicas are reachable, the default is to lower
the quorum to what exists and keep serving; `strict_quorum: true` fails the
request instead. Both are defensible, so it is a setting and not a silent choice -
and either way a failed quorum is an error, never a `false`.

## Repair

Three mechanisms, covering different holes:

**Read repair** pushes the merged winner to any replica that answered with
something older. It costs nothing extra - the answers are already in hand - but it
only reaches replicas that answered inside the quorum window, and a key nobody
reads is never repaired.

**Hinted handoff** covers the rest. A write that succeeded but could not reach one
of its replicas leaves that record in a per-node queue, merged by the same LWW
rule so a key written a hundred times replays once. The queue drains on a timer
and is kicked the moment the node rejoins.

Hints live in memory on the coordinating node and the queue is bounded; if the
coordinator restarts first, or a replica is away long enough to overflow the
queue, the third mechanism is the remaining path.

**Active anti-entropy** covers what neither of those can see. Both of them are
reactive - handoff needs a coordinator that noticed a failure, read repair needs
somebody to read the key - so a replica that *acknowledged* a write and then lost
it (a crash before the fsync, a restore from an old snapshot) is wrong with
nobody aware of it. `Kurwa.Repair` goes looking: it folds the local store into a
digest per key-hash bucket, restricted to the keys it and one peer are both
supposed to hold, compares vectors with that peer, and swaps the contents of any
bucket where they disagree.

The usual structure here is a Merkle tree, so that replicas holding millions of
keys exchange kilobytes instead of everything. With no values the tree is not
worth building: a leaf digest is `phash2` over a handful of fixed-size fields,
and the entire vector of 4096 bucket digests is 32 KB - one message, one round
trip, no descent. The comparison a tree exists to avoid is cheaper than the tree.

Digests are folded from ETS in a background process and never maintained on the
write path, so the feature costs a write nothing; the tables are `:protected`, so
reading them does not touch the shards either. A round is real work - one full
fold per peer - so it runs every ten minutes against one peer at a time, and the
number of buckets repaired per round is capped.

## Storage

`Kurwa.Store.Engine` is a behaviour with a deliberate split: write callbacks take
engine state and only ever run in the owning shard process, while read callbacks
take a cheap process-independent handle. That is why a local membership check is
one `:ets.lookup` in the caller and not a `GenServer.call`.

`Kurwa.Store.Ets` is the engine today: an ETS set per shard plus an append-only
log.

```
snapshot   every live record at the last compaction
wal        every record accepted since
```

Both use the same framing: a magic header, then `len[4] crc32[4] payload` per
entry. Recovery reads the snapshot then the log. A tail cut short by a crash fails
its length or CRC check and ends the replay, leaving everything before it intact -
there are tests for a torn tail, a bad CRC and an absurd length. Compaction
fsyncs the new snapshot and renames it into place *before* the old log is
dropped, so a crash at any point leaves one consistent pair.

Durability is deliberately a window, not a guarantee, at the node level. An
accepted write is appended to the log at once, but the fsync is periodic
(`wal_sync_interval`, 100ms), so a node killed hard comes back missing writes it
had acknowledged. That is the same bargain Redis makes with `appendfsync
everysec`, and the reason it is acceptable here is replication: the other
replicas have the write. `wal_sync_on_write: true` trades an fsync per write for
closing the window locally. Both behaviours have a cluster test.

Sharding here is only about local write concurrency (`:erlang.phash2`); which
*node* holds a key is the ring's business. Live keys are counted in an
`:atomics` counter rather than `:ets.info(:size)`, because the table also holds
tombstones and `count` must not see them.

## Failure behaviour, as measured

Three nodes on one machine, `n=3 r=2 w=2`, 20 keys written.

| | |
|---|---|
| all three up | 20 keys, 20 on each node, `count` reports 20 |
| one node killed | ring keeps 3 members, 2 up; reads and writes continue |
| 10 keys written while it was down | accepted at `w=2`; 10 hints queued for the absent node |
| node restarted | WAL replay gives it the 20 it had; handoff delivers the 10 it missed |
| after ~5s, no reads issued | 30 on each of the three nodes, handoff queue empty |

The first version of this failed: read repair alone healed 8 of 10 keys, because
the third replica usually does not answer inside a quorum of 2, and because a
downed node had been dropped from the preference list entirely so nothing was
even accumulating for it. Stable placement plus handoff is the fix, and the
measurement above is the same scenario re-run.

This is now an automated suite (`mix test --include cluster`) rather than a
manual exercise, and writing it turned up the second hole. A node stopped by
killing its BEAM came back with *nothing*, not with what it had: the WAL's fsync
had not fired yet. Which means the two ways a replica falls behind need different
mechanisms, and only one of them was covered:

| how a replica falls behind | who knows | what fixes it |
|---|---|---|
| it was unreachable when the write happened | the coordinator | hinted handoff |
| it acknowledged the write, then crashed before fsync | **nobody** | anti-entropy |

The second row is why `wal_sync_on_write` exists, and it is what active
anti-entropy was built for. There is a cluster test for exactly that scenario:
kill a node hard so it loses writes it had already acknowledged, restart it, read
nothing, and let one repair round put the keys back.

## Plan 9

The 9P frontend is not a novelty. In Plan 9 the data *is* a name in a namespace,
and a set with no values *is* a directory of empty files, so the mapping is exact:
`stat` is `member?`, `create` is `add`, `remove` is `delete`. Control files
replace a pile of admin endpoints, and `ls`/`cat`/`stat` replace a client library.

Named sets come from the same place. A union mount (`bind -a`) makes one name
resolve through several directories in order, which for a key-only store is
exactly set union - so `Kurwa.Namespace.member_any?/3` is a union mount, composed
at read time, copying nothing. Unions short-circuit: the first `true` settles a
union, the first `false` an intersection.

Where Plan 9 does not fit, stated plainly:

* **9P is not an internal protocol.** It is stateful and walk-oriented, which is
  the wrong shape for fanning a write out to replicas, and it says nothing about
  quorums. Nodes replicate over Erlang distribution. 9P is a client frontend.
* **File names are not arbitrary binary keys** - no `/`, no NUL, UTF-8 expected.
  Hence `/b64`, which is honest rather than elegant.
* **A directory listing is a scan**, and kurwadb has no scans. `/keys`, `/b64`
  and `/sets/<set>` return an error saying so, rather than an empty listing that
  would be a lie. `/sets` is the exception, and only because of the registry
  below.

## The set registry

"Which sets exist" is enumeration, and enumeration is the thing this store
refuses to do. The way out is not to add an index but to notice that the answer
is itself a set: **a set name is a key, in a set**. Registry entries live in the
reserved `_sets` namespace and replicate, merge, hand off and repair through
exactly the same path as everything else, with no new machinery at all.

The reservation is airtight rather than conventional. `Kurwa.Key.valid_name?/1`
requires a namespace to start with a letter or digit, so `_sets` is unspellable
through `encode/2` and nothing a caller writes can collide with it.

What makes it cheap to read is one local routing decision: `Kurwa.Store` sends
system keys to a shard of their own, so listing folds a table with one record
per set rather than a table with every key ever written. That is the whole
implementation - no index to keep in step, nothing to rebuild at boot.

Two limits, both stated in the moduledoc rather than discovered later. A name is
registered on the first add and stays until `forget/1`, because dropping it when
the set empties would mean counting the set's keys. And `list/1` unions what
every reachable node can see, reporting the nodes it could not ask, because with
fewer replicas than nodes no single node holds the whole registry.

## The extractor

`Kurwa.Extractor` is the single read path: cache, then single-flight, then the
coordinator. With `cache: false` (the default) it is a straight pass-through, so
enabling the cache changes how often the cluster is reached and nothing else.

Single-flight is the part that matters even with a small cache: a thousand
concurrent checks of one cold key become one quorum read and 999 processes parked
on a deferred reply. The leader is monitored, so a leader that dies hands its
waiters an error instead of leaving them parked until timeout.

Positive and negative answers have separate TTLs, because the two stale answers
fail differently: a stale `true` rejects something new, a stale `false` lets a
duplicate through. Which one you can afford is policy.

**The honest cost.** A write coordinated elsewhere does not invalidate this
node's entry. Writes broadcast a best-effort invalidation to the other members,
but best-effort is the operative word - a dropped message leaves a stale entry
until its TTL. So the cache turns a linearizable-per-key read into a
bounded-staleness read, and the bound is the TTL. A test pins that behaviour down
rather than leaving it implied.

## Decided, not built

**Fallback vnodes.** Today a write goes to the reachable primaries only, so while
a replica is down the write has `n-1` copies until handoff runs. Dynamo-style
fallbacks would write to a substitute node immediately, which means hints have to
live with the data on the fallback rather than on the coordinator. That also
makes hints survive a coordinator restart.

**A disk engine.** The `Engine` behaviour exists so the store is not married to
RAM. Two shapes fit it:

* An **LSM** (memtable, SSTables, bloom filters, compaction). For a key-only
  store the bloom filter is not an optimisation but the whole read path - "is this
  key here" is answered in memory and never touches the disk in the common case.
* **Venti**'s arena plus index. Venti is write-once, SHA-1-addressed block
  storage - the "key = hash(value)" model - and its arena (append-only) plus
  separate index layout is a good blueprint for immutable on-disk sets.

Measured, not estimated: a key costs 120 bytes in ETS with a 13-byte key and 144
bytes with a 36-byte one (200k keys, `:ets.info(:memory)`). So 100M keys is
~11 GB per node, and that is where RAM stops being the obvious answer.

**Kurwa Proxy: native client protocols.** The idea is that a PostgreSQL, MySQL or
MongoDB client talks to kurwadb with no adapter, over its own wire protocol. It
is a real technique - CockroachDB, Materialize, QuestDB and RisingWave all speak
the PostgreSQL wire protocol - and the mapping onto a key-only store is clean:

```sql
SELECT 1 FROM blacklist WHERE key = $1        -- Namespace.member?("blacklist", key)
INSERT INTO blacklist(key) VALUES ($1)        -- Namespace.add
DELETE FROM blacklist WHERE key = $1          -- Namespace.delete
SELECT count(*) FROM blacklist                -- the approximate count
SELECT 1 FROM a WHERE key=$1 UNION
SELECT 1 FROM b WHERE key=$1                  -- member_any?(["a","b"], key)
```

A table is a set, which is the same idea as a directory being a set in 9P. The
sensible implementation is a whitelist of query shapes, not a SQL parser.

The honest part is where the cost actually is, and it is not the codec:

* **Clients introspect before they work.** `psql \d`, ORMs and BI tools issue
  large `pg_catalog` / `information_schema` queries first. Faking enough catalog
  to survive them is most of the work, and it needs the set registry above.
* **Clients assume scans, joins, `ORDER BY`, `LIMIT`.** kurwadb has none of them
  on purpose. Anything beyond a point lookup has to be refused, and a refusal
  from a "PostgreSQL" server surprises people.
* **Transactions.** `BEGIN`/`COMMIT` over an LWW set with no MVCC can only be
  accepted-and-ignored, which is a lie that breaks ORMs quietly. Refusing is
  better and still surprising.
* **Auth.** PostgreSQL means SCRAM-SHA-256; MySQL means `caching_sha2_password`,
  which needs TLS or an RSA exchange. Neither is a weekend.
* **MongoDB** is the easiest codec (OP_MSG plus BSON, no SQL to parse) and the
  hardest handshake: drivers negotiate `hello`, monitor topology, open sessions
  and expect cursors.

So "any client, natively" is not one feature - it is one adapter per protocol,
each with its own semantic mismatch to manage. The recommendation is one protocol
done properly, PostgreSQL first, point lookups and point writes only, as another
thin frontend over `Kurwa.Extractor`. The architecture is already ready for it;
that is the part that matters today.

**Persistent hints**, and **per-set statistics**, both blocked on the same thing:
somewhere to put data that is not a key.
