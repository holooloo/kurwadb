# Handoff

Where kurwadb stands and what is next, for whoever picks it up. State as of
2026-10-12, release **0.18.0** (tag `v0.18.0`).

## What is there

* **Core**: a key-only distributed set - Lamport-stamped LWW records,
  consistent-hash ring, quorum reads and writes (`n=3 r=2 w=2` by default),
  hinted handoff, anti-entropy, two engines (ETS, LSM with a Rust Bloom
  filter). Since 0.18.0 a read asks r replicas, this node first, and the rest
  only after `read_hedge_ms`; `KURWA_READ_FANOUT=all` restores asking all n.
* **Frontends**: HTTP, 9P, PostgreSQL (schemas; a catalog evaluator good
  enough for DBeaver and pgjdbc metadata), MySQL, MongoDB, SQL Server (TDS,
  TLS, stored procedures from `.sql` files, SSMS - see below), Redis (RESP2/3).
* **SSMS**: answered from a session recorded against a real SQL Server.
  `deploy/tds_record.py` records, `deploy/ssms_fixture.py` builds
  `priv/mssql/ssms.json`, `deploy/tds_replay.py` checks kurwadb against the
  recording (66 of 67 requests; the 67th is a missing database, answered the
  same way by both). kurwadb reports the recorded server's version because SMO
  chooses its queries by version.
* **Dashboard**: `/dashboard` on every node, fed by `Kurwa.Metrics`
  (per-request counters, sampled once a second). `docs/dashboard.html` is the
  same page on simulated data, built by `scripts/build_demo_dashboard.py`.
* **Clients**: `clients/js` (no dependencies) and `clients/go` (standard
  library only): discovery with `KURWA.NODES`, the ring with `KURWA.RING`,
  each key routed to a replica, failover. `clients/ring_fixture.json` is what
  both check their placement against.
* **Deployment**: `Dockerfile` (the `test` target runs the suite on both
  engines), `deploy/docker-compose.yml` - three published nodes plus an
  optional `demo` load generator; `deploy/smoke.sh HOST` runs every driver
  check over the network.

## How to work on it

```sh
export PATH=$HOME/.cargo/bin:/opt/homebrew/bin:$PATH   # without cargo the NIF is silently skipped
mix test --include psql --include mssql                 # 397 tests
KURWA_TEST_ENGINE=lsm mix test                          # the same suite on the LSM engine
mix test --only cluster                                 # 21 tests against three real nodes
```

* Change settings at runtime with `Kurwa.Config.put/2` and `delete/1`, never
  `Application.put_env/3`: settings are cached in `:persistent_term`.
* Regexes and `:binary.match` pattern lists on request paths go through
  `Kurwa.Re` - since OTP 28 a `~r` literal recompiles on every use.
* The performance rule: a release slower than the previous one is broken.
  Compare against the previous release in a `git worktree`, alternating the
  two, medians - see "What 0.17.0 changed" in PERFORMANCE.md for how.

## Open

* **SSMS** is verified by replaying the recording, not yet by a person in a
  live SSMS against kurwadb. Not recorded yet: a table's columns, database
  properties, "Select Top 1000 Rows" (which kurwadb must refuse: a scan).
* **pgbench and tedious throughput** for 0.17.0/0.18.0 were not measurable on
  the day (±40% run to run); re-run `bench/pg.sh` and `bench/mssql.sh` on a
  quiet machine and add the column.
* **Named sets over RESP** have no TTL and no atomic add-new, so the clients
  answer those with `UNSUPPORTED`; a server command would close it.
* **Clients** are not published to npm or as a tagged Go module, and have no
  TLS; there is no TLS between nodes either.
* GitHub milestones cannot be created from this setup (pushes go through a
  deploy key); releases are marked by annotated tags.

## Next, in the order they pay

1. A live SSMS session against kurwadb, and recordings for whatever it trips on.
2. RESP commands for named-set TTL and add-new, then clients without `UNSUPPORTED`.
3. Publish the clients.
4. Quiet-machine re-measurement of the frontend throughput rows.
5. TLS between nodes.
