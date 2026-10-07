# kurwadb

A distributed set. Keys, and nothing else.

```elixir
Kurwa.add("order:1029")      #=> :ok
Kurwa.member?("order:1029")  #=> true
Kurwa.delete("order:1029")   #=> :ok
Kurwa.member?("order:1029")  #=> false

Kurwa.add("seen:event:88", ttl: :timer.minutes(10))
```

There are no values, no scans, no queries, no indexes. Every operation names
exactly one key. That is not a missing feature list - it is the constraint the
whole design is bought with:

* **No values** means conflict resolution is a total order on two integers. Two
  replicas that saw the same writes agree, in any order, without talking to each
  other. No vector clocks, no siblings, no merge callbacks.
* **No scans** means a key's location is the only thing that decides which node
  answers, so any node can coordinate any request and there is no leader.
* **A key is its own payload**, so a membership check is one `:ets.lookup` on the
  replica - the read path never touches a process mailbox.

What that buys you is the set of jobs people actually keep a key-only store for:
deduplication, idempotency keys, rate-limit buckets, blocklists, "have I seen
this event", seen-URL frontiers.

## Status

Working, with 246 unit tests and 15 cluster tests — and both storage engines
pass the same suite.

```sh
mix test                          # 372 tests, ~3s
mix test --include cluster        # 19 more, ~14s: three real nodes, three real BEAMs
KURWA_TEST_ENGINE=lsm mix test    # the same suite against the on-disk engine
mix test --include psql           # the real psql against the PostgreSQL frontend
mix test --include mysql          # the real mysql client against the MySQL frontend
mix test --include mongo          # the real mongosh against the MongoDB frontend
mix test --include mssql          # Microsoft's sqlcmd against the SQL Server frontend, over TLS
```

The cluster tests boot actual distributed nodes and assert the guarantees this
README makes: replication, cross-node reads, stable placement across an outage,
hinted handoff, read repair, strict versus lenient quorums, and what a hard crash
costs. They are opt-in only because they start BEAMs with fixed node names.

Running them is how two design holes were found - see
[ARCHITECTURE.md](ARCHITECTURE.md#failure-behaviour-as-measured).

## Quick start

```sh
mix deps.get
mix test
iex -S mix                       # HTTP on http://localhost:4040
```

A Rust toolchain is optional. With `cargo` on the path one function - Bloom
membership, the read path of the on-disk engine - is compiled natively; without
it the Elixir implementation is used and everything still passes.

Three nodes on one machine:

```sh
scripts/cluster.sh               # kurwa1..3, HTTP on 4040, 4041, 4042
```

```sh
curl -X PUT localhost:4040/k/order:1029      # add, on one node
curl -i   localhost:4041/k/order:1029        # 200, read from another
curl -X DELETE localhost:4042/k/order:1029   # delete, from a third
curl      localhost:4040/count
curl      localhost:4040/info
```

### Docker

One node with every frontend on, the on-disk engine, data on a volume and the
procedures from `deploy/procedures`:

```sh
cd deploy
cp .env.example .env             # set KURWA_AUTH_TOKEN; move ports if taken
docker compose up -d --build
```

The token is the password for every protocol, with any user name. Host ports
default to 1433 for SQL Server and the 2xxxx range for the rest, out of the
way of a developer machine's own databases. `docker build --target test .`
runs the suite against both engines inside the build, Rust NIF included, and
`deploy/smoke.sh HOST` runs every driver check in `test/drivers` against a
running container over the network.

## HTTP API

| | |
|---|---|
| `PUT /k/:key` | add the key; `?ttl=<seconds>` or `?ttl_ms=<ms>` to make it expire |
| `GET /k/:key` | `200` if a member, `404` if not, `503` if we could not find out |
| `DELETE /k/:key` | remove the key |
| `POST /batch` | `{"op":"add"\|"member"\|"delete","keys":[...],"ttl":<seconds>}`, up to 1000 keys |
| `PUT GET DELETE /sets/:set/k/:key` | the same three, in a named set |
| `GET /sets` | the named sets that exist |
| `DELETE /sets/:set` | stop listing a set; its keys stay |
| `GET /union/k/:key?sets=a,b` | member of **any** of these sets |
| `GET /intersection/k/:key?sets=a,b` | member of **all** of these sets |
| `GET /count` | approximate live keys, and who reported |
| `GET /info` | ring, reachability, quorum settings, cache and handoff stats |
| `GET /health` | liveness, never authenticated |

A failed quorum is `503`, never `200` - an unreachable replica must not read as
"the key is not in the set".

Keys are URL path segments, so percent-encode `/`. For keys that are not valid
UTF-8, send them base64url-encoded with `?b64=1`; responses echo the key exactly
as it arrived, because a binary key has no JSON form.

Set `KURWA_AUTH_TOKEN` to require `Authorization: Bearer <token>` on everything
except `/health`.

## 9P

Mount the store as a filesystem. `stat` is `member?`, `create` is `add`,
`remove` is `delete` - the same data model, not a metaphor.

```
/ctl                write "compact" | "gc" | "sync" | "join node@host" | "forget node@host"
/stats              local keys, lamport, members, pending handoff
/ring               members with up/down, vnodes, n/r/w
/keys/<key>         the default set
/b64/<base64url>    the default set, for keys that are not valid file names
/sets/<set>/<key>   a named set
```

```sh
KURWA_9P=1 KURWA_9P_PORT=1564 iex -S mix
# then, with plan9port or v9fs:
9p -a localhost:1564 ls /
9p -a localhost:1564 read ring
```

Off by default; port 564 is the registered one but needs privileges to bind.
`/keys`, `/b64` and `/sets/<set>` refuse to be listed - kurwadb has no scans,
and an empty listing would be a lie.

## PostgreSQL

`psql` and the PostgreSQL drivers connect as they would to PostgreSQL. A set is
a table with one column, `key`; the default set is the table `kurwa`.

```sh
KURWA_PG=1 KURWA_PG_PORT=5433 iex -S mix
psql "host=127.0.0.1 port=5433 dbname=kurwadb"
```

```sql
INSERT INTO seen VALUES ('order:1'), ('order:2');
INSERT INTO seen (key, ttl) VALUES ('session:9', 3600);   -- expires in an hour
SELECT key FROM seen WHERE key IN ('order:1', 'order:3'); -- the members among these
SELECT key FROM seen WHERE key = ANY($1);                 -- the same, one text[] parameter
SELECT EXISTS (SELECT 1 FROM seen WHERE key = $1);
DELETE FROM seen WHERE key = 'order:1';                   -- DELETE 1, or DELETE 0 if it was not there
INSERT INTO seen VALUES ($1) ON CONFLICT DO NOTHING;       -- INSERT 0 1 for one caller, 0 0 for the rest
SELECT kurwa_ttl('seen', 'session:9'), kurwa_count();
\dt                                                       -- sets, as tables
```

Schemas are namespaces for sets: `CREATE SCHEMA analytics`, then
`analytics.events` is the set named `analytics.events` - so a set made over
HTTP with a dot in its name shows up inside a schema. `DROP SCHEMA` works while
no set in it has keys. There is one database, `kurwadb`. The catalogs are
modelled well enough for GUI tools: DBeaver's navigator lists schemas, tables
and the `key` column (`test/drivers/JdbcCheck.java` runs its queries through
pgjdbc), and a table designer that names the column anything but `key` is told
so at `CREATE TABLE`.

Without a `WHERE key`, a `SELECT` or `DELETE` is refused with the reason: it
would be a scan. Simple and extended protocol, text and binary formats, so
prepared statements in psycopg, node-postgres and the like work; the scripts in
`test/drivers/` check two of them. `BEGIN` and `COMMIT` are accepted because
drivers send them unasked, but there are no transactions: every statement takes
effect when it runs, and `ROLLBACK` says so.

With `auth_token` set, the password is the token and the method is
SCRAM-SHA-256, as in PostgreSQL 14 and later; `KURWA_PG_AUTH=md5` or `password`
for old clients. Give it a certificate and it does TLS, and over TLS it offers
SCRAM-SHA-256-PLUS, so `channel_binding=require` works:

```sh
KURWA_PG=1 KURWA_AUTH_TOKEN=s3cret \
KURWA_PG_TLS_CERT=server.crt KURWA_PG_TLS_KEY=server.key iex -S mix
PGPASSWORD=s3cret psql "host=127.0.0.1 port=5432 dbname=kurwadb sslmode=require channel_binding=require"
```

The reasons for each choice are in
[ARCHITECTURE.md](ARCHITECTURE.md#the-postgresql-frontend).

## Redis protocol

`redis-cli`, `redis-benchmark` and Redis clients connect as they would to Redis.
Redis sets are named sets, and Redis string keys are the default set, where a
key either exists or does not:

```sh
KURWA_RESP=1 KURWA_RESP_PORT=6380 iex -S mix
redis-cli -p 6380
```

```
SET order:1 x EX 3600 NX     OK, then nil: the idempotency check
EXISTS order:1               1
TTL order:1                  3600
DEL order:1                  1, and 0 the second time
SADD seen:orders a b         2
SMISMEMBER seen:orders a z   1 0
GET order:1                  error: there are no values to return
SMEMBERS seen:orders         error: it would be a scan
```

`SET` discards its value; `GET` refuses rather than invent one. `SET NX` has a
single winner among concurrent callers, as on Redis, without a leader - a
majority of replicas has to agree, and the reasoning is in
[ARCHITECTURE.md](ARCHITECTURE.md#one-winner-without-a-leader). `WATCH` works:
`EXEC` runs nothing if a watched key changed. `SADD`, `SREM` and `DEL` report
what changed; `MULTI`/`EXEC` queue and run in order, without isolation from
other clients. RESP2 and RESP3, pipelining, `AUTH` with `auth_token`.
`test/drivers/redis_py_check.py` runs redis-py against it.

## MySQL protocol

`mysql`, `mariadb` and the MySQL connectors (PyMySQL, Connector/Python, mysql2)
connect as they would to MySQL 8. The tables are the same as over PostgreSQL;
`key` is a reserved word in MySQL, so it is written with backticks:

```sh
KURWA_MYSQL=1 KURWA_MYSQL_PORT=3307 iex -S mix
mysql -h 127.0.0.1 -P 3307 kurwadb
```

```sql
INSERT INTO seen VALUES ('order:1'), ('order:2');          -- 2 rows affected
INSERT IGNORE INTO seen VALUES ('order:1'), ('order:3');   -- 1 row affected: only the new one
SELECT `key` FROM seen WHERE `key` IN (?, ?);             -- prepared, binary protocol
DELETE FROM seen WHERE `key` = 'order:1';                 -- 1, or 0 if it was not there
SHOW TABLES;  DESCRIBE seen;  SHOW VARIABLES LIKE 'version%';
```

`INSERT IGNORE` has one winner among concurrent inserts, like `SET NX`.
Authentication is `caching_sha2_password` or `mysql_native_password`, with
`auth_token` as the password; TLS is not offered on this port, which clients'
default `ssl-mode=PREFERRED` accepts. Columns are named as MySQL names them, so
dictionary cursors find `DATABASE()` under that name. `test/drivers/` has
PyMySQL, Connector/Python and mysql2 checks.

## MongoDB protocol

`mongosh` and the MongoDB drivers connect as they would to a `mongos`. A
collection is a set and a document is `{_id: key}`; in the database `kurwadb` a
collection is the set of the same name, so `seen` is the table SQL sees:

```sh
KURWA_MONGO=1 KURWA_MONGO_PORT=27018 iex -S mix
mongosh mongodb://127.0.0.1:27018/kurwadb
```

```js
db.seen.insertMany([{_id: "order:1"}, {_id: "order:2"}])
db.seen.insertOne({_id: "order:1"})                    // E11000 duplicate key, one winner
db.seen.find({_id: {$in: ["order:1", "order:3"]}})     // [{_id: "order:1"}]
db.seen.updateOne({_id: "job:9"}, {$setOnInsert: {}}, {upsert: true})
db.seen.insertOne({_id: "s", expireAt: new Date(Date.now() + 3600e3)})  // a TTL
db.seen.find({})                                       // error: it would be a scan
```

Filters are on `_id` only, and a document has no fields but `_id` (and
`expireAt`, which becomes the key's TTL). Every node answers `hello` as a
`mongos`, so a driver given several nodes - `mongodb://n1,n2,n3/` - treats them
as routers and fails over between them. SCRAM-SHA-256 with `auth_token` as the
password, for any user. `abortTransaction` is an error that says nothing was
rolled back. `test/drivers/` has PyMongo and Node.js driver checks.

## SQL Server protocol (TDS)

`sqlcmd`, ODBC Driver 18, tedious, pymssql and the other SQL Server drivers
connect as they would to SQL Server 2022. The tables are the PostgreSQL and
MySQL ones, written the T-SQL way:

```sh
KURWA_MSSQL=1 KURWA_MSSQL_PORT=1434 iex -S mix
sqlcmd -S tcp:127.0.0.1,1434 -U app -P "$KURWA_AUTH_TOKEN" -C
```

```sql
INSERT INTO seen VALUES (N'order:1'), (N'order:2');
SELECT [key] FROM dbo.seen WHERE [key] IN (@a, @b);            -- sp_executesql from drivers
SELECT TOP 1 [key] FROM seen WHERE [key] = 'order:1';
IF NOT EXISTS (SELECT 1 FROM seen WHERE [key] = @k)
  INSERT INTO seen VALUES (@k);                               -- one winner, like SET NX
DELETE FROM seen OUTPUT deleted.[key] WHERE [key] = 'order:1';
```

Encryption is on by default, as drivers expect, including "login-only" and
TDS 8 strict. Without `KURWA_MSSQL_TLS_CERT` / `KURWA_MSSQL_TLS_KEY`, a
self-signed certificate is made at start, as SQL Server does, so clients need
`TrustServerCertificate=yes` (`sqlcmd -C`); strict encryption needs a
certificate the client trusts. SQL Server authentication with `auth_token`
as the password; Windows authentication is refused. Parameters arrive as
`sp_executesql` / `sp_prepare` / `sp_execute` RPCs and are answered as such.
`test/drivers/` has pymssql/pyodbc and tedious checks.

### Stored procedures

Procedures are `.sql` files in `KURWA_PROCEDURES_DIR`, read when the node
starts - a definition is a value, and this store keeps keys, so they are
deployed with the node rather than created with `CREATE PROCEDURE` over the
wire. [`examples/procedures/tokens.sql`](examples/procedures/tokens.sql):

```sql
CREATE OR ALTER PROCEDURE dbo.consume @token NVARCHAR(200), @taken BIT OUTPUT AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM tokens WHERE [key] = @token)
        INSERT INTO tokens VALUES (@token);          -- one atomic add_new
    SET @taken = @@ROWCOUNT;
    IF @taken = 0 RETURN 1;
    RETURN 0;
END
```

```sql
DECLARE @t BIT, @rc INT;
EXEC @rc = dbo.consume N'order:1', @t OUTPUT;   -- @t = 1, @rc = 0; the second time 0 and 1
```

Drivers call them by name too (ODBC `{call ...}`, tedious `callProcedure`).
The language is a T-SQL subset: parameters with defaults and OUTPUT, `DECLARE`,
`SET`, `IF`/`ELSE`, `BEGIN ... END`, conditions with comparisons, `AND`/`OR`/`NOT`,
`IS NULL`, `IN` and `EXISTS`, `RETURN`, `THROW`, `RAISERROR`, `PRINT`, nested
`EXEC` up to 32 deep, and the statements a set answers. `WHILE`, cursors,
`TRY/CATCH`, `CASE` and table variables are refused by name. A broken file
stops the node from starting; `/info` shows a hash of what each node loaded, so
nodes with different files are visible.

## Configuration

Every setting has a default, so the app boots with no config at all. Environment
variables are read at boot (`config/runtime.exs`).

| setting | env | default | |
|---|---|---|---|
| `n` `r` `w` | `KURWA_N` `KURWA_R` `KURWA_W` | 3, 2, 2 | replicas, read quorum, write quorum |
| `strict_quorum` | `KURWA_STRICT_QUORUM` | `false` | fail instead of lowering the quorum to the replicas that exist |
| `vnodes` | `KURWA_VNODES` | 128 | ring points per node |
| `shards` | `KURWA_SHARDS` | 8 | local ETS tables, for write concurrency |
| `data_dir` | `KURWA_DATA_DIR` | `data` | WAL and snapshots, scoped per node |
| `engine` | `KURWA_ENGINE` | `Kurwa.Store.Ets` | `lsm` switches to the on-disk engine |
| `seeds` | `KURWA_SEEDS` | `[]` | comma-separated nodes to connect to |
| `tombstone_ttl` | `KURWA_TOMBSTONE_TTL_MS` | 24h | must exceed your longest outage |
| `repair_interval` | | 10 min | how often anti-entropy checks one peer |
| `repair_buckets` | | 4096 | digest vector size; must match across the cluster |
| `wal_sync_on_write` | `KURWA_WAL_SYNC_ON_WRITE` | `false` | fsync every write; closes the crash window at a cost per write |
| `cache` | | `false` | extractor cache; trades linearizable reads for bounded staleness |
| `http_port` | `KURWA_HTTP_PORT` | 4040 | |
| `start_9p` `ninep_port` | `KURWA_9P` `KURWA_9P_PORT` | `false`, 564 | |
| `start_pg` `pg_port` | `KURWA_PG` `KURWA_PG_PORT` | `false`, 5432 | the PostgreSQL wire protocol |
| `pg_auth` | `KURWA_PG_AUTH` | `:scram` | `:scram`, `:md5` or `:password`, when `auth_token` is set |
| `pg_tls` | `KURWA_PG_TLS_CERT` `KURWA_PG_TLS_KEY` | none | `:ssl` server options; with them, SSLRequest is accepted |
| `start_resp` `resp_port` | `KURWA_RESP` `KURWA_RESP_PORT` | `false`, 6379 | the Redis protocol |
| `start_mysql` `mysql_port` | `KURWA_MYSQL` `KURWA_MYSQL_PORT` | `false`, 3306 | the MySQL protocol |
| `start_mongo` `mongo_port` | `KURWA_MONGO` `KURWA_MONGO_PORT` | `false`, 27017 | the MongoDB protocol |
| `start_mssql` `mssql_port` | `KURWA_MSSQL` `KURWA_MSSQL_PORT` | `false`, 1433 | the SQL Server protocol (TDS) |
| `mssql_tls` | `KURWA_MSSQL_TLS_CERT` `KURWA_MSSQL_TLS_KEY` | self-signed | `:ssl` server options for TDS |
| `procedures_dir` | `KURWA_PROCEDURES_DIR` | none | `.sql` files of stored procedures, read at start |
| `auth_token` | `KURWA_AUTH_TOKEN` | none | bearer token for HTTP, the password for PostgreSQL, MySQL, SQL Server, MongoDB and Redis `AUTH` |

`r + w > n` is what gives you read-your-writes on a key. Weaker settings are
allowed and logged as a warning, because it is a real durability decision.

**Durability has a window by default.** A write reaches the log immediately but
the log is fsynced every `wal_sync_interval` (100ms), so a node killed hard can
come back missing writes it had already acknowledged. Replication is the intended
answer - another replica has it, and a read repairs the one that lost it - and
`wal_sync_on_write: true` closes the window locally if you would rather pay an
fsync per write. There is a cluster test for each of those two behaviours.

## Layout

```
Kurwa                 the public API, default set
Kurwa.Namespace       named sets, union and intersection on the read path
Kurwa.Extractor       cache + single-flight (pass-through unless enabled)
Kurwa.Coordinator     leaderless quorum reads and writes, read repair
Kurwa.Placement       which replicas own a key, and which can answer
Kurwa.Handoff         writes a replica missed, kept durably, replayed on return
Kurwa.Repair          anti-entropy: finds replicas that drifted silently
Kurwa.Registry        which named sets exist, as keys in a reserved set
Kurwa.Cluster         membership, reachability, the ring
Kurwa.Ring            consistent hashing
Kurwa.Quorum          first-K-of-N fan-out
Kurwa.Store           local shards
Kurwa.Store.Engine    storage behaviour, with two implementations
Kurwa.Store.Ets       every key in memory; the default
Kurwa.Store.Lsm       memtable plus sorted tables on disk, ~3 B of RAM per key
Kurwa.Native          the little that is Rust: Bloom membership
Kurwa.Gateway         HTTP
Kurwa.NineP           9P2000
Kurwa.Pg              the PostgreSQL wire protocol, and enough pg_catalog for psql
Kurwa.Sql             the SQL a set understands, shared by any SQL frontend
Kurwa.Resp            the Redis protocol, RESP2 and RESP3
Kurwa.Mysql           the MySQL protocol, text and binary result sets
Kurwa.Mongo           the MongoDB protocol: OP_MSG, BSON, answering as a mongos
Kurwa.Mssql           the SQL Server protocol, TDS 7.4 and 8, with TLS inside PRELOGIN
Kurwa.Procedures      stored procedures, loaded from .sql files
Kurwa.Sql.Procedural  the T-SQL subset they and batches run in
```

[ARCHITECTURE.md](ARCHITECTURE.md) has the reasoning, the measured failure
behaviour, and what is deliberately not built yet.
[docs/adr/0001](docs/adr/0001-key-only-distributed-set.md) is the case for the
key-only model against other stores, with the component diagrams and the measured
numbers.

## Measured

Speed is part of the contract: **a release that is slower than the one before it
is a broken release.** Every figure below comes from a script in [`bench/`](bench),
and every version gets a column in [PERFORMANCE.md](PERFORMANCE.md).

One laptop, all three nodes sharing the same CPU.

| | |
|---|---|
| local membership check | 361 ns |
| quorum write / read across 3 nodes | ~40 µs single-client latency |
| the same, 64 concurrent clients | 65k writes/sec, 69k reads/sec |
| `GET /k/:key` over HTTP, keep-alive | 131k req/sec |
| `POST /batch`, 100 keys per request | 446k keys/sec |
| RAM per key | 128 B (13-byte keys), 152 B (36-byte keys) |

```sh
KURWA_DATA_DIR=tmp/bench mix run bench/local.exs
MIX_ENV=test mix run --no-start bench/cluster.exs
bench/http.sh
```

## License

MIT
