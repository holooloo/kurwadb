import Config

config :kurwadb,
  # replication / quorum
  n: 3,
  r: 2,
  w: 2,
  strict_quorum: false,
  request_timeout: 2_000,

  # ring
  vnodes: 128,

  # extractor: read-path cache, off by default (it trades linearizable reads
  # for bounded staleness - see Kurwa.Extractor)
  cache: false,
  cache_ttl: 5_000,
  cache_negative_ttl: 500,
  cache_max_keys: 1_000_000,
  cache_sweep_interval: 1_000,
  cache_broadcast: true,

  # local storage
  engine: Kurwa.Store.Ets,
  shards: 8,
  data_dir: "data",
  wal_sync_interval: 100,
  wal_sync_on_write: false,

  # only read by Kurwa.Store.Lsm, the on-disk engine
  lsm_memtable_keys: 100_000,
  lsm_max_tables: 8,
  wal_snapshot_after: 100_000,
  tombstone_ttl: 86_400_000,
  gc_interval: 300_000,

  # cluster
  seeds: [],
  seed_retry_interval: 5_000,

  # hinted handoff: writes a replica missed, replayed when it is back
  handoff_interval: 5_000,
  handoff_max_hints: 100_000,
  handoff_batch: 500,

  # active anti-entropy: finds replicas that drifted with nobody watching.
  # repair_buckets must match across the cluster or rounds are skipped.
  repair_interval: 600_000,
  repair_buckets: 4_096,
  repair_max_buckets: 64,

  # gateways
  start_gateway: true,
  http_port: 4040,
  auth_token: nil,

  # 9P frontend: off by default, and port 564 needs privileges to bind
  start_9p: false,
  ninep_port: 564,

  # PostgreSQL wire protocol: psql and drivers, off by default
  start_pg: false,
  pg_port: 5432,
  # scram (SCRAM-SHA-256, PostgreSQL's default since 14), md5 or password.
  # Only consulted when auth_token is set; without one, anyone may connect.
  pg_auth: :scram,
  # :ssl server options - certfile and keyfile at least - to accept TLS
  pg_tls: nil,

  # Redis protocol: redis-cli and Redis clients, off by default
  start_resp: false,
  resp_port: 6379,

  # MySQL protocol: mysql, mariadb and the MySQL connectors, off by default
  start_mysql: false,
  mysql_port: 3306,

  # MongoDB protocol: mongosh and the MongoDB drivers, off by default
  start_mongo: false,
  mongo_port: 27017,

  # Microsoft SQL Server protocol (TDS): sqlcmd, ODBC, .NET, JDBC, off by default.
  # mssql_tls takes :ssl server options; without them a self-signed certificate
  # is made at start, as SQL Server does. mssql_encryption: :off refuses TLS.
  start_mssql: false,
  mssql_port: 1433,
  mssql_tls: nil,
  mssql_encryption: :on

config :logger, level: :info

import_config "#{config_env()}.exs"
