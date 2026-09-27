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
  ninep_port: 564

config :logger, level: :info

import_config "#{config_env()}.exs"
