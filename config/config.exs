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

  # local storage
  engine: Kurwa.Store.Ets,
  shards: 8,
  data_dir: "data",
  wal_sync_interval: 100,
  wal_snapshot_after: 100_000,
  tombstone_ttl: 86_400_000,
  gc_interval: 300_000,

  # cluster
  seeds: [],
  seed_retry_interval: 5_000,

  # gateway
  start_gateway: true,
  http_port: 4040,
  auth_token: nil

config :logger, level: :info

import_config "#{config_env()}.exs"
