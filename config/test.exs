import Config

config :kurwadb,
  n: 1,
  r: 1,
  w: 1,
  shards: 2,
  data_dir: "tmp/test-data",
  start_gateway: false,
  wal_sync_interval: 10,
  gc_interval: 60_000,
  request_timeout: 500

config :logger, level: :warning
