import Config

# The whole suite runs against either engine:
#
#     mix test                         the ETS engine
#     KURWA_TEST_ENGINE=lsm mix test   the on-disk one
#
# Both have to satisfy the same contract, and running the suite is how that is
# checked rather than asserted.
engine =
  if System.get_env("KURWA_TEST_ENGINE") == "lsm",
    do: Kurwa.Store.Lsm,
    else: Kurwa.Store.Ets

config :kurwadb,
  engine: engine,
  lsm_memtable_keys: 200,
  lsm_max_tables: 3,
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
