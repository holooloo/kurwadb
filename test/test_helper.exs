# Each run starts from an empty data directory: the shards replay their WAL on
# boot, so leftovers from a previous run would leak into the next one.
Application.stop(:kurwadb)

Application.get_env(:kurwadb, :data_dir)
|> File.rm_rf!()

{:ok, _} = Application.ensure_all_started(:kurwadb)

# The cluster tests boot real nodes, which takes seconds each. They are opt-in:
#
#     mix test --include cluster
# The psql and mysql tests need those clients installed:
#
#     mix test --include psql --include mysql
ExUnit.start(exclude: [:cluster, :psql, :mysql])
