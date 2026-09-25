# Each run starts from an empty data directory: the shards replay their WAL on
# boot, so leftovers from a previous run would leak into the next one.
Application.stop(:kurwadb)

Application.get_env(:kurwadb, :data_dir)
|> File.rm_rf!()

{:ok, _} = Application.ensure_all_started(:kurwadb)

ExUnit.start()
