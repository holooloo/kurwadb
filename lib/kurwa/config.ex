defmodule Kurwa.Config do
  @moduledoc """
  Single place that reads the `:kurwadb` application environment.

  Every value has a default here, so the app boots with no config file at all.
  """

  @defaults %{
    n: 3,
    r: 2,
    w: 2,
    request_timeout: 2_000,
    vnodes: 128,
    cache: false,
    cache_ttl: 5_000,
    cache_negative_ttl: 500,
    cache_max_keys: 1_000_000,
    cache_sweep_interval: 1_000,
    cache_broadcast: true,
    engine: Kurwa.Store.Ets,
    shards: 8,
    data_dir: "data",
    wal_sync_interval: 100,
    wal_sync_on_write: false,
    lsm_memtable_keys: 100_000,
    lsm_max_tables: 8,
    wal_snapshot_after: 100_000,
    tombstone_ttl: 86_400_000,
    gc_interval: 300_000,
    seeds: [],
    seed_retry_interval: 5_000,
    handoff_interval: 5_000,
    handoff_max_hints: 100_000,
    handoff_batch: 500,
    repair_interval: 600_000,
    repair_buckets: 4_096,
    repair_max_buckets: 64,
    strict_quorum: false,
    start_gateway: true,
    http_port: 4040,
    start_9p: false,
    ninep_port: 564,
    start_pg: false,
    pg_port: 5432,
    pg_auth: :scram,
    pg_tls: nil,
    start_resp: false,
    start_mysql: false,
    start_mongo: false,
    start_mssql: false,
    mssql_port: 1433,
    mssql_tls: nil,
    mssql_encryption: :on,
    procedures_dir: nil,
    public_host: nil,
    dashboard_show_password: false,
    public_ports: %{},
    mongo_port: 27017,
    mysql_port: 3306,
    resp_port: 6379,
    auth_token: nil
  }

  @doc """
  Reads one setting, falling back to the built-in default.

  Settings are read on every request - the quorum sizes, the timeout, the
  engine - and `Application.get_env/3` costs about 45 ns each, which on a
  one-microsecond read is a quarter of it. So `load/0` copies them into
  `:persistent_term` at boot, where a read is a fraction of that, and
  `put/2` / `delete/1` change a setting at runtime and its copy together.
  Change settings through those, not `Application.put_env/3`, or the copy
  goes stale.
  """
  def get(key) when is_map_key(@defaults, key) do
    case :persistent_term.get({__MODULE__, key}, :unloaded) do
      :unloaded -> Application.get_env(:kurwadb, key, Map.fetch!(@defaults, key))
      value -> value
    end
  end

  @doc "Copies every setting into `:persistent_term`. Called at boot."
  def load do
    for key <- Map.keys(@defaults) do
      :persistent_term.put(
        {__MODULE__, key},
        Application.get_env(:kurwadb, key, Map.fetch!(@defaults, key))
      )
    end

    :ok
  end

  @doc "Changes a setting at runtime."
  def put(key, value) when is_map_key(@defaults, key) do
    Application.put_env(:kurwadb, key, value)
    :persistent_term.put({__MODULE__, key}, value)
    :ok
  end

  @doc "Returns a setting to its configured default at runtime."
  def delete(key) when is_map_key(@defaults, key) do
    Application.delete_env(:kurwadb, key)
    :persistent_term.put({__MODULE__, key}, Map.fetch!(@defaults, key))
    :ok
  end

  def n, do: get(:n)
  def r, do: get(:r)
  def w, do: get(:w)
  def vnodes, do: get(:vnodes)
  def shards, do: get(:shards)
  def engine, do: get(:engine)
  def data_dir, do: get(:data_dir)
  def request_timeout, do: get(:request_timeout)
  def tombstone_ttl, do: get(:tombstone_ttl)
  def gc_interval, do: get(:gc_interval)
  def seeds, do: get(:seeds)
  def auth_token, do: get(:auth_token)

  @doc """
  Sanity-checks the quorum settings.

  `r + w > n` is what gives us read-your-writes on a single key; we allow weaker
  settings but say so loudly, because it is a real durability decision.
  """
  def validate! do
    n = n()
    r = r()
    w = w()

    cond do
      n < 1 -> raise ArgumentError, "n must be >= 1, got #{inspect(n)}"
      r < 1 or r > n -> raise ArgumentError, "r must be in 1..#{n}, got #{inspect(r)}"
      w < 1 or w > n -> raise ArgumentError, "w must be in 1..#{n}, got #{inspect(w)}"
      true -> :ok
    end

    if r + w <= n do
      require Logger

      Logger.warning("kurwadb: r + w (#{r} + #{w}) <= n (#{n}) - reads may miss the latest write")
    end

    :ok
  end
end
