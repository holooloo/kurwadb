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
    engine: Kurwa.Store.Ets,
    shards: 8,
    data_dir: "data",
    wal_sync_interval: 100,
    wal_snapshot_after: 100_000,
    tombstone_ttl: 86_400_000,
    gc_interval: 300_000,
    seeds: [],
    seed_retry_interval: 5_000,
    strict_quorum: false,
    start_gateway: true,
    http_port: 4040,
    auth_token: nil
  }

  @doc "Reads one setting, falling back to the built-in default."
  def get(key) when is_map_key(@defaults, key) do
    Application.get_env(:kurwadb, key, Map.fetch!(@defaults, key))
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
