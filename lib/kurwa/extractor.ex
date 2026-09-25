defmodule Kurwa.Extractor do
  @moduledoc """
  The read path everything goes through.

      frontends (HTTP, 9P)  ->  Kurwa / Kurwa.Namespace
                            ->  Kurwa.Extractor      cache + single-flight
                            ->  Kurwa.Coordinator    quorum + read repair
                            ->  Kurwa.Store          ETS + WAL

  With `cache: false` (the default) this layer is a straight pass-through and
  costs nothing, so enabling it never changes where the code goes, only how often
  it reaches the cluster.

  With the cache on, a membership check is served from a node-local ETS table,
  concurrent misses on the same key collapse into one quorum read
  (`Kurwa.Extractor.Flight`), and writes are written through: `add/2` leaves
  `true` behind, `delete/2` leaves `false`.

  ## What the cache costs you

  A write coordinated on another node does not invalidate this node's cache
  entry. `cache_broadcast` (on by default) sends a best-effort invalidation to
  the other members, but it is best-effort - a dropped message or a node that was
  briefly unreachable leaves a stale entry until its TTL runs out. So the cache
  turns a linearizable-per-key read into a bounded-staleness read, and the bound
  is `cache_ttl` / `cache_negative_ttl`. If you cannot afford that, leave it off.
  """

  alias Kurwa.Cluster
  alias Kurwa.Config
  alias Kurwa.Coordinator
  alias Kurwa.Extractor.Cache
  alias Kurwa.Extractor.Flight
  alias Kurwa.Key

  @type opts :: keyword()

  @doc "Is `key` in the set? `set:` picks a named set."
  @spec member?(binary(), opts()) :: {:ok, boolean()} | {:error, term()}
  def member?(key, opts \\ []) when is_binary(key) do
    storage_key = storage_key(key, opts)

    if enabled?() do
      cached(storage_key, opts)
    else
      Coordinator.member?(storage_key, opts)
    end
  end

  @doc "Adds `key`, then writes the answer through the cache."
  @spec add(binary(), opts()) :: :ok | {:error, term()}
  def add(key, opts \\ []) when is_binary(key), do: write(key, opts, &Coordinator.add/2, true)

  @doc "Removes `key`, then writes the answer through the cache."
  @spec delete(binary(), opts()) :: :ok | {:error, term()}
  def delete(key, opts \\ []) when is_binary(key),
    do: write(key, opts, &Coordinator.delete/2, false)

  @doc "Forgets a key everywhere we can reach, without touching the data."
  @spec invalidate(binary(), opts()) :: :ok
  def invalidate(key, opts \\ []) when is_binary(key) do
    storage_key = storage_key(key, opts)
    invalidate_local(storage_key)
    broadcast(storage_key)
  end

  @doc "Invalidation target for peers. Local only, no forwarding."
  @spec invalidate_local(binary()) :: :ok
  def invalidate_local(storage_key) when is_binary(storage_key), do: Cache.delete(storage_key)

  @doc "Cache counters and size."
  defdelegate stats(), to: Cache

  @doc "Drops every cached answer on this node."
  defdelegate flush(), to: Cache

  defp cached(storage_key, opts) do
    case Cache.get(storage_key) do
      {:ok, member?} ->
        Cache.hit()
        {:ok, member?}

      :miss ->
        timeout = Keyword.get(opts, :timeout, Config.request_timeout())

        case Flight.fetch(storage_key, fn -> reload(storage_key, opts) end, timeout * 4) do
          {:lead, result} ->
            Cache.miss()
            result

          {:joined, result} ->
            Cache.coalesced()
            result
        end
    end
  end

  # An error is never cached: the next reader should try the cluster again.
  defp reload(storage_key, opts) do
    case Coordinator.member?(storage_key, opts) do
      {:ok, member?} = answer ->
        Cache.put(storage_key, member?)
        answer

      error ->
        error
    end
  end

  defp write(key, opts, operation, outcome) do
    storage_key = storage_key(key, opts)

    case operation.(storage_key, opts) do
      :ok ->
        if enabled?() do
          Cache.put(storage_key, outcome)
          broadcast(storage_key)
        end

        :ok

      {:error, _reason} = error ->
        # A failed write may still have reached some replicas, so the honest
        # cached value is no value at all.
        if enabled?() do
          Cache.delete(storage_key)
          broadcast(storage_key)
        end

        error
    end
  end

  defp broadcast(storage_key) do
    if Config.get(:cache_broadcast) do
      for node <- Cluster.members(), node != node() do
        :erpc.cast(node, __MODULE__, :invalidate_local, [storage_key])
      end
    end

    :ok
  end

  defp storage_key(key, opts), do: Key.encode(Keyword.get(opts, :set), key)

  defp enabled?, do: Config.get(:cache)
end
