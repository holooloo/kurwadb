defmodule Kurwa.Extractor.Cache do
  @moduledoc """
  Node-local membership cache: storage key -> `true | false`, with an expiry.

  A public ETS table, so a hit costs one `:ets.lookup` in the caller. The process
  only owns the table and sweeps it.

  Positive and negative answers get separate TTLs on purpose. For the jobs a
  key-only store is actually used for - deduplication, idempotency keys, rate
  limiting - the two stale answers fail differently: a stale `true` rejects
  something new, a stale `false` lets a duplicate through. Which one you can
  afford is a policy decision, so it is two settings rather than one.
  """

  use GenServer

  alias Kurwa.Config

  require Logger

  @table :kurwa_extractor_cache
  @counters {__MODULE__, :counters}

  @hits 1
  @misses 2
  @coalesced 3
  @evictions 4

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Cached answer for a storage key, if it has not expired."
  @spec get(binary()) :: {:ok, boolean()} | :miss
  def get(storage_key) do
    case :ets.lookup(@table, storage_key) do
      [{^storage_key, member?, expires_at}] ->
        if expires_at > now(), do: {:ok, member?}, else: :miss

      [] ->
        :miss
    end
  rescue
    ArgumentError -> :miss
  end

  @doc """
  Caches an answer, with the TTL that matches its polarity.

  `max_ttl` caps it: a key that expires in 200ms must not be remembered as
  present for the full positive TTL, or the cache would outlive the key.
  """
  @spec put(binary(), boolean(), non_neg_integer() | :never) :: :ok
  def put(storage_key, member?, max_ttl \\ :never) do
    ttl = if member?, do: Config.get(:cache_ttl), else: Config.get(:cache_negative_ttl)
    ttl = if max_ttl == :never, do: ttl, else: min(ttl, max_ttl)

    :ets.insert(@table, {storage_key, member?, now() + ttl})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Forgets one key."
  @spec delete(binary()) :: :ok
  def delete(storage_key) do
    :ets.delete(@table, storage_key)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Forgets everything."
  def flush do
    :ets.delete_all_objects(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Cached entries, expired ones included until the next sweep."
  def size do
    case :ets.info(@table, :size) do
      :undefined -> 0
      size -> size
    end
  end

  def hit, do: bump(@hits)
  def miss, do: bump(@misses)
  def coalesced, do: bump(@coalesced)

  @doc "Counters since boot, plus the current size."
  def stats do
    %{
      hits: read(@hits),
      misses: read(@misses),
      coalesced: read(@coalesced),
      evictions: read(@evictions),
      size: size(),
      enabled: Config.get(:cache)
    }
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true,
      decentralized_counters: true
    ])

    :persistent_term.put(@counters, :counters.new(4, [:write_concurrency]))
    schedule()
    {:ok, %{}}
  end

  @impl true
  def handle_info(:sweep, state) do
    dropped = :ets.select_delete(@table, [{{:_, :_, :"$1"}, [{:<, :"$1", now()}], [true]}])

    # Over the ceiling after a sweep means the live set genuinely does not fit.
    # Flushing wholesale is blunt but O(1) and predictable, and single-flight
    # keeps the refill from turning into a stampede on the cluster.
    if size() > Config.get(:cache_max_keys) do
      flush()
      bump(@evictions)
      Logger.warning("kurwadb: extractor cache over #{Config.get(:cache_max_keys)} keys, flushed")
    end

    if dropped > 0 do
      Logger.debug("kurwadb: extractor cache swept #{dropped} expired entries")
    end

    schedule()
    {:noreply, state}
  end

  @impl true
  def handle_info(_other, state), do: {:noreply, state}

  defp schedule, do: Process.send_after(self(), :sweep, Config.get(:cache_sweep_interval))

  defp now, do: System.monotonic_time(:millisecond)

  defp bump(index) do
    case :persistent_term.get(@counters, nil) do
      nil -> :ok
      ref -> :counters.add(ref, index, 1)
    end
  end

  defp read(index) do
    case :persistent_term.get(@counters, nil) do
      nil -> 0
      ref -> :counters.get(ref, index)
    end
  end
end
