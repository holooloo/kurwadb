defmodule Kurwa.Store.Shard do
  @moduledoc """
  Owns one local partition: one engine instance, one WAL, one ETS table.

  Sharding here is purely about local write concurrency - which node holds a key
  is the ring's business (`Kurwa.Ring`), which shard holds it locally is just
  `:erlang.phash2/2`. Writes are serialised through this process so WAL appends
  stay ordered; reads bypass it entirely.
  """

  use GenServer

  alias Kurwa.Config
  alias Kurwa.Record

  require Logger

  @doc "Registered name of shard `index`."
  def name(index), do: :"kurwa_shard_#{index}"

  def child_spec(index) do
    %{
      id: {__MODULE__, index},
      start: {__MODULE__, :start_link, [index]},
      type: :worker,
      restart: :permanent,
      shutdown: 30_000
    }
  end

  def start_link(index) do
    GenServer.start_link(__MODULE__, index, name: name(index))
  end

  @doc "Merges `record` into this shard. Returns the winning record."
  @spec put(non_neg_integer(), Record.t(), timeout()) ::
          {:ok, Record.t()} | {:stale, Record.t()} | {:error, term()}
  def put(index, record, timeout \\ 5_000) do
    GenServer.call(name(index), {:put, record}, timeout)
  catch
    :exit, reason -> {:error, {:shard_unavailable, reason}}
  end

  @doc "Forces a compaction (snapshot + fresh WAL)."
  def compact(index), do: GenServer.call(name(index), :compact, 60_000)

  @doc "Forces a tombstone sweep. Returns how many tombstones were dropped."
  def gc(index), do: GenServer.call(name(index), :gc, 60_000)

  @doc "Forces an fsync of the WAL."
  def sync(index), do: GenServer.call(name(index), :sync, 30_000)

  @impl true
  def init(index) do
    Process.flag(:trap_exit, true)
    engine = Config.engine()
    dir = Kurwa.Store.dir(index)

    # Every engine option, whichever engine is configured: an engine ignores
    # the ones it has no use for.
    opts = [
      dir: dir,
      snapshot_after: Config.get(:wal_snapshot_after),
      sync_on_write: Config.get(:wal_sync_on_write),
      memtable_keys: Config.get(:lsm_memtable_keys),
      max_tables: Config.get(:lsm_max_tables)
    ]

    case engine.open(name(index), opts) do
      {:ok, engine_state} ->
        schedule(:sync, Config.get(:wal_sync_interval))
        schedule(:gc, Config.gc_interval())
        {:ok, %{index: index, engine: engine, engine_state: engine_state}}

      {:error, reason} ->
        {:stop, {:engine_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:put, record}, _from, state) do
    {result, winner, engine_state} = state.engine.put(state.engine_state, record)
    {:reply, {result, winner}, %{state | engine_state: engine_state}}
  end

  @impl true
  def handle_call(:compact, _from, state) do
    engine_state = state.engine.compact(state.engine_state)
    {:reply, :ok, %{state | engine_state: engine_state}}
  end

  @impl true
  def handle_call(:gc, _from, state) do
    {dropped, engine_state} = state.engine.gc(state.engine_state, cutoff())
    {:reply, {:ok, dropped}, %{state | engine_state: engine_state}}
  end

  @impl true
  def handle_call(:sync, _from, state) do
    :ok = state.engine.sync(state.engine_state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:sync, state) do
    :ok = state.engine.sync(state.engine_state)
    engine_state = state.engine.maybe_compact(state.engine_state)
    schedule(:sync, Config.get(:wal_sync_interval))
    {:noreply, %{state | engine_state: engine_state}}
  end

  @impl true
  def handle_info(:gc, state) do
    {dropped, engine_state} = state.engine.gc(state.engine_state, cutoff())

    if dropped > 0 do
      Logger.info("kurwadb: shard #{state.index} dropped #{dropped} tombstones")
    end

    schedule(:gc, Config.gc_interval())
    {:noreply, %{state | engine_state: engine_state}}
  end

  @impl true
  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    state.engine.close(state.engine_state)
    :ok
  end

  defp cutoff, do: System.system_time(:millisecond) - Config.tombstone_ttl()

  defp schedule(msg, interval), do: Process.send_after(self(), msg, interval)
end
