defmodule Kurwa.Store.Ets do
  @moduledoc """
  The default engine: every key in an ETS set, every accepted write in a WAL.

  The ETS table is `:protected` and `:named_table`, so the owning shard process
  is the only writer while any process on the node can read it directly. That is
  what makes `member?/1` cheap - a local membership check is one `:ets.lookup`
  in the caller, not a `GenServer.call`.

  Live keys are tracked in an `:atomics` counter instead of `:ets.info(:size)`,
  because the table also holds tombstones and `count/0` must not see them.
  """

  @behaviour Kurwa.Store.Engine

  alias Kurwa.Clock
  alias Kurwa.Record
  alias Kurwa.Store.Wal

  require Logger

  defstruct [:name, :table, :live, :wal, :dir, :snapshot_after]

  @type t :: %__MODULE__{}

  @impl true
  def open(name, opts) do
    dir = Keyword.fetch!(opts, :dir)
    snapshot_after = Keyword.get(opts, :snapshot_after, 100_000)

    table =
      :ets.new(table_name(name), [
        :set,
        :protected,
        :named_table,
        read_concurrency: true
      ])

    live = :counters.new(1, [:atomics])
    :persistent_term.put(pt_key(name), %{table: table_name(name), live: live})

    replay = fn record, n ->
      restore(table, live, record)
      n + 1
    end

    case Wal.replay(dir, 0, replay) do
      {:ok, _n, from_wal} ->
        case Wal.open(dir) do
          {:ok, wal} ->
            state = %__MODULE__{
              name: name,
              table: table_name(name),
              live: live,
              wal: %{wal | appended: from_wal},
              dir: dir,
              snapshot_after: snapshot_after
            }

            {:ok, state}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def close(%__MODULE__{} = state) do
    Wal.close(state.wal)
    :persistent_term.erase(pt_key(state.name))
    # Normally the table dies with the shard process; deleting it explicitly lets
    # the same instance be reopened in the same process, which recovery does.
    if :ets.whereis(state.table) != :undefined, do: :ets.delete(state.table)
    :ok
  end

  @impl true
  def handle(name) do
    :persistent_term.get(pt_key(name))
  end

  @impl true
  def get(%{table: table}, key) when is_binary(key), do: lookup(table, key)

  @impl true
  def count(%{live: live}), do: :counters.get(live, 1)

  @impl true
  def fold(%{table: table}, acc, fun) when is_function(fun, 2) do
    :ets.foldl(fun, acc, table)
  end

  @impl true
  def put(%__MODULE__{} = state, record) do
    Clock.observe(Record.lamport(record))
    key = Record.key(record)
    current = lookup(state.table, key)

    if Record.newer?(record, current) do
      true = :ets.insert(state.table, record)
      adjust_live(state.live, current, record)

      case Wal.append(state.wal, record) do
        {:ok, wal} ->
          {:ok, record, %{state | wal: wal}}

        {:error, reason} ->
          # The record is already visible in memory; losing its log entry only
          # costs us durability across a restart, so shout but do not crash the
          # shard and take the whole replica offline.
          Logger.error("kurwadb: WAL append failed in #{state.dir}: #{inspect(reason)}")
          {:ok, record, state}
      end
    else
      {:stale, current, state}
    end
  end

  @impl true
  def sync(%__MODULE__{wal: wal}) do
    case Wal.sync(wal) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("kurwadb: WAL sync failed: #{inspect(reason)}")
        :ok
    end
  end

  @impl true
  def gc(%__MODULE__{} = state, cutoff) when is_integer(cutoff) do
    # Tombstones only. Dropping one is not logged: a replay can resurrect it
    # from the WAL, which is harmless (it is still a tombstone) and it goes away
    # for good at the next compaction.
    spec = [{{:_, :_, :_, false, :"$1"}, [{:<, :"$1", cutoff}], [true]}]
    {:ets.select_delete(state.table, spec), state}
  end

  @impl true
  def maybe_compact(%__MODULE__{wal: wal, snapshot_after: after_n} = state) do
    if wal.appended >= after_n, do: compact(state), else: state
  end

  @impl true
  def compact(%__MODULE__{} = state) do
    records = :ets.tab2list(state.table)

    case Wal.compact(state.wal, records) do
      {:ok, wal} ->
        Logger.info("kurwadb: compacted #{state.dir} (#{length(records)} records)")
        %{state | wal: wal}

      {:error, reason} ->
        Logger.error("kurwadb: compaction of #{state.dir} failed: #{inspect(reason)}")
        state
    end
  end

  @doc "ETS table name for an engine instance."
  def table_name(name), do: :"#{name}_tab"

  defp pt_key(name), do: {__MODULE__, name}

  defp lookup(table, key) do
    case :ets.lookup(table, key) do
      [record] -> record
      [] -> nil
    end
  end

  # Replay path: same merge rule as `put/2`, but no WAL write (we are reading it).
  defp restore(table, live, record) do
    Clock.observe(Record.lamport(record))
    key = Record.key(record)
    current = lookup(table, key)

    if Record.newer?(record, current) do
      true = :ets.insert(table, record)
      adjust_live(live, current, record)
    end

    :ok
  end

  defp adjust_live(live, current, new) do
    case {Record.alive?(current), Record.alive?(new)} do
      {false, true} -> :counters.add(live, 1, 1)
      {true, false} -> :counters.sub(live, 1, 1)
      _same -> :ok
    end
  end
end
