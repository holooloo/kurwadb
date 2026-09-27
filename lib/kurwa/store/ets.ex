defmodule Kurwa.Store.Ets do
  @moduledoc """
  The default engine: every key in an ETS set, every accepted write in a WAL.

  The ETS table is `:protected` and `:named_table`, so the owning shard process
  is the only writer while any process on the node can read it directly. That is
  what makes `member?/1` cheap - a local membership check is one `:ets.lookup`
  in the caller, not a `GenServer.call`.

  Live keys are tracked in an `:atomics` counter instead of `:ets.info(:size)`,
  because the table also holds tombstones and `count/0` must not see them.

  `count/0` counts records whose tombstone flag is set, so a key that expired
  but has not been swept yet is still counted. The sweep corrects it; between
  sweeps the count can run high by however many keys expired.

  Durability has a window by default. A write is appended to the log immediately
  but the log is fsynced periodically (`wal_sync_interval`), so a node that is
  killed hard can come back missing the last few milliseconds of writes it had
  already acknowledged. `wal_sync_on_write: true` closes the window at the cost of
  an fsync per write. With replication the usual answer is neither: another
  replica has the write, and read repair is what puts it back.
  """

  @behaviour Kurwa.Store.Engine

  alias Kurwa.Clock
  alias Kurwa.Record
  alias Kurwa.Store.Wal

  require Logger

  defstruct [:name, :table, :live, :wal, :dir, :snapshot_after, sync_on_write: false]

  @type t :: %__MODULE__{}

  @impl true
  def open(name, opts) do
    dir = Keyword.fetch!(opts, :dir)
    snapshot_after = Keyword.get(opts, :snapshot_after, 100_000)
    sync_on_write = Keyword.get(opts, :sync_on_write, false)

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
              snapshot_after: snapshot_after,
              sync_on_write: sync_on_write
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
          # Without this the write is durable only as far as the next periodic
          # fsync, so a hard crash can lose the window. Costs an fsync per write.
          if state.sync_on_write, do: Wal.sync(wal)
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
    # Two kinds of dead record, both kept for the same grace period so a replica
    # that was away cannot resurrect them: tombstones, and keys that outlived
    # their expiry. Dropping either is not logged - a replay can bring it back
    # from the WAL, which is harmless, and it goes for good at the next
    # compaction.
    tombstones = [{{:_, :_, :_, false, :"$1", :_}, [{:<, :"$1", cutoff}], [true]}]

    # `:never` needs no guard of its own: an atom never compares below an integer.
    expired = [{{:_, :_, :_, true, :_, :"$1"}, [{:<, :"$1", cutoff}], [true]}]

    dropped_tombstones = :ets.select_delete(state.table, tombstones)
    dropped_expired = :ets.select_delete(state.table, expired)

    # An expired key was still counted as live, because nothing happened to it
    # when it expired. The sweep is where the counter catches up.
    if dropped_expired > 0, do: :counters.sub(state.live, 1, dropped_expired)

    {dropped_tombstones + dropped_expired, state}
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
