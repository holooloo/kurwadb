defmodule Kurwa.Store.Lsm do
  @moduledoc """
  A log-structured engine: recent keys in memory, older ones on disk, and only a
  Bloom filter per table kept in RAM.

  This is the engine for when the keys stop fitting in memory. `Kurwa.Store.Ets`
  holds every key in an ETS table at about 128 bytes each, which puts 100M keys
  at roughly 12 GB. Here a key on disk costs about 1.25 bytes of RAM - the bits
  of its table's Bloom filter - so the same machine holds an order of magnitude
  more.

  It is not the default. ETS is faster for anything that fits, and this engine
  is younger.

      config :kurwadb, engine: Kurwa.Store.Lsm

  ## Shape

      memtable      an ETS set, exactly like the ETS engine, plus its WAL
      tables        immutable sorted files, newest first (Kurwa.Store.SSTable)

  A write goes to the memtable and the log. When the memtable passes
  `lsm_memtable_keys` it is written out as a table and a fresh one starts. When
  the tables get numerous they are merged into one.

  ## Reads, and why they merge

  A read checks the memtable, then each table whose filter does not rule the key
  out, and merges everything it finds. Merging rather than "newest wins" is not
  caution: a write that arrives with an *older* stamp after its key was flushed
  lands in an empty memtable, so the newest place a key appears is not always
  the winning version. `Kurwa.Record.merge/2` settles it, the same way it
  settles a disagreement between replicas.

  Writes read first, for the same reason, which the Bloom filters make cheap for
  keys that are new - the common case for the jobs this store is for.

  ## Two costs worth knowing

  `count/1` adds the memtable's live count to each table's, so a key written,
  flushed, and written again is counted twice until the tables merge. It is an
  over-estimate that tightens at compaction, where the ETS engine's count is
  exact.

  Compaction and anti-entropy stream through `SSTable.merge/3` and never hold a
  table in memory, but they do read every table end to end.
  """

  @behaviour Kurwa.Store.Engine

  alias Kurwa.Clock
  alias Kurwa.Record
  alias Kurwa.Store.SSTable
  alias Kurwa.Store.Wal

  require Logger

  defstruct [
    :name,
    :memtable,
    :live,
    :wal,
    :dir,
    :tables,
    :generation,
    :memtable_keys,
    :max_tables,
    sync_on_write: false
  ]

  @extension ".sst"

  @impl true
  def open(name, opts) do
    dir = Keyword.fetch!(opts, :dir)
    :ok = File.mkdir_p(dir)

    memtable =
      :ets.new(table_name(name), [:set, :protected, :named_table, read_concurrency: true])

    live = :counters.new(1, [:atomics])
    tables = open_tables(dir)

    replay = fn record, count ->
      restore(memtable, live, record)
      count + 1
    end

    with {:ok, _n, _from_wal} <- Wal.replay(dir, 0, replay),
         {:ok, wal} <- Wal.open(dir) do
      state = %__MODULE__{
        name: name,
        memtable: table_name(name),
        live: live,
        wal: wal,
        dir: dir,
        tables: tables,
        generation: next_generation(tables),
        memtable_keys: Keyword.get(opts, :memtable_keys, 100_000),
        max_tables: Keyword.get(opts, :max_tables, 8),
        sync_on_write: Keyword.get(opts, :sync_on_write, false)
      }

      publish(state)
      {:ok, state}
    end
  end

  @impl true
  def close(%__MODULE__{} = state) do
    Wal.close(state.wal)
    Enum.each(state.tables, &SSTable.close/1)
    :persistent_term.erase(pt_key(state.name))
    if :ets.whereis(state.memtable) != :undefined, do: :ets.delete(state.memtable)
    :ok
  end

  @impl true
  def handle(name), do: :persistent_term.get(pt_key(name))

  @impl true
  def get(%{memtable: memtable, tables: tables}, key) when is_binary(key) do
    found =
      Enum.reduce(tables, lookup(memtable, key), fn table, winner ->
        case SSTable.get(table, key) do
          nil -> winner
          record -> Record.merge(winner, record)
        end
      end)

    found
  end

  @impl true
  def count(%{live: live, tables: tables}) do
    Enum.reduce(tables, :counters.get(live, 1), fn table, sum ->
      sum + SSTable.stats(table).live
    end)
  end

  @impl true
  def fold(%{memtable: memtable, tables: tables}, acc, fun) do
    readers = [SSTable.list_reader(:ets.tab2list(memtable)) | Enum.map(tables, &SSTable.reader/1)]
    SSTable.merge(readers, acc, fun)
  end

  @impl true
  def put(%__MODULE__{} = state, record) do
    Clock.observe(Record.lamport(record))
    key = Record.key(record)
    current = get(handle_of(state), key)

    # Read what the memtable held *before* inserting, or the counter compares
    # the new record against itself and never moves.
    previous = lookup(state.memtable, key)

    if Record.newer?(record, current) do
      true = :ets.insert(state.memtable, record)
      adjust_live(state.live, previous, current, record)

      case Wal.append(state.wal, record) do
        {:ok, wal} ->
          state = %{state | wal: wal}
          if state.sync_on_write, do: Wal.sync(wal)
          {:ok, record, state}

        {:error, reason} ->
          Logger.error("kurwadb: WAL append failed in #{state.dir}: #{inspect(reason)}")
          {:ok, record, state}
      end
    else
      {:stale, current, state}
    end
  end

  @impl true
  def sync(%__MODULE__{wal: wal}) do
    _ = Wal.sync(wal)
    :ok
  end

  @impl true
  def gc(%__MODULE__{} = state, cutoff) do
    # The memtable can be swept in place. Tables are immutable, so the dead
    # records in them go at the next compaction, which is where the merge
    # already has to look at every record anyway.
    tombstones = [{{:_, :_, :_, false, :"$1", :_}, [{:<, :"$1", cutoff}], [true]}]
    expired = [{{:_, :_, :_, true, :_, :"$1"}, [{:<, :"$1", cutoff}], [true]}]

    dropped_tombstones = :ets.select_delete(state.memtable, tombstones)
    dropped_expired = :ets.select_delete(state.memtable, expired)
    if dropped_expired > 0, do: :counters.sub(state.live, 1, dropped_expired)

    {dropped_tombstones + dropped_expired, state}
  end

  @impl true
  def maybe_compact(%__MODULE__{} = state) do
    state =
      if :ets.info(state.memtable, :size) >= state.memtable_keys, do: flush(state), else: state

    if length(state.tables) > state.max_tables, do: merge_tables(state), else: state
  end

  @impl true
  def compact(%__MODULE__{} = state) do
    state |> flush() |> merge_tables()
  end

  @doc "ETS table name for the memtable of an engine instance."
  def table_name(name), do: :"#{name}_memtable"

  @doc "Tables currently open, newest first. For tests and introspection."
  def tables(%__MODULE__{tables: tables}), do: tables

  # ------------------------------------------------------------------ private

  defp flush(%__MODULE__{} = state) do
    records = :ets.tab2list(state.memtable)

    if records == [] do
      state
    else
      path = path_for(state.dir, state.generation)

      case SSTable.write(path, records) do
        {:ok, count} ->
          {:ok, table} = SSTable.open(path)

          # Order matters: the table is on disk and fsynced before the log that
          # described it is thrown away.
          {:ok, wal} = Wal.compact(state.wal, [])
          :ets.delete_all_objects(state.memtable)
          :counters.put(state.live, 1, 0)

          Logger.info("kurwadb: flushed #{count} records to #{Path.basename(path)}")

          state = %{
            state
            | wal: wal,
              tables: [table | state.tables],
              generation: state.generation + 1
          }

          publish(state)
          state

        {:error, reason} ->
          Logger.error("kurwadb: flush of #{state.dir} failed: #{inspect(reason)}")
          state
      end
    end
  end

  defp merge_tables(%__MODULE__{tables: tables} = state) when length(tables) < 2, do: state

  defp merge_tables(%__MODULE__{} = state) do
    path = path_for(state.dir, state.generation)
    capacity = Enum.reduce(state.tables, 1, &(&2 + SSTable.stats(&1).records))
    readers = Enum.map(state.tables, &SSTable.reader/1)

    # Merge pushes records at a fold, so the writer is the fold: nothing is
    # collected, and a table larger than memory still compacts.
    result =
      with {:ok, writer} <- SSTable.writer_open(path, capacity) do
        readers
        |> SSTable.merge(writer, fn record, writer -> SSTable.writer_add(writer, record) end)
        |> SSTable.writer_close()
      end

    case result do
      {:ok, count} ->
        {:ok, table} = SSTable.open(path)
        old = state.tables
        Enum.each(old, &SSTable.close/1)
        Enum.each(old, fn t -> File.rm(t.path) end)

        Logger.info(
          "kurwadb: merged #{length(old)} tables into #{Path.basename(path)} (#{count})"
        )

        state = %{state | tables: [table], generation: state.generation + 1}
        publish(state)
        state

      {:error, reason} ->
        Logger.error("kurwadb: merge in #{state.dir} failed: #{inspect(reason)}")
        state
    end
  end

  defp open_tables(dir) do
    dir
    |> File.ls!()
    |> Enum.filter(&String.ends_with?(&1, @extension))
    |> Enum.sort(:desc)
    |> Enum.flat_map(fn file ->
      path = Path.join(dir, file)

      case SSTable.open(path) do
        {:ok, table} ->
          [table]

        {:error, reason} ->
          Logger.warning("kurwadb: ignoring unreadable table #{file}: #{inspect(reason)}")
          []
      end
    end)
  end

  defp next_generation([]), do: 0

  defp next_generation(tables) do
    tables
    |> Enum.map(&(&1.path |> Path.basename(@extension) |> String.to_integer()))
    |> Enum.max()
    |> Kernel.+(1)
  end

  defp path_for(dir, generation) do
    Path.join(dir, String.pad_leading(Integer.to_string(generation), 10, "0") <> @extension)
  end

  defp lookup(memtable, key) do
    case :ets.lookup(memtable, key) do
      [record] -> record
      [] -> nil
    end
  end

  defp handle_of(%__MODULE__{} = state),
    do: %{memtable: state.memtable, live: state.live, tables: state.tables}

  defp publish(state), do: :persistent_term.put(pt_key(state.name), handle_of(state))

  defp pt_key(name), do: {__MODULE__, name}

  # `previous` is what the memtable held, `current` what the whole engine held.
  # The counter tracks the memtable alone, so only a change inside it counts.
  defp adjust_live(live, previous_in_memtable, _current, new) do
    was = Record.member?(previous_in_memtable)
    now = Record.member?(new)

    cond do
      not was and now -> :counters.add(live, 1, 1)
      was and not now -> :counters.sub(live, 1, 1)
      true -> :ok
    end
  end

  defp restore(memtable, live, record) do
    Clock.observe(Record.lamport(record))
    key = Record.key(record)
    current = lookup(memtable, key)

    if Record.newer?(record, current) do
      true = :ets.insert(memtable, record)
      adjust_live(live, current, current, record)
    end

    :ok
  end
end
