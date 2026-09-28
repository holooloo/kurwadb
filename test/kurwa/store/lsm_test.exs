defmodule Kurwa.Store.LsmTest do
  use ExUnit.Case, async: true

  import Kurwa.TestHelpers, only: [tmp_dir: 1]

  alias Kurwa.Record
  alias Kurwa.Store.Lsm

  setup do
    dir = tmp_dir("lsm")
    name = :"lsm_#{System.unique_integer([:positive])}"
    {:ok, state} = Lsm.open(name, dir: dir, memtable_keys: 50, max_tables: 3)

    on_exit(fn -> if :ets.whereis(Lsm.table_name(name)) != :undefined, do: Lsm.close(state) end)

    {:ok, dir: dir, name: name, state: state}
  end

  defp put!(state, key, lamport, opts \\ []) do
    record =
      Record.new(
        key,
        lamport,
        node(),
        Keyword.get(opts, :alive?, true),
        nil,
        Keyword.get(opts, :expires_at, :never)
      )

    {_result, _winner, state} = Lsm.put(state, record)
    state
  end

  defp fill(state, range, lamport \\ 1) do
    Enum.reduce(range, state, fn i, state -> put!(state, "k:#{pad(i)}", lamport) end)
  end

  defp pad(i), do: String.pad_leading(Integer.to_string(i), 6, "0")

  test "stores and reads a key", %{name: name, state: state} do
    state = put!(state, "a", 1)

    assert Record.key(Lsm.get(Lsm.handle(name), "a")) == "a"
    assert Lsm.get(Lsm.handle(name), "nope") == nil
    assert Lsm.count(Lsm.handle(name)) == 1

    Lsm.close(state)
  end

  test "keys survive being flushed to disk", %{name: name, state: state} do
    state = fill(state, 1..120)
    state = Lsm.maybe_compact(state)

    assert Lsm.tables(state) != [], "crossing the memtable limit should write a table"

    for i <- 1..120 do
      assert Lsm.get(Lsm.handle(name), "k:#{pad(i)}") != nil, "lost k:#{pad(i)} in the flush"
    end

    Lsm.close(state)
  end

  test "a stale write arriving after a flush does not win", %{name: name, state: state} do
    # The subtle one. After a flush the memtable is empty, so an older record
    # for a flushed key looks new to it - which is why reads merge instead of
    # taking the first hit.
    state = put!(state, "contested", 9)
    state = fill(state, 1..80)
    state = Lsm.maybe_compact(state)

    state = put!(state, "contested", 3)

    winner = Lsm.get(Lsm.handle(name), "contested")
    assert Record.lamport(winner) == 9

    Lsm.close(state)
  end

  test "a newer write after a flush does win", %{name: name, state: state} do
    state = put!(state, "moving", 2)
    state = fill(state, 1..80)
    state = Lsm.maybe_compact(state)

    state = put!(state, "moving", 20, alive?: false)

    winner = Lsm.get(Lsm.handle(name), "moving")
    assert Record.lamport(winner) == 20
    refute Record.alive?(winner)

    Lsm.close(state)
  end

  test "many flushes merge into fewer tables", %{name: name, state: state} do
    state =
      Enum.reduce(1..8, state, fn round, state ->
        state
        |> fill((round * 100)..(round * 100 + 60))
        |> Lsm.maybe_compact()
      end)

    assert length(Lsm.tables(state)) <= 4, "tables should be merged, not accumulated"

    # and nothing was lost on the way
    for round <- 1..8, i <- [round * 100, round * 100 + 60] do
      assert Lsm.get(Lsm.handle(name), "k:#{pad(i)}") != nil
    end

    Lsm.close(state)
  end

  test "survives close and reopen, from tables and from the log", %{
    dir: dir,
    name: name,
    state: state
  } do
    state = fill(state, 1..120)
    state = Lsm.maybe_compact(state)
    # these stay in the memtable, so they can only come back from the WAL
    state = fill(state, 500..510)

    :ok = Lsm.close(state)

    {:ok, state} = Lsm.open(name, dir: dir, memtable_keys: 50)

    assert Lsm.get(Lsm.handle(name), "k:#{pad(1)}") != nil, "lost a flushed key"

    assert Lsm.get(Lsm.handle(name), "k:#{pad(505)}") != nil,
           "lost a key that was only in the log"

    Lsm.close(state)
  end

  test "fold yields each key once, with the winning version", %{name: name, state: state} do
    state = put!(state, "dup", 1)
    state = fill(state, 1..80)
    state = Lsm.maybe_compact(state)
    state = put!(state, "dup", 7)

    records = Lsm.fold(Lsm.handle(name), [], fn record, acc -> [record | acc] end)
    keys = Enum.map(records, &Record.key/1)

    assert length(keys) == length(Enum.uniq(keys)), "a key must appear once in a fold"
    assert Enum.find(records, &(Record.key(&1) == "dup")) |> Record.lamport() == 7

    Lsm.close(state)
  end

  test "count is an over-estimate that tightens after compaction", %{name: name, state: state} do
    state = fill(state, 1..60)
    state = Lsm.maybe_compact(state)
    # rewriting the same keys puts a second copy of each in the memtable
    state = fill(state, 1..60, 5)

    inflated = Lsm.count(Lsm.handle(name))
    assert inflated > 60, "the same key in a table and the memtable is counted twice"

    state = Lsm.compact(state)
    assert Lsm.count(Lsm.handle(name)) == 60

    Lsm.close(state)
  end

  test "tombstones and expiry survive a flush and are swept at compaction", %{
    name: name,
    state: state
  } do
    past = System.system_time(:millisecond) - 60_000

    state = put!(state, "gone", 1, alive?: false)
    state = put!(state, "stale", 1, expires_at: past)
    state = put!(state, "kept", 1)
    state = fill(state, 1..80)
    state = Lsm.maybe_compact(state)

    refute Record.member?(Lsm.get(Lsm.handle(name), "gone"))
    refute Record.member?(Lsm.get(Lsm.handle(name), "stale"))
    assert Record.member?(Lsm.get(Lsm.handle(name), "kept"))

    Lsm.close(state)
  end

  test "gc sweeps the memtable", %{name: name, state: state} do
    state = put!(state, "dead", 1, alive?: false)
    state = put!(state, "alive", 1)

    assert {dropped, state} = Lsm.gc(state, System.system_time(:millisecond) + 1_000)
    assert dropped >= 1
    assert Lsm.get(Lsm.handle(name), "dead") == nil
    assert Lsm.get(Lsm.handle(name), "alive") != nil

    Lsm.close(state)
  end

  test "thousands of keys round-trip through repeated flushes", %{name: name, state: state} do
    state =
      Enum.reduce(1..2_000, state, fn i, state ->
        state |> put!("bulk:#{pad(i)}", i) |> Lsm.maybe_compact()
      end)

    missing = Enum.reject(1..2_000, &(Lsm.get(Lsm.handle(name), "bulk:#{pad(&1)}") != nil))
    assert missing == [], "lost #{length(missing)} keys across flushes"

    Lsm.close(state)
  end
end
