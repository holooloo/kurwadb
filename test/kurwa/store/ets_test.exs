defmodule Kurwa.Store.EtsTest do
  use ExUnit.Case, async: true

  import Kurwa.TestHelpers, only: [tmp_dir: 1, record: 2]

  alias Kurwa.Store.Ets

  setup context do
    dir = tmp_dir("ets")
    name = :"ets_test_#{System.unique_integer([:positive])}"
    {:ok, state} = Ets.open(name, dir: dir, snapshot_after: 5)

    on_exit(fn -> if :ets.whereis(Ets.table_name(name)) != :undefined, do: Ets.close(state) end)

    {:ok, dir: dir, name: name, state: state, test_name: context.test}
  end

  test "a new key is stored and readable", %{name: name, state: state} do
    rec = record("a", lamport: 1)
    assert {:ok, ^rec, state} = Ets.put(state, rec)

    assert Ets.get(Ets.handle(name), "a") == rec
    assert Ets.count(Ets.handle(name)) == 1

    Ets.close(state)
  end

  test "an unknown key reads as nil", %{name: name} do
    assert Ets.get(Ets.handle(name), "nope") == nil
  end

  test "an older write is rejected and the newer record kept", %{state: state} do
    newer = record("a", lamport: 5)
    older = record("a", lamport: 2)

    {:ok, _, state} = Ets.put(state, newer)
    assert {:stale, ^newer, _state} = Ets.put(state, older)
  end

  test "the same write twice is idempotent", %{name: name, state: state} do
    rec = record("a", lamport: 1)

    {:ok, _, state} = Ets.put(state, rec)
    {result, _, _state} = Ets.put(state, rec)

    assert result == :stale
    assert Ets.count(Ets.handle(name)) == 1
  end

  test "tombstones are stored but not counted", %{name: name, state: state} do
    {:ok, _, state} = Ets.put(state, record("a", lamport: 1))
    assert Ets.count(Ets.handle(name)) == 1

    {:ok, _, state} = Ets.put(state, record("a", lamport: 2, alive?: false))

    assert Ets.count(Ets.handle(name)) == 0
    refute Ets.get(Ets.handle(name), "a") == nil
    assert Kurwa.Record.alive?(Ets.get(Ets.handle(name), "a")) == false

    # resurrect
    {:ok, _, _state} = Ets.put(state, record("a", lamport: 3))
    assert Ets.count(Ets.handle(name)) == 1
  end

  test "gc drops only tombstones older than the cutoff", %{name: name, state: state} do
    {:ok, _, state} = Ets.put(state, record("live", lamport: 1, wall: 5_000))
    {:ok, _, state} = Ets.put(state, record("old", lamport: 1, alive?: false, wall: 1_000))
    {:ok, _, state} = Ets.put(state, record("fresh", lamport: 1, alive?: false, wall: 9_000))

    assert {2, _state} = Ets.gc(state, 10_000)
    assert Ets.get(Ets.handle(name), "old") == nil
    assert Ets.get(Ets.handle(name), "fresh") == nil

    {:ok, _, state} = Ets.put(state, record("old", lamport: 2, alive?: false, wall: 1_000))
    assert {1, state} = Ets.gc(state, 5_000)
    assert Ets.get(Ets.handle(name), "old") == nil
    refute Ets.get(Ets.handle(name), "live") == nil
    assert Ets.count(Ets.handle(name)) == 1

    Ets.close(state)
  end

  test "survives a close and reopen", %{dir: dir, name: name, state: state} do
    state =
      Enum.reduce(1..3, state, fn i, state ->
        {:ok, _, state} = Ets.put(state, record("k#{i}", lamport: i))
        state
      end)

    {:ok, _, state} = Ets.put(state, record("k2", lamport: 10, alive?: false))
    :ok = Ets.close(state)

    {:ok, state} = Ets.open(name, dir: dir)
    handle = Ets.handle(name)

    assert Ets.count(handle) == 2
    assert Kurwa.Record.lamport(Ets.get(handle, "k2")) == 10
    refute Kurwa.Record.alive?(Ets.get(handle, "k2"))

    Ets.close(state)
  end

  test "compaction keeps the data and resets the log", %{dir: dir, name: name, state: state} do
    state =
      Enum.reduce(1..10, state, fn i, state ->
        {:ok, _, state} = Ets.put(state, record("k#{i}", lamport: i))
        state
      end)

    state = Ets.compact(state)
    assert state.wal.appended == 0
    assert File.exists?(Path.join(dir, "snapshot"))

    :ok = Ets.close(state)
    {:ok, state} = Ets.open(name, dir: dir)

    assert Ets.count(Ets.handle(name)) == 10
    Ets.close(state)
  end

  test "maybe_compact fires once the threshold is crossed", %{state: state} do
    # snapshot_after: 5 from setup
    state =
      Enum.reduce(1..4, state, fn i, state ->
        {:ok, _, state} = Ets.put(state, record("k#{i}", lamport: i))
        state
      end)

    assert Ets.maybe_compact(state).wal.appended == 4

    {:ok, _, state} = Ets.put(state, record("k5", lamport: 5))
    assert Ets.maybe_compact(state).wal.appended == 0
  end

  test "the clock catches up with what was replayed", %{dir: dir, name: name, state: state} do
    high = Kurwa.Clock.peek() + 5_000

    {:ok, _, state} = Ets.put(state, record("a", lamport: high))
    :ok = Ets.close(state)

    {:ok, state} = Ets.open(name, dir: dir)

    assert Kurwa.Clock.peek() >= high
    assert Kurwa.Clock.tick() > high

    Ets.close(state)
  end

  test "sync_on_write puts the record on disk before any periodic flush", %{dir: dir} do
    name = :"ets_sync_#{System.unique_integer([:positive])}"
    {:ok, state} = Ets.open(name, dir: Path.join(dir, "synced"), sync_on_write: true)

    {:ok, _, state} = Ets.put(state, record("durable", lamport: 1))

    # Read the log back while the engine is still open and has flushed nothing on
    # a timer: with sync_on_write the entry is already durable.
    assert {:ok, [{"durable", 1, _, true, _}], 1} =
             Kurwa.Store.Wal.replay(Path.join(dir, "synced"), [], fn rec, acc -> [rec | acc] end)

    Ets.close(state)
  end

  test "fold visits every record, tombstones included", %{name: name, state: state} do
    {:ok, _, state} = Ets.put(state, record("a", lamport: 1))
    {:ok, _, _state} = Ets.put(state, record("b", lamport: 1, alive?: false))

    keys = Ets.fold(Ets.handle(name), [], fn {key, _, _, _, _}, acc -> [key | acc] end)

    assert Enum.sort(keys) == ["a", "b"]
  end
end
