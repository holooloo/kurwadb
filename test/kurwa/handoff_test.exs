defmodule Kurwa.HandoffTest do
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [unique_key: 1]

  alias Kurwa.Handoff
  alias Kurwa.Key
  alias Kurwa.Record
  alias Kurwa.Store

  setup do
    original = Application.get_env(:kurwadb, :handoff_max_hints)
    on_exit(fn -> Application.put_env(:kurwadb, :handoff_max_hints, original) end)
    {:ok, absent: :"absent#{System.unique_integer([:positive])}@nowhere"}
  end

  test "hints for an unreachable node are kept", %{absent: absent} do
    :ok = Handoff.store(absent, record(unique_key("hint")))
    :ok = Handoff.store(absent, record(unique_key("hint")))

    assert Handoff.depth()[absent] == 2
  end

  test "hints for the same key collapse to the newest version", %{absent: absent} do
    key = unique_key("collapse")

    :ok = Handoff.store(absent, record(key, lamport: 1))
    :ok = Handoff.store(absent, record(key, lamport: 9, alive?: false))
    :ok = Handoff.store(absent, record(key, lamport: 4))

    assert Handoff.depth()[absent] == 1
  end

  test "a full queue refuses new hints instead of growing without bound", %{absent: absent} do
    Application.put_env(:kurwadb, :handoff_max_hints, 2)

    for _ <- 1..5, do: Handoff.store(absent, record(unique_key("bounded")))

    assert Handoff.depth()[absent] == 2
  end

  test "an existing key can still be updated when the queue is full", %{absent: absent} do
    key = unique_key("update-when-full")
    :ok = Handoff.store(absent, record(key, lamport: 1))

    Application.put_env(:kurwadb, :handoff_max_hints, 1)

    :ok = Handoff.store(absent, record(key, lamport: 5, alive?: false))
    :ok = Handoff.store(absent, record(unique_key("rejected")))

    assert Handoff.depth()[absent] == 1
  end

  test "a reachable target gets its hints replayed and the queue empties" do
    key = unique_key("replay")
    storage_key = Key.encode(key)

    :ok = Handoff.store(node(), Record.new(storage_key, 1, node(), true))
    assert Handoff.depth()[node()] == 1

    left = Handoff.drain()

    refute Map.has_key?(left, node())
    assert {:ok, stored} = Store.get(storage_key)
    assert Record.alive?(stored)
  end

  test "replay applies the newest version, not the first one queued" do
    key = unique_key("replay-newest")
    storage_key = Key.encode(key)

    :ok = Handoff.store(node(), Record.new(storage_key, 1, node(), true))
    :ok = Handoff.store(node(), Record.new(storage_key, 7, node(), false))

    Handoff.drain()

    assert {:ok, stored} = Store.get(storage_key)
    assert Record.lamport(stored) == 7
    refute Record.alive?(stored)
  end

  test "an unreachable target keeps its hints across a drain", %{absent: absent} do
    :ok = Handoff.store(absent, record(unique_key("kept")))

    assert Handoff.drain()[absent] == 1
  end

  test "a successful write leaves a hint for the replicas that missed it" do
    # A single-node ring has nobody to miss a write, so there is nothing to hint.
    key = unique_key("no-hint")
    before = Handoff.depth()

    :ok = Kurwa.add(key)

    assert Handoff.depth() == before
  end

  defp record(key, opts \\ []) do
    Record.new(
      Key.encode(key),
      Keyword.get(opts, :lamport, 1),
      node(),
      Keyword.get(opts, :alive?, true)
    )
  end
end
