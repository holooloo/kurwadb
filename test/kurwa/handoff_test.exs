defmodule Kurwa.HandoffTest do
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [unique_key: 1]

  alias Kurwa.Handoff
  alias Kurwa.Key
  alias Kurwa.Record
  alias Kurwa.Store

  setup do
    original = Application.get_env(:kurwadb, :handoff_max_hints)
    on_exit(fn -> Kurwa.Config.put(:handoff_max_hints, original) end)
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
    Kurwa.Config.put(:handoff_max_hints, 2)

    for _ <- 1..5, do: Handoff.store(absent, record(unique_key("bounded")))

    assert Handoff.depth()[absent] == 2
  end

  test "an existing key can still be updated when the queue is full", %{absent: absent} do
    key = unique_key("update-when-full")
    :ok = Handoff.store(absent, record(key, lamport: 1))

    Kurwa.Config.put(:handoff_max_hints, 1)

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

  describe "durability" do
    test "a hint key round-trips, carrying the original's liveness", %{absent: absent} do
      key = Key.encode(unique_key("rt"))

      for alive? <- [true, false] do
        hint = Key.hint_key(absent, key, alive?)
        assert Key.hint_parts(hint) == {:ok, absent, key, alive?}
        assert Key.system?(hint)
        assert Key.local_only?(hint), "hints belong to this node alone"
      end

      refute Key.local_only?(Key.registry_key("alpha")), "registry entries do replicate"
      assert Key.hint_parts(Key.registry_key("alpha")) == :error
    end

    test "hints survive the process that was holding them", %{absent: absent} do
      :ok = Handoff.store(absent, record(unique_key("durable")))
      :ok = Handoff.store(absent, record(unique_key("durable")))
      assert Handoff.depth()[absent] == 2

      restart_handoff()

      assert Handoff.depth()[absent] == 2, "hints were only in memory"
    end

    test "delivered hints do not come back", %{absent: _absent} do
      key = unique_key("delivered")
      :ok = Handoff.store(node(), Record.new(Key.encode(key), 1, node(), true))
      Handoff.drain()

      restart_handoff()

      refute Map.has_key?(Handoff.depth(), node())
    end

    test "a hint for a delete replays as a delete, not as an add" do
      # The reason the original's liveness lives in the key: on the record,
      # `alive?` already means "still owed".
      key = unique_key("hinted-delete")
      storage_key = Key.encode(key)

      {:ok, _} = Store.put(Record.new(storage_key, 1, node(), true))
      assert Store.get(storage_key) |> elem(1) |> Record.alive?()

      :ok = Handoff.store(node(), Record.new(storage_key, 5, node(), false))
      restart_handoff()

      Handoff.drain()

      assert {:ok, stored} = Store.get(storage_key)
      refute Record.alive?(stored), "the replayed hint should have deleted the key"
      assert Record.lamport(stored) == 5
    end

    test "an add then a delete of one key leaves nothing behind once handed over" do
      # The original's liveness is part of the hint key, so this key has two
      # durable hints while the queue - which dedupes by the original key - only
      # hands over the newer one. Delivery has to clear both, or the older
      # variant is replayed after every restart forever.
      key = unique_key("add-then-delete")
      storage_key = Key.encode(key)

      :ok = Handoff.store(node(), Record.new(storage_key, 1, node(), true))
      :ok = Handoff.store(node(), Record.new(storage_key, 7, node(), false))
      Handoff.drain()

      restart_handoff()

      refute Map.has_key?(Handoff.depth(), node())
    end

    test "pending hints are not counted as keys anybody stored", %{absent: absent} do
      before = Store.count()
      for _ <- 1..5, do: Handoff.store(absent, record(unique_key("uncounted")))

      assert Handoff.depth()[absent] >= 5
      assert Store.count() == before, "bookkeeping is not data"
    end
  end

  # Through the supervisor, not with a kill: three deliberate crashes inside one
  # test module trip max_restarts and take the whole application down with
  # them, which shows up as most of the suite failing for no visible reason.
  defp restart_handoff do
    :ok = Supervisor.terminate_child(Kurwa.Supervisor, Kurwa.Handoff)
    {:ok, _pid} = Supervisor.restart_child(Kurwa.Supervisor, Kurwa.Handoff)
    :ok
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
