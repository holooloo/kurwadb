defmodule Kurwa.CoordinatorTest do
  # Not async: these go through the node's real shards.
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [unique_key: 1]

  alias Kurwa.Coordinator

  test "a key that was never added is not a member" do
    assert Coordinator.member?(unique_key("absent")) == {:ok, false}
  end

  test "add then member then delete" do
    key = unique_key("basic")

    assert Coordinator.add(key) == :ok
    assert Coordinator.member?(key) == {:ok, true}
    assert Coordinator.delete(key) == :ok
    assert Coordinator.member?(key) == {:ok, false}
  end

  test "add is idempotent" do
    key = unique_key("idem")

    assert Coordinator.add(key) == :ok
    assert Coordinator.add(key) == :ok
    assert Coordinator.member?(key) == {:ok, true}
  end

  test "delete of an unknown key is not an error" do
    assert Coordinator.delete(unique_key("ghost")) == :ok
  end

  test "a deleted key can be added back" do
    key = unique_key("resurrect")

    :ok = Coordinator.add(key)
    :ok = Coordinator.delete(key)
    :ok = Coordinator.add(key)

    assert Coordinator.member?(key) == {:ok, true}
  end

  test "binary keys that are not text work too" do
    key = <<0, 255, 10, 0>>

    assert Coordinator.add(key) == :ok
    assert Coordinator.member?(key) == {:ok, true}
    assert Coordinator.delete(key) == :ok
  end

  test "keys are independent" do
    a = unique_key("a")
    b = unique_key("b")

    :ok = Coordinator.add(a)

    assert Coordinator.member?(a) == {:ok, true}
    assert Coordinator.member?(b) == {:ok, false}
  end

  test "count grows with adds and shrinks with deletes" do
    key = unique_key("counted")

    {:ok, before} = Coordinator.count()
    :ok = Coordinator.add(key)
    {:ok, after_add} = Coordinator.count()
    :ok = Coordinator.delete(key)
    {:ok, after_delete} = Coordinator.count()

    assert after_add.approximate == before.approximate + 1
    assert after_delete.approximate == before.approximate
    assert after_add.replicas == 1
    assert after_add.unreachable == %{}
  end

  test "a write that cannot reach enough replicas fails instead of pretending" do
    # One node in the ring, so three acks can never happen. Without
    # strict_quorum the request would be served by the single replica; with it,
    # kurwadb refuses rather than quietly lowering the durability it promised.
    assert {:error, {:quorum_not_met, details}} =
             with_strict_quorum(fn -> Coordinator.add(unique_key("strict-write"), w: 3) end)

    assert details.op == :write
    assert details.needed == 3
    assert details.got == 1
  end

  test "a read that cannot reach a quorum errors instead of answering false" do
    key = unique_key("strict-read")
    :ok = Coordinator.add(key)

    assert {:error, {:quorum_not_met, details}} =
             with_strict_quorum(fn -> Coordinator.member?(key, r: 3) end)

    assert details.op == :read
    assert details.needed == 3
  end

  test "the same request succeeds once the quorum is allowed to shrink" do
    key = unique_key("lenient")

    assert Coordinator.add(key, w: 3) == :ok
    assert Coordinator.member?(key, r: 3) == {:ok, true}
  end

  defp with_strict_quorum(fun) do
    original = Application.get_env(:kurwadb, :strict_quorum)
    Kurwa.Config.put(:strict_quorum, true)

    try do
      fun.()
    after
      Kurwa.Config.put(:strict_quorum, original)
    end
  end
end
