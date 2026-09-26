defmodule Kurwa.RingTest do
  use ExUnit.Case, async: true

  alias Kurwa.Ring

  @nodes [:a@host, :b@host, :c@host, :d@host]

  test "an empty ring has no owners" do
    ring = Ring.new([])

    assert Ring.preflist(ring, "anything", 3) == []
    assert Ring.owner(ring, "anything") == nil
  end

  test "is deterministic - node order in the input does not matter" do
    a = Ring.new(@nodes, 16)
    b = Ring.new(Enum.reverse(@nodes), 16)

    assert a.points == b.points

    for key <- keys(200) do
      assert Ring.preflist(a, key, 3) == Ring.preflist(b, key, 3)
    end
  end

  test "preflist returns n distinct nodes in a stable order" do
    ring = Ring.new(@nodes, 64)

    for key <- keys(200) do
      prefs = Ring.preflist(ring, key, 3)

      assert length(prefs) == 3
      assert Enum.uniq(prefs) == prefs
      assert Enum.all?(prefs, &(&1 in @nodes))
      assert hd(prefs) == Ring.owner(ring, key)
    end
  end

  test "never returns more nodes than the cluster has" do
    ring = Ring.new([:a@host], 8)

    assert Ring.preflist(ring, "k", 3) == [:a@host]
  end

  test "spreads keys roughly evenly" do
    ring = Ring.new(@nodes, 256)
    keys = keys(4_000)

    counts = Enum.frequencies_by(keys, &Ring.owner(ring, &1))

    assert map_size(counts) == length(@nodes)

    average = div(length(keys), length(@nodes))

    for {node, count} <- counts do
      assert_in_delta count,
                      average,
                      average * 0.35,
                      "node #{node} owns #{count} of #{length(keys)}"
    end
  end

  test "adding a node moves only its share of the keyspace" do
    before = Ring.new(@nodes, 256)
    after_join = Ring.new([:e@host | @nodes], 256)
    keys = keys(4_000)

    moved = Enum.count(keys, &(Ring.owner(before, &1) != Ring.owner(after_join, &1)))

    # With 5 nodes the newcomer should claim ~1/5 of the keys; everything else
    # must stay put. A naive `hash |> rem(node_count)` would move ~80% here.
    assert moved > 0
    assert moved < length(keys) * 0.35, "#{moved} of #{length(keys)} keys moved"
  end

  test "removing a node only reassigns the keys it owned" do
    full = Ring.new(@nodes, 256)
    reduced = Ring.new(@nodes -- [:d@host], 256)
    keys = keys(2_000)

    {owned_by_d, others} = Enum.split_with(keys, &(Ring.owner(full, &1) == :d@host))

    assert Enum.all?(others, &(Ring.owner(full, &1) == Ring.owner(reduced, &1)))
    assert Enum.all?(owned_by_d, &(Ring.owner(reduced, &1) != :d@host))
  end

  test "hash is stable and 64 bit" do
    assert Ring.hash("x") == Ring.hash("x")
    assert Ring.hash("x") != Ring.hash("y")
    assert Ring.hash("x") < Bitwise.bsl(1, 64)
  end

  defp keys(n), do: for(i <- 1..n, do: "key-#{i}")
end
