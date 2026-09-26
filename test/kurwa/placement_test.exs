defmodule Kurwa.PlacementTest do
  use ExUnit.Case, async: true

  alias Kurwa.Placement
  alias Kurwa.Ring

  @nodes [:a@host, :b@host, :c@host, :d@host]

  setup do
    {:ok, ring: Ring.new(@nodes, 64)}
  end

  test "all replicas reachable: nothing to hint", %{ring: ring} do
    placement = Placement.targets("k", 3, ring, MapSet.new(@nodes))

    assert length(placement.primaries) == 3
    assert placement.up == placement.primaries
    assert placement.down == []
  end

  test "an unreachable replica moves from up to down, and stays a primary", %{ring: ring} do
    primaries = Ring.preflist(ring, "k", 3)
    absent = hd(primaries)

    placement = Placement.targets("k", 3, ring, MapSet.new(@nodes) |> MapSet.delete(absent))

    assert placement.primaries == primaries
    assert placement.down == [absent]
    assert placement.up == tl(primaries)
  end

  test "placement does not move when a node goes down", %{ring: ring} do
    keys = for i <- 1..500, do: "key-#{i}"
    all_up = Placement.targets("x", 3, ring, MapSet.new(@nodes))

    degraded = MapSet.new(@nodes) |> MapSet.delete(:c@host)

    # This is the whole point of keeping down nodes in the ring: the replica set
    # is identical, only its reachability changed. A ring rebuilt from the
    # reachable nodes alone would reassign roughly a quarter of the keyspace.
    for key <- keys do
      stable = Placement.targets(key, 3, ring, MapSet.new(@nodes))
      partial = Placement.targets(key, 3, ring, degraded)

      assert stable.primaries == partial.primaries
    end

    assert all_up.down == []
  end

  test "everything down leaves nowhere to write and everything to hint", %{ring: ring} do
    placement = Placement.targets("k", 3, ring, MapSet.new())

    assert placement.up == []
    assert placement.down == placement.primaries
  end

  test "an empty ring has no placement at all" do
    placement = Placement.targets("k", 3, Ring.new([]), MapSet.new())

    assert placement == %{primaries: [], up: [], down: []}
  end

  test "against the live single-node ring, this node is the only replica" do
    placement = Placement.targets("k")

    assert placement.primaries == [node()]
    assert placement.up == [node()]
    assert placement.down == []
  end
end
