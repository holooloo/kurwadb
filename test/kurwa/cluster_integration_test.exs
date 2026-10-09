defmodule Kurwa.ClusterIntegrationTest do
  @moduledoc """
  Three real nodes, three real BEAMs, Erlang distribution between them.

  These are the guarantees the README makes about a cluster, so they are asserted
  rather than described. Excluded from `mix test` by default because each case
  boots a cluster; run them with:

      mix test --include cluster
  """

  use ExUnit.Case, async: false

  alias Kurwa.TestCluster, as: TC

  @moduletag :cluster
  @moduletag timeout: 180_000

  setup context do
    dir =
      Path.join(["tmp", "cluster-test", "#{context.test |> to_string() |> String.slice(0, 20)}"])
      |> String.replace(" ", "_")

    File.rm_rf!(dir)
    peers = TC.start(3, dir, context[:cluster_opts] || [])

    on_exit(fn ->
      Enum.each(peers, &TC.stop/1)
      File.rm_rf!(dir)
    end)

    {:ok, peers: peers, nodes: Enum.map(peers, & &1.node), dir: dir}
  end

  test "a write on one node is readable from every other node", %{peers: [one, two, three]} do
    keys = for i <- 1..10, do: "order:#{i}"

    for key <- keys, do: assert(TC.call(one, Kurwa, :add, [key]) == :ok)

    for key <- keys do
      assert TC.call(two, Kurwa, :fetch, [key]) == {:ok, true}
      assert TC.call(three, Kurwa, :fetch, [key]) == {:ok, true}
    end

    # n = 3 and the cluster is 3, so every node holds every key
    for peer <- [one, two, three], do: assert(TC.local_keys(peer) == 10)

    {:ok, count} = TC.call(one, Kurwa, :count, [])
    assert count.approximate == 10
    assert count.replicas == 3
    assert count.unreachable == %{}
  end

  test "a delete on one node is visible from the others", %{peers: [one, two, three]} do
    :ok = TC.call(one, Kurwa, :add, ["gone"])
    assert TC.call(three, Kurwa, :fetch, ["gone"]) == {:ok, true}

    assert TC.call(two, Kurwa, :delete, ["gone"]) == :ok

    assert TC.call(one, Kurwa, :fetch, ["gone"]) == {:ok, false}
    assert TC.call(three, Kurwa, :fetch, ["gone"]) == {:ok, false}
    assert TC.local_keys(three) == 0
  end

  test "named sets stay separate across the cluster", %{peers: [one, two, _three]} do
    :ok = TC.call(one, Kurwa.Namespace, :add, ["blacklist", "ip:10.0.0.1"])

    assert TC.call(two, Kurwa.Namespace, :member?, ["blacklist", "ip:10.0.0.1"]) == {:ok, true}
    assert TC.call(two, Kurwa.Namespace, :member?, ["greylist", "ip:10.0.0.1"]) == {:ok, false}
    assert TC.call(two, Kurwa, :fetch, ["ip:10.0.0.1"]) == {:ok, false}

    assert TC.call(two, Kurwa.Namespace, :member_any?, [
             ["greylist", "blacklist"],
             "ip:10.0.0.1"
           ]) == {:ok, true}
  end

  test "a key with a ttl expires at the same instant on every replica", %{
    peers: [one, two, three]
  } do
    key = "expiring"

    assert TC.call(one, Kurwa, :add, [key, [ttl: 400]]) == :ok

    # All three agree it is there...
    for peer <- [one, two, three], do: assert(TC.call(peer, Kurwa, :fetch, [key]) == {:ok, true})

    # ...and each of them holds the same absolute deadline, rather than each
    # starting its own countdown when the write arrived.
    deadlines =
      for peer <- [one, two, three] do
        {:ok, record} = TC.call(peer, Kurwa.Store, :get, [Kurwa.Key.encode(key)])
        Kurwa.Record.expires_at(record)
      end

    assert Enum.uniq(deadlines) |> length() == 1

    Process.sleep(500)

    # No write, no message between them: expiry is a pure function of the record.
    for peer <- [one, two, three], do: assert(TC.call(peer, Kurwa, :fetch, [key]) == {:ok, false})
  end

  test "a set created on one node is listed from another", %{peers: [one, two, three]} do
    set = "crossnode"

    assert TC.call(one, Kurwa.Namespace, :add, [set, "a-key"]) == :ok

    # The name is a key like any other, so it replicates without the registry
    # needing a protocol of its own.
    TC.await(fn ->
      {:ok, %{sets: sets}} = TC.call(two, Kurwa.Namespace, :list, [])
      set in sets
    end)

    {:ok, %{sets: sets, unreachable: unreachable}} = TC.call(three, Kurwa.Namespace, :list, [])
    assert set in sets
    assert unreachable == %{}

    # forgetting it on one node removes it cluster-wide, and leaves the keys
    assert TC.call(three, Kurwa.Namespace, :forget, [set]) == :ok

    TC.await(fn ->
      {:ok, %{sets: sets}} = TC.call(one, Kurwa.Namespace, :list, [])
      set not in sets
    end)

    assert TC.call(one, Kurwa.Namespace, :member?, [set, "a-key"]) == {:ok, true}
  end

  test "placement does not move when a replica becomes unreachable", %{
    peers: [one, _two, three],
    nodes: nodes
  } do
    key = Kurwa.Key.encode("placement-probe")
    before = TC.call(one, Kurwa.Placement, :targets, [key, 3])

    assert Enum.sort(before.primaries) == Enum.sort(nodes)
    assert before.down == []

    TC.stop(three)
    TC.await_reachability(one, 3, 2)

    after_outage = TC.call(one, Kurwa.Placement, :targets, [key, 3])

    # Same replica set, only its reachability changed. This is what keeps a
    # returning node responsible for the writes it missed.
    assert after_outage.primaries == before.primaries
    assert after_outage.down == [three.node]
    assert Enum.sort(after_outage.up) == Enum.sort(before.primaries -- [three.node])
  end

  test "the cluster keeps serving with one node down, and hands off what it missed", %{
    peers: [one, two, three],
    nodes: nodes,
    dir: dir
  } do
    early = for i <- 1..5, do: "early:#{i}"
    for key <- early, do: assert(TC.call(one, Kurwa, :add, [key]) == :ok)
    assert TC.local_keys(three) == 5

    # Graceful, so the WAL is flushed: this test is about handoff, not about the
    # fsync window (which "a crashed replica..." below covers).
    TC.stop(three, :graceful)
    TC.await_reachability(one, 3, 2)

    # w = 2 and two replicas are reachable, so writes still succeed
    late = for i <- 1..5, do: "late:#{i}"
    for key <- late, do: assert(TC.call(one, Kurwa, :add, [key]) == :ok)

    # reads too
    assert TC.call(two, Kurwa, :fetch, ["late:3"]) == {:ok, true}

    # and the absent replica's share is waiting for it
    assert TC.call(one, Kurwa.Handoff, :depth, [])[three.node] == 5

    revived = TC.boot(:kurwa_node3, nodes, dir)
    on_exit(fn -> TC.stop(revived) end)

    TC.await_reachability(one, 3, 3)

    # No reads against these keys. The split is already pinned down: the queue
    # held exactly 5 hints, so the other 5 can only have come from the WAL.
    # (Not asserting the intermediate 5 - handoff is fast enough to race it.)
    TC.await(fn -> TC.local_keys(revived) == 10 end, 15_000)
    TC.await(fn -> TC.call(one, Kurwa.Handoff, :depth, []) == %{} end, 15_000)

    for key <- early ++ late do
      assert TC.call(revived, Kurwa.Store, :get, [Kurwa.Key.encode(key)])
             |> then(fn {:ok, record} -> Kurwa.Record.alive?(record) end)
    end
  end

  test "a read repairs a replica that answers with something stale", %{peers: [one, two, three]} do
    key = "repairable"
    storage_key = Kurwa.Key.encode(key)

    :ok = TC.call(one, Kurwa, :add, [key])

    # Rewind one replica underneath the cluster, the way a missed delete would.
    stale = Kurwa.Record.new(storage_key, 999, three.node, false)
    assert {:ok, _} = TC.call(three, Kurwa.Store, :put, [stale])

    assert TC.call(three, Kurwa.Store, :get, [storage_key]) |> elem(1) |> Kurwa.Record.alive?() ==
             false

    # r = 3 waits for every replica, so every answer is in hand and the stale one
    # gets the winner pushed back to it.
    assert TC.call(two, Kurwa, :fetch, [key, [r: 3]]) == {:ok, false}

    :ok = TC.call(one, Kurwa, :add, [key])
    assert TC.call(two, Kurwa, :fetch, [key, [r: 3]]) == {:ok, true}

    TC.await(
      fn ->
        {:ok, record} = TC.call(three, Kurwa.Store, :get, [storage_key])
        Kurwa.Record.alive?(record)
      end,
      5_000
    )
  end

  test "hints survive the restart of the node holding them", %{
    peers: [one, two, three],
    nodes: nodes,
    dir: dir
  } do
    TC.stop(three, :graceful)
    TC.await_reachability(one, 3, 2)

    keys = for i <- 1..5, do: "owed:#{i}"
    for key <- keys, do: assert(TC.call(one, Kurwa, :add, [key]) == :ok)
    assert TC.call(one, Kurwa.Handoff, :depth, [])[three.node] == 5

    # The coordinator itself goes down, still owing those five writes.
    TC.stop(one, :graceful)
    TC.await_reachability(two, 3, 1)

    revived_one = TC.boot(:kurwa_node1, nodes, dir)
    on_exit(fn -> TC.stop(revived_one) end)
    TC.await_reachability(revived_one, 3, 2)

    # They were written to its own store, so they came back with it.
    assert TC.call(revived_one, Kurwa.Handoff, :depth, [])[three.node] == 5

    revived_three = TC.boot(:kurwa_node3, nodes, dir)
    on_exit(fn -> TC.stop(revived_three) end)
    TC.await_reachability(revived_one, 3, 3)

    TC.await(fn -> TC.local_keys(revived_three) == 5 end, 15_000)
    TC.await(fn -> TC.call(revived_one, Kurwa.Handoff, :depth, []) == %{} end, 15_000)
  end

  test "a quorum that cannot be met is an error, not a false", %{peers: [one, two, three]} do
    :ok = TC.call(one, Kurwa, :add, ["survivor"])

    TC.stop(two)
    TC.stop(three)
    TC.await_reachability(one, 3, 1)

    # Lenient by default: one replica is all there is, so it answers.
    assert TC.call(one, Kurwa, :fetch, ["survivor"]) == {:ok, true}
    assert TC.call(one, Kurwa, :add, ["written-alone"]) == :ok

    # Strict: refuse rather than quietly lower the durability that was promised.
    TC.call(one, Kurwa.Config, :put, [:strict_quorum, true])

    assert {:error, {:quorum_not_met, read}} = TC.call(one, Kurwa, :fetch, ["survivor"])
    assert read.op == :read
    assert read.got == 1
    assert Enum.sort(read.unreachable) == Enum.sort([two.node, three.node])

    assert {:error, {:quorum_not_met, write}} = TC.call(one, Kurwa, :add, ["refused"])
    assert write.op == :write
  end

  # Found as a flaky read-repair test: a write stamped by a coordinator whose
  # clock is behind a replica's record loses on every replica, and every one
  # still answers {:ok, winner}. It was acknowledged and had done nothing.
  test "an add coordinated by a node whose clock is behind still takes effect", %{
    peers: [one, two, three]
  } do
    key = "behind"
    storage_key = Kurwa.Key.encode(key)

    # A delete stamped far ahead, on two replicas only - so node one's clock has
    # never seen it.
    ahead = TC.call(one, Kurwa.Clock, :peek, []) + 10_000
    tombstone = Kurwa.Record.new(storage_key, ahead, three.node, false)
    for peer <- [two, three], do: {:ok, _} = TC.call(peer, Kurwa.Store, :put, [tombstone])
    assert TC.call(one, Kurwa.Clock, :peek, []) < ahead

    assert TC.call(one, Kurwa, :add, [key]) == :ok
    assert TC.call(two, Kurwa, :fetch, [key, [r: 3]]) == {:ok, true}
  end

  # SET NX's guarantee: of concurrent conditional adds for one absent key, at
  # most one is told it added it. Three coordinators race on every key.
  test "concurrent add_new from every node: never two winners", %{peers: peers} do
    keys = for i <- 1..150, do: "race:#{i}"

    winners =
      for key <- keys do
        peers
        |> Enum.map(fn peer -> Task.async(fn -> TC.call(peer, Kurwa, :add_new, [key]) end) end)
        |> Task.await_many(10_000)
        |> Enum.count(&(&1 == :ok))
      end

    assert Enum.max(winners) <= 1, "a key had #{Enum.max(winners)} winners"

    # With every node up they all ask the same replica first, so each key has
    # exactly one. (Before that ordering, 135 of 150 had none: each coordinator's
    # own replica answered it first, and three replicas chose three winners.)
    assert Enum.count(winners, &(&1 == 1)) == length(keys)

    # And every key is now present, winner or not.
    [one | _] = peers
    for key <- keys, do: assert(TC.call(one, Kurwa, :fetch, [key, [r: 3]]) == {:ok, true})
    assert TC.call(one, Kurwa, :add_new, ["race:1"]) == :exists
  end

  test "add_new needs a majority of n, and says so without one", %{peers: [one, two, three]} do
    TC.stop(three)
    TC.await_reachability(one, 3, 2)
    assert TC.call(one, Kurwa, :add_new, ["two-of-three"]) == :ok

    TC.stop(two)
    TC.await_reachability(one, 3, 1)

    assert {:error, {:no_majority, %{needed: 2, reachable: 1}}} =
             TC.call(one, Kurwa, :add_new, ["alone"])

    # A plain add still goes through with the lowered quorum, as before.
    assert TC.call(one, Kurwa, :add, ["alone"]) == :ok
  end

  # Replica requests carry no process monitor - it cost more than :erpc did -
  # so a replica that is gone must be reported by monitor_node, at once. Asked
  # directly, so that placement cannot quietly leave the dead nodes out first.
  test "a replica that is gone answers a request at once, not at the deadline", %{
    peers: [one, two, three]
  } do
    :ok = TC.call(one, Kurwa, :add, ["still-here"])
    key = TC.call(one, Kurwa.Key, :encode, ["still-here"])

    TC.stop(two, :kill)
    TC.stop(three, :kill)

    nodes = [one.node, two.node, three.node]

    {micros, outcome} =
      TC.call(one, :timer, :tc, [Kurwa.Quorum, :request, [nodes, {:get, key}, 3, 10_000]])

    assert [{_, record}] = outcome.ok
    assert Kurwa.Record.member?(record)

    assert outcome.failed |> Enum.map(&elem(&1, 0)) |> Enum.sort() ==
             Enum.sort([two.node, three.node])

    assert Enum.all?(outcome.failed, fn {_, reason} -> reason == {:no_reply, :noconnection} end)
    assert div(micros, 1000) < 3_000, "waited #{div(micros, 1000)}ms for two dead replicas"
  end

  test "a forgotten node leaves the ring and stops collecting hints", %{
    peers: [one, _two, three]
  } do
    TC.stop(three)
    TC.await_reachability(one, 3, 2)

    :ok = TC.call(one, Kurwa, :add, ["before-forget"])
    assert TC.call(one, Kurwa.Handoff, :depth, [])[three.node] == 1

    assert TC.call(one, Kurwa.Cluster, :forget, [three.node]) == :ok
    TC.await_reachability(one, 2, 2)

    :ok = TC.call(one, Kurwa, :add, ["after-forget"])

    # It is no longer a replica for anything, so nothing new is kept for it.
    assert TC.call(one, Kurwa.Handoff, :depth, [])[three.node] == 1
    refute TC.call(one, Kurwa, :info, []).members |> Enum.member?(three.node)
  end

  @tag cluster_opts: [wal_sync_interval: 60_000]
  test "a crashed replica loses its unsynced writes, and a read puts them back", %{
    peers: [one, _two, three],
    nodes: nodes,
    dir: dir
  } do
    keys = for i <- 1..5, do: "crash:#{i}"
    for key <- keys, do: assert(TC.call(one, Kurwa, :add, [key]) == :ok)
    assert TC.local_keys(three) == 5

    # Killed hard with the fsync interval pushed out of the way, so the log still
    # holds these writes in a buffer that never reaches the disk.
    TC.stop(three, :kill)
    TC.await_reachability(one, 3, 2)

    revived = TC.boot(:kurwa_node3, nodes, dir)
    on_exit(fn -> TC.stop(revived) end)
    TC.await_reachability(one, 3, 3)

    # Gone locally, and nothing was hinted for them: this replica had already
    # acknowledged the writes, so no coordinator knew it needed them again.
    assert TC.local_keys(revived) == 0

    # The cluster still has them - that is what replication is for - and a read
    # at r = 3 hears from every replica, so the stale one gets repaired.
    for key <- keys, do: assert(TC.call(one, Kurwa, :fetch, [key, [r: 3]]) == {:ok, true})

    TC.await(fn -> TC.local_keys(revived) == 5 end, 10_000)
  end

  @tag cluster_opts: [wal_sync_interval: 60_000]
  test "anti-entropy finds what a crash lost, with nobody reading the keys", %{
    peers: [one, _two, three],
    nodes: nodes,
    dir: dir
  } do
    keys = for i <- 1..8, do: "silent:#{i}"
    for key <- keys, do: assert(TC.call(one, Kurwa, :add, [key]) == :ok)
    assert TC.local_keys(three) == 8

    # Killed hard with the fsync interval pushed out of reach: this replica
    # acknowledged all eight writes and then lost them. Nothing anywhere knows
    # that happened - no coordinator saw a failure, so no hints exist.
    TC.stop(three, :kill)
    TC.await_reachability(one, 3, 2)

    revived = TC.boot(:kurwa_node3, nodes, dir)
    on_exit(fn -> TC.stop(revived) end)
    TC.await_reachability(one, 3, 3)

    assert TC.local_keys(revived) == 0
    assert TC.call(one, Kurwa.Handoff, :depth, []) == %{}

    # Not one read of these keys, so read repair has nothing to act on either.
    assert {:ok, result} = TC.call(one, Kurwa.Repair, :run, [revived.node], 60_000)
    assert result.diverged > 0
    assert result.repaired > 0

    assert TC.local_keys(revived) == 8

    for key <- keys do
      {:ok, record} = TC.call(revived, Kurwa.Store, :get, [Kurwa.Key.encode(key)])
      assert Kurwa.Record.alive?(record)
    end
  end

  test "anti-entropy is quiet when the replicas already agree", %{peers: [one, _two, three]} do
    for i <- 1..5, do: :ok = TC.call(one, Kurwa, :add, ["agreed:#{i}"])

    assert {:ok, result} = TC.call(one, Kurwa.Repair, :run, [three.node], 60_000)
    assert result.diverged == 0
    assert result.repaired == 0
    assert result.keys >= 5
  end

  @tag cluster_opts: [wal_sync_interval: 60_000, wal_sync_on_write: true]
  test "with an fsync on every write, a crash loses nothing", %{
    peers: [one, _two, three],
    nodes: nodes,
    dir: dir
  } do
    keys = for i <- 1..5, do: "durable:#{i}"
    for key <- keys, do: assert(TC.call(one, Kurwa, :add, [key]) == :ok)

    TC.stop(three, :kill)
    TC.await_reachability(one, 3, 2)

    revived = TC.boot(:kurwa_node3, nodes, dir, wal_sync_on_write: true)
    on_exit(fn -> TC.stop(revived) end)

    assert TC.local_keys(revived) == 5
  end
end
