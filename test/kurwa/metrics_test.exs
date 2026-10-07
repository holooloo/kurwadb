defmodule Kurwa.MetricsTest do
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [eventually: 1]

  test "a request through the stack shows up at every layer it passed" do
    for i <- 1..50, do: :ok = Kurwa.add("metrics-#{i}")
    for i <- 1..50, do: true = Kurwa.member?("metrics-#{i}")
    Kurwa.Metrics.measure(:pg, fn -> :ok end)

    snapshot =
      eventually(fn ->
        snapshot = Kurwa.Metrics.snapshot()
        snapshot.writes > 0 and snapshot.reads > 0 and snapshot
      end)

    assert snapshot.up and snapshot.node == to_string(node())
    assert snapshot.local_replica > 0
    assert Enum.any?(snapshot.shards, &(&1.puts > 0))
    assert Enum.any?(snapshot.shards, &(&1.gets > 0))
    assert length(snapshot.history.rps) > 0
  end

  test "the cluster view lists this node, and serialises to JSON" do
    assert [%{node: node, up: true} = snapshot] = Kurwa.Metrics.cluster()
    assert node == to_string(node())
    assert {:ok, _} = Jason.encode(snapshot)
  end

  test "the dashboard page is compiled in" do
    assert Kurwa.Gateway.Dashboard.html() =~ "/dashboard/state"
  end
end
