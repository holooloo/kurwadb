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

  test "an open connection is listed as a client, named as it named itself" do
    {:ok, server} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Pg.Server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    {socket, _} =
      Kurwa.PgClient.connect(port, user: "alice", params: [{"application_name", "metrics-test"}])

    Kurwa.PgClient.query(socket, "SELECT 1")

    client =
      eventually(fn ->
        Enum.find(Kurwa.Metrics.snapshot().clients, &(&1.app == "metrics-test"))
      end)

    assert %{frontend: :pg, user: "alice", requests: 1} = client
    assert client.peer =~ "127.0.0.1:"

    :gen_tcp.close(socket)

    eventually(fn ->
      not Enum.any?(Kurwa.Metrics.snapshot().clients, &(&1.app == "metrics-test"))
    end)
  end

  test "the dashboard page is compiled in" do
    assert Kurwa.Gateway.Dashboard.html() =~ "/dashboard/state"
  end
end
