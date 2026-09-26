# Three real nodes, three real BEAMs, quorum across all of them.
#
#     MIX_ENV=test mix run --no-start bench/cluster.exs
#
# Uses the same peer helper as the cluster tests, so the orchestrating VM stays
# out of the cluster it is measuring.

alias Kurwa.TestCluster, as: TC

dir = "tmp/bench-cluster"
File.rm_rf!(dir)
[one | _] = peers = TC.start(3, dir, shards: 8)
IO.puts("\n3 nodes, n=3 r=2 w=2\n")

# The loop runs inside a node, so the peer control channel is not in the timing.
latency = """
n = 20_000
{add, _} = :timer.tc(fn -> for i <- 1..n, do: :ok = Kurwa.add("lat:\#{i}") end)
{get, _} = :timer.tc(fn -> for i <- 1..n, do: {:ok, true} = Kurwa.fetch("lat:\#{i}") end)
{Float.round(add / n, 2), Float.round(get / n, 2)}
"""

throughput = """
run = fn concurrency, per_worker, fun ->
  {us, _} =
    :timer.tc(fn ->
      1..concurrency
      |> Enum.map(fn w -> Task.async(fn -> for i <- 1..per_worker, do: fun.(w, i) end) end)
      |> Task.await_many(300_000)
    end)

  round(concurrency * per_worker / (us / 1_000_000))
end

writes = run.(64, 2_000, fn w, i -> :ok = Kurwa.add("tp:\#{w}:\#{i}") end)
reads = run.(64, 2_000, fn w, i -> {:ok, true} = Kurwa.fetch("tp:\#{w}:\#{i}") end)
local = run.(64, 20_000, fn w, i -> {:ok, _} = Kurwa.Store.get(Kurwa.Key.encode("tp:\#{w}:\#{i}")) end)
{writes, reads, local}
"""

{add, get} = TC.call(one, Code, :eval_string, [latency], 300_000) |> elem(0)
IO.puts("single client, sequential")
IO.puts("  add     (w=2)  #{add} us/op")
IO.puts("  member? (r=2)  #{get} us/op")

{writes, reads, local} = TC.call(one, Code, :eval_string, [throughput], 600_000) |> elem(0)
IO.puts("\n64 concurrent clients, one coordinating node")
IO.puts("  add     (w=2)  #{writes} ops/sec")
IO.puts("  member? (r=2)  #{reads} ops/sec")
IO.puts("  local ETS read #{local} ops/sec")
IO.puts("")

Enum.each(peers, &TC.stop/1)
File.rm_rf!(dir)
