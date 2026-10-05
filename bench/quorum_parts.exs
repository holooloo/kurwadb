# Where a quorum read's time goes, in parts, from inside a node of a real
# three-node cluster - the table in PERFORMANCE.md under "What 0.9.0 changed".
#
#     MIX_ENV=test mix run --no-start bench/quorum_parts.exs

alias Kurwa.TestCluster, as: TC
dir = "tmp/bench-quorum-parts"
File.rm_rf!(dir)
[one | _] = peers = TC.start(3, dir, shards: 8)

probe = ~S"""
n = 20_000
key = fn i -> Kurwa.Key.encode("lat:#{i}") end
for i <- 1..n, do: :ok = Kurwa.add("lat:#{i}")
[remote | _] = Node.list()
per = fn f -> {us, _} = :timer.tc(fn -> for i <- 1..n, do: f.(i) end); Float.round(us / n, 2) end

# a bare echo process on the remote node: the floor for one distribution round trip
echo = Node.spawn(remote, fn ->
  loop = fn loop -> receive do {from, ref} -> send(from, {ref, :ok}); loop.(loop) end end
  loop.(loop)
end)
ping = fn _ -> ref = make_ref(); send(echo, {self(), ref}); receive do {^ref, :ok} -> :ok end end

local_targets = [node(), node(), node()]
noop = fn _node -> {:ok, nil} end

[
  {"Placement.targets", per.(fn i -> Kurwa.Placement.targets(key.(i), 3) end)},
  {"Store.get, local", per.(fn i -> Kurwa.Store.get(key.(i)) end)},
  {"dist round trip, bare send/receive", per.(ping)},
  {":erpc.call remote, no-op (:erlang.node)", per.(fn _ -> :erpc.call(remote, :erlang, :node, [], 2000) end)},
  {":erpc.call remote, Replica.get", per.(fn i -> :erpc.call(remote, Kurwa.Replica, :get, [key.(i)], 2000) end)},
  {"Quorum.run, 3 local no-op targets, need 2", per.(fn _ -> Kurwa.Quorum.run(local_targets, noop, 2, 2000) end)},
  {"Coordinator.member?", per.(fn i -> Kurwa.Coordinator.member?(key.(i)) end)},
  {"Kurwa.fetch", per.(fn i -> {:ok, true} = Kurwa.fetch("lat:#{i}") end)},
]
"""

for {label, us} <- TC.call(one, Code, :eval_string, [probe], 600_000) |> elem(0),
    do: IO.puts(String.pad_trailing("  " <> label, 60) <> "#{us} us")

Enum.each(peers, &TC.stop/1)
File.rm_rf!(dir)
