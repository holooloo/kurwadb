# Single-node numbers: what a key costs to store, and what each layer costs to
# cross. Run with a scratch data directory so it never touches real data:
#
#     KURWA_DATA_DIR=tmp/bench mix run bench/local.exs
#
# Single process, no concurrency - these are latencies and sizes, not throughput.

words = :erlang.system_info(:wordsize)
shards = Kurwa.Config.shards()

table_memory = fn ->
  Enum.reduce(0..(shards - 1), 0, fn i, acc ->
    name = Kurwa.Store.Ets.table_name(Kurwa.Store.Shard.name(i))
    acc + :ets.info(name, :memory) * words
  end)
end

footprint = fn label, keyfun, count ->
  before = table_memory.()

  for i <- 1..count do
    key = Kurwa.Key.encode(keyfun.(i))
    {:ok, _} = Kurwa.Store.put(Kurwa.Record.new(key, i, node(), true))
  end

  per_key = (table_memory.() - before) / count
  IO.puts("  #{String.pad_trailing(label, 34)} #{Float.round(per_key, 1)} bytes/key")
  per_key
end

bench = fn label, n, fun ->
  {us, _} = :timer.tc(fn -> for i <- 1..n, do: fun.(i) end)
  per_op = us / n
  unit = if per_op < 1, do: "#{Float.round(per_op * 1000, 0)} ns", else: "#{Float.round(per_op, 2)} us"
  IO.puts("  #{String.pad_trailing(label, 34)} #{unit}/op")
  per_op
end

IO.puts("\nstorage footprint")
short = footprint.("13-byte key", &"order:#{1_000_000 + &1}", 200_000)

long =
  footprint.(
    "36-byte key",
    fn i ->
      hex = i |> :erlang.phash2(4_294_967_296) |> Integer.to_string(16) |> String.pad_leading(8, "0")
      "#{hex}-aaaa-bbbb-cccc-ddddeeeeffff"
    end,
    200_000
  )

IO.puts("\n  projected RAM, keys only")

for n <- [10_000_000, 100_000_000] do
  IO.puts(
    "    #{div(n, 1_000_000)}M keys: " <>
      "#{Float.round(n * short / 1024 / 1024 / 1024, 1)} GB .. " <>
      "#{Float.round(n * long / 1024 / 1024 / 1024, 1)} GB"
  )
end

probe = Kurwa.Key.encode("order:1000001")

# Warm up and discard: the first pass pays for code loading and for growing the
# ETS tables, and reporting that as the steady state is how you publish a number
# you later have to correct.
for i <- 1..5_000 do
  Kurwa.Store.get(probe)
  Kurwa.add("warmup:#{i}")
end

IO.puts("\nwhere the time goes (single node)")
bench.("Clock.tick", 50_000, fn _ -> Kurwa.Clock.tick() end)
bench.("Placement.targets", 50_000, fn _ -> Kurwa.Placement.targets(probe, 3) end)
bench.("Quorum.run, 1 target", 20_000, fn _ ->
  Kurwa.Quorum.run([node()], fn _ -> {:ok, :x} end, 1, 1_000)
end)
bench.("Store.get (ETS only)", 200_000, fn _ -> Kurwa.Store.get(probe) end)
bench.("Store.put (shard + WAL)", 20_000, fn i ->
  Kurwa.Store.put(Kurwa.Record.new(probe, i, node(), true))
end)

IO.puts("\nend to end (single node)")
bench.("Kurwa.add", 20_000, fn i -> :ok = Kurwa.add("bench:#{i}") end)
bench.("Kurwa.member? (present)", 20_000, fn i -> true = Kurwa.member?("bench:#{i}") end)
bench.("Kurwa.member? (absent)", 20_000, fn i -> false = Kurwa.member?("nope:#{i}") end)
IO.puts("")
