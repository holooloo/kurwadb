# What the on-disk engine is for: the same keys, and what each engine keeps in
# RAM to answer for them.
#
#     MIX_ENV=test mix run --no-start bench/engines.exs

alias Kurwa.Record
alias Kurwa.Store.{Ets, Lsm, SSTable}

count = 200_000
dir = "tmp/bench-engines"
File.rm_rf!(dir)

keys = for i <- 1..count, do: "order:#{1_000_000 + i}"
records = for {key, i} <- Enum.with_index(keys, 1), do: Record.new(key, i, node(), true)

fill = fn state, put ->
  Enum.reduce(records, state, fn record, state ->
    {_, _, state} = put.(state, record)
    state
  end)
end

IO.puts("\n#{count} keys, 13-byte names\n")

# ---------------------------------------------------------------------- ETS
{:ok, ets} = Ets.open(:bench_ets, dir: Path.join(dir, "ets"))
ets = fill.(ets, &Ets.put/2)
words = :erlang.system_info(:wordsize)
ets_ram = :ets.info(Ets.table_name(:bench_ets), :memory) * words

{ets_read, _} = :timer.tc(fn -> for k <- Enum.take_random(keys, 20_000), do: Ets.get(Ets.handle(:bench_ets), k) end)

IO.puts("ETS engine")
IO.puts("  resident          #{Float.round(ets_ram / 1024 / 1024, 1)} MB  (#{Float.round(ets_ram / count, 1)} bytes/key)")
IO.puts("  get               #{Float.round(ets_read / 20_000 * 1000, 0)} ns")
Ets.close(ets)

# ---------------------------------------------------------------------- LSM
{:ok, lsm} = Lsm.open(:bench_lsm, dir: Path.join(dir, "lsm"), memtable_keys: 50_000, max_tables: 2)
lsm = fill.(lsm, &Lsm.put/2)
lsm = Lsm.compact(lsm)

lsm_ram = Lsm.tables(lsm) |> Enum.reduce(0, fn t, sum -> sum + SSTable.memory(t) end)
on_disk =
  Path.join(dir, "lsm") |> File.ls!() |> Enum.filter(&String.ends_with?(&1, ".sst"))
  |> Enum.reduce(0, fn f, sum -> sum + File.stat!(Path.join([dir, "lsm", f])).size end)

{lsm_read, _} = :timer.tc(fn -> for k <- Enum.take_random(keys, 20_000), do: Lsm.get(Lsm.handle(:bench_lsm), k) end)
{lsm_miss, _} = :timer.tc(fn -> for i <- 1..20_000, do: Lsm.get(Lsm.handle(:bench_lsm), "absent:#{i}") end)

IO.puts("\nLSM engine (everything flushed to disk)")
IO.puts("  resident          #{Float.round(lsm_ram / 1024 / 1024, 2)} MB  (#{Float.round(lsm_ram / count, 2)} bytes/key)")
IO.puts("  on disk           #{Float.round(on_disk / 1024 / 1024, 1)} MB")
IO.puts("  get, present      #{Float.round(lsm_read / 20_000, 2)} us")
IO.puts("  get, absent       #{Float.round(lsm_miss / 20_000, 2)} us  (the filter answers, no seek)")

IO.puts("\nRAM per key: #{Float.round(ets_ram / count, 1)} B  ->  #{Float.round(lsm_ram / count, 2)} B")
IO.puts("same machine holds #{Float.round(ets_ram / lsm_ram, 0)}x more keys\n")

Lsm.close(lsm)
File.rm_rf!(dir)
