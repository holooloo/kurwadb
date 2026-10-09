# One client, sequential requests over loopback, through the real PostgreSQL
# and SQL Server frontends: the minimum of seven rounds, which on a busy
# machine is the least noisy estimate of what the server itself costs.
#
#     MIX_ENV=test mix run --no-start bench/frontend_latency.exs
Logger.configure(level: :warning)
{:ok, _} = Application.ensure_all_started(:kurwadb)
Logger.configure(level: :warning)
:ok = Kurwa.add("lat1")
{:ok, pg} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Pg.Server)
{:ok, {_, pg_port}} = ThousandIsland.listener_info(pg)
{:ok, ms} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Mssql.Server)
{:ok, {_, ms_port}} = ThousandIsland.listener_info(ms)
{ps, _} = Kurwa.PgClient.connect(pg_port)
{ts, _} = Kurwa.TdsClient.connect(ms_port)
n = 3000
round = fn f ->
  for _ <- 1..300, do: f.()
  {us, _} = :timer.tc(fn -> for _ <- 1..n, do: f.() end)
  us / n
end
cases = [
  {"pg simple query", fn -> Kurwa.PgClient.query(ps, "SELECT key FROM kurwa WHERE key = 'lat1'") end},
  {"tds batch", fn -> Kurwa.TdsClient.batch(ts, "SELECT [key] FROM kurwa WHERE [key] = 'lat1'") end},
  {"tds sp_executesql", fn -> Kurwa.TdsClient.executesql(ts, "SELECT [key] FROM kurwa WHERE [key] = @k", [{"k", "lat1"}]) end}
]
for {label, f} <- cases do
  best = Enum.min(for _ <- 1..7, do: round.(f))
  IO.puts("#{String.pad_trailing(label, 22)} #{Float.round(best, 2)} us")
end
