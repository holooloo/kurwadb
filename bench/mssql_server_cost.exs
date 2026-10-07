# What the SQL Server frontend costs on the server per sp_executesql lookup,
# in parts, with no client and no TLS in the way.
#
#     KURWA_N=1 KURWA_R=1 KURWA_W=1 mix run bench/mssql_server_cost.exs

alias Kurwa.Mssql.Tds
alias Kurwa.Sql.{Exec, Parser}
Kurwa.add("1")
sql = "SELECT [key] FROM kurwa WHERE [key] = @k"

param = fn name, value ->
  v = Tds.ucs2(value)

  [
    byte_size(Tds.ucs2(name)) |> div(2),
    Tds.ucs2(name),
    0,
    0xE7,
    <<8000::16-little>>,
    Tds.collation(),
    <<byte_size(v)::16-little>>,
    v
  ]
end

payload =
  IO.iodata_to_binary([
    <<22::32-little, 18::32-little, 2::16-little, 0::64, 1::32-little>>,
    <<0xFFFF::16, 10::16-little, 0::16>>,
    param.("", sql),
    param.("", "@k nvarchar(4000)"),
    param.("@k", "1")
  ])

session = %{
  user: "u",
  database: "kurwadb",
  pid: 1,
  settings: %{},
  sysvars: %{},
  server_properties: %{}
}

n = 50_000

t = fn label, f ->
  for _ <- 1..2000, do: f.()
  {us, _} = :timer.tc(fn -> for _ <- 1..n, do: f.() end)
  IO.puts(String.pad_trailing(label, 44) <> "#{Float.round(us / n, 2)} us")
end

t.("RPC decode (headers, params, UTF-16)", fn ->
  {_, r} = Tds.all_headers(payload)
  Tds.rpcs(r)
end)

t.("Parser.parse(sql, :tsql)", fn -> Parser.parse(sql, :tsql) end)
{:ok, [st]} = Parser.parse(sql, :tsql)
t.("Exec.run (the lookup)", fn -> Exec.run(st, %{"k" => "1"}, session) end)
{:rows, cols, rows, _} = Exec.run(st, %{"k" => "1"}, session)

t.("encode COLMETADATA + ROW + DONE + packets", fn ->
  Tds.packets(
    4,
    [
      Tds.colmetadata(cols),
      Enum.map(rows, &Tds.row(&1, cols)),
      Tds.done(:in_proc, count: 1),
      Tds.return_status(0),
      Tds.done(:proc)
    ],
    4096
  )
end)
