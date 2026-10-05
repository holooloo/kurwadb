defmodule Kurwa.Pg.PsqlTest do
  @moduledoc """
  The real psql against the real server. Skipped where psql is not installed:

      mix test --include psql
  """
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [eventually: 1]

  @moduletag :psql

  setup_all do
    psql = System.find_executable("psql")
    if psql == nil, do: raise("psql is not installed; run without --include psql")

    {:ok, server} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Pg.Server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, psql_bin: psql, dsn: "host=127.0.0.1 port=#{port} user=tester dbname=kurwadb"}
  end

  defp psql(%{psql_bin: psql, dsn: dsn}, args) do
    System.cmd(psql, [dsn, "-X", "-v", "ON_ERROR_STOP=0" | args], stderr_to_stdout: true)
  end

  test "queries, \\dt and \\d", context do
    set = "psqltest#{System.unique_integer([:positive])}"

    {out, 0} =
      psql(context, [
        "-c",
        "INSERT INTO #{set} VALUES ('a'), ('b')",
        "-c",
        "SELECT key FROM #{set} WHERE key IN ('a', 'c')",
        "-c",
        "DELETE FROM #{set} WHERE key = 'b'"
      ])

    assert out =~ "INSERT 0 2"
    assert out =~ ~r/key\s*\n-+\n a\n\(1 row\)/
    assert out =~ "DELETE 1"

    eventually(fn ->
      {:ok, %{sets: sets}} = Kurwa.Namespace.list()
      set in sets
    end)

    {out, 0} = psql(context, ["-c", "\\dt"])
    assert out =~ ~r/public \| #{set}\s*\| table \| tester/

    {out, 0} = psql(context, ["-c", "\\d #{set}"])
    assert out =~ ~s(Table "public.#{set}")
    assert out =~ ~r/key\s+\| text\s+\|\s+\| not null/
  end
end
