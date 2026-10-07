defmodule Kurwa.Mssql.SqlcmdTest do
  @moduledoc """
  Microsoft's go-sqlcmd against the real server, over TLS - both encrypted
  throughout and login-only. Skipped by default:

      mix test --include mssql          (SQLCMD_BIN=/path/to/sqlcmd if it is not on the path)
  """
  use ExUnit.Case, async: false

  @moduletag :mssql

  setup_all do
    bin =
      System.get_env("SQLCMD_BIN") || System.find_executable("sqlcmd") ||
        Enum.find([Path.expand("tmp/sqlcmd/sqlcmd")], &File.exists?/1) ||
        raise "no sqlcmd; set SQLCMD_BIN or run without --include mssql"

    {:ok, server} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Mssql.Server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, bin: bin, port: port}
  end

  defp sqlcmd(%{bin: bin, port: port}, encrypt, sql) do
    server = "tcp:127.0.0.1,#{port}"
    args = ["-S", server, "-U", "sa", "-P", "x", "-C", "-N", encrypt, "-W", "-Q", sql]
    System.cmd(bin, args, stderr_to_stdout: true)
  end

  for encrypt <- ["true", "false"] do
    test "queries over TLS (encrypt #{encrypt})", context do
      set = "sqlcmd#{System.unique_integer([:positive])}"

      {out, 0} =
        sqlcmd(
          context,
          unquote(encrypt),
          "INSERT INTO #{set} VALUES ('a'); SELECT [key] FROM #{set} WHERE [key] = 'a'"
        )

      assert out =~ "(1 row affected)"
      assert out =~ ~r/key\n---\na\n/
    end
  end
end
