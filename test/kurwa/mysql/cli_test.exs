defmodule Kurwa.Mysql.CliTest do
  @moduledoc """
  The real mysql client against the real server. Skipped by default:

      mix test --include mysql          (MYSQL_BIN=/path/to/mysql if it is not on the path)
  """
  use ExUnit.Case, async: false

  @moduletag :mysql

  setup_all do
    bin =
      System.get_env("MYSQL_BIN") || System.find_executable("mysql") ||
        Enum.find(["/opt/homebrew/opt/mysql-client/bin/mysql"], &File.exists?/1)

    if bin == nil, do: raise("no mysql client; set MYSQL_BIN or run without --include mysql")

    {:ok, server} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Mysql.Server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, mysql_bin: bin, port: port}
  end

  defp mysql(%{mysql_bin: bin, port: port}, sql, env \\ []) do
    System.cmd(bin, ["-h", "127.0.0.1", "-P", "#{port}", "-u", "tester", "kurwadb", "-e", sql],
      stderr_to_stdout: true,
      env: env
    )
  end

  test "queries, SHOW TABLES and DESCRIBE", context do
    set = "clitest#{System.unique_integer([:positive])}"

    {out, 0} =
      mysql(
        context,
        "INSERT INTO #{set} VALUES ('a'), ('b'); SELECT `key` FROM #{set} WHERE `key` IN ('a', 'z'); DESCRIBE #{set}"
      )

    assert out =~ ~r/key\na\n/
    assert out =~ "key\tvarchar(255)\tNO\tPRI"

    {out, code} = mysql(context, "SELECT * FROM #{set}")
    assert code != 0
    assert out =~ "ERROR 1235"
  end

  test "caching_sha2_password with a token", context do
    original = Application.get_env(:kurwadb, :auth_token)
    Kurwa.Config.put(:auth_token, "s3cret")
    on_exit(fn -> Kurwa.Config.put(:auth_token, original) end)

    assert {"ok\n1\n", 0} = mysql(context, "SELECT 1 AS ok", [{"MYSQL_PWD", "s3cret"}])
    {out, code} = mysql(context, "SELECT 1", [{"MYSQL_PWD", "nope"}])
    assert code != 0
    assert out =~ "ERROR 1045"
  end
end
