defmodule Kurwa.Mysql.ServerTest do
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [eventually: 1]

  alias Kurwa.MysqlClient, as: C

  setup_all do
    {:ok, server} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Mysql.Server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, port: port}
  end

  setup %{port: port} do
    {s, %{auth: :ok}} = C.connect(port)
    on_exit(fn -> :gen_tcp.close(s) end)
    {:ok, s: s, set: "mytest#{System.unique_integer([:positive])}"}
  end

  test "insert, read back with `key`, delete with true counts", %{s: s, set: set} do
    assert [{:ok, 2, 0}] = C.query(s, "INSERT INTO #{set} VALUES ('a'), ('b')")

    assert [{:rows, ["key"], [["a"], ["b"]]}] =
             C.query(s, "SELECT `key` FROM #{set} WHERE `key` IN ('a', \"b\", 'c')")

    assert [{:ok, 1, 0}] = C.query(s, "DELETE FROM #{set} WHERE `key` IN ('a', 'zz')")
    assert [{:ok, 0, 0}] = C.query(s, "DELETE FROM #{set} WHERE `key` = 'a'")
  end

  test "INSERT IGNORE counts only new rows; ON DUPLICATE KEY UPDATE is refused", %{s: s, set: set} do
    C.query(s, "INSERT INTO #{set} VALUES ('a')")
    assert [{:ok, 1, 0}] = C.query(s, "INSERT IGNORE INTO #{set} VALUES ('a'), ('b')")

    assert [{:error, 1235, _}] =
             C.query(s, "INSERT INTO #{set} VALUES ('a') ON DUPLICATE KEY UPDATE `key` = 'x'")
  end

  test "a scan is refused with MySQL's code, and the connection carries on", %{s: s, set: set} do
    assert [{:error, 1235, message}] = C.query(s, "SELECT * FROM #{set}")
    assert message =~ "scan"
    assert [{:rows, _, [["1"]]}] = C.query(s, "SELECT 1")
  end

  test "several statements in one query", %{s: s, set: set} do
    assert [{:ok, 1, 0}, {:rows, ["key"], [["x"]]}] =
             C.query(
               s,
               "INSERT INTO #{set} VALUES ('x'); SELECT `key` FROM #{set} WHERE `key` = 'x'"
             )
  end

  test "what the mysql client asks on its own", %{s: s} do
    assert [{:rows, ["@@version_comment"], [[comment]]}] =
             C.query(s, "select @@version_comment limit 1")

    assert comment =~ "kurwadb"
    assert [{:rows, ["DATABASE()"], [["kurwadb"]]}] = C.query(s, "SELECT DATABASE()")
    assert [{:rows, ["kurwa_count()", "n"], _}] = C.query(s, "SELECT kurwa_count(), 1 AS n")
    assert [{:ok, 0, 0}] = C.query(s, "SET NAMES utf8mb4")
    assert [{:ok, 0, 0}] = C.query(s, "USE kurwadb")
    assert [{:error, 1049, _}] = C.query(s, "USE other")

    assert [{:rows, ["Variable_name", "Value"], [["max_allowed_packet", _]]}] =
             C.query(s, "SHOW VARIABLES LIKE 'max_allowed%'")
  end

  test "SHOW TABLES and DESCRIBE", %{s: s, set: set} do
    C.query(s, "INSERT INTO #{set} VALUES ('a')")

    eventually(fn ->
      {:ok, %{sets: sets}} = Kurwa.Namespace.list()
      set in sets
    end)

    assert [{:rows, ["Tables_in_kurwadb"], tables}] = C.query(s, "SHOW TABLES")
    assert [set] in tables

    assert [{:rows, ["Field" | _], [["key", "varchar(255)", "NO", "PRI", nil, ""]]}] =
             C.query(s, "DESCRIBE #{set}")
  end

  test "ROLLBACK warns, and SHOW WARNINGS says why", %{s: s} do
    assert [{:ok, 0, 0}] = C.query(s, "START TRANSACTION")
    assert [{:ok, 0, 1}] = C.query(s, "ROLLBACK")

    assert [{:rows, ["Level", "Code", "Message"], [["Warning", _, message]]}] =
             C.query(s, "SHOW WARNINGS")

    assert message =~ "no transactions"
  end

  test "prepared statements: binary results, tinyint and bigint", %{s: s, set: set} do
    C.query(s, "INSERT INTO #{set} VALUES ('a')")
    assert {:ok, id, 1, 1} = C.prepare(s, "SELECT `key` FROM #{set} WHERE `key` = ?")
    assert [{:rows, ["key"], [["a"]]}] = C.execute(s, id, ["a"])
    assert [{:rows, ["key"], []}] = C.execute(s, id, ["zz"])

    assert {:ok, id, 3, 2} = C.prepare(s, "SELECT kurwa_member(?, ?), kurwa_ttl(?, 'a')")
    assert [{:rows, _, [[1, -1]]}] = C.execute(s, id, [set, "a", set])
  end

  test "an unknown database is refused at connect", %{port: port} do
    {s, %{auth: {:error, 1049}}} = C.connect(port, database: "other")
    :gen_tcp.close(s)
  end

  for plugin <- ["caching_sha2_password", "mysql_native_password"] do
    test "with an auth token, the password is the token: #{plugin}", %{port: port} do
      original = Application.get_env(:kurwadb, :auth_token)
      Application.put_env(:kurwadb, :auth_token, "s3cret")
      on_exit(fn -> Application.put_env(:kurwadb, :auth_token, original) end)

      {s, %{auth: :ok}} = C.connect(port, password: "s3cret", plugin: unquote(plugin))
      :gen_tcp.close(s)
      {s, %{auth: {:error, 1045}}} = C.connect(port, password: "wrong", plugin: unquote(plugin))
      :gen_tcp.close(s)
    end
  end

  test "without CLIENT_DEPRECATE_EOF, result sets end with EOF", %{port: port, set: set} do
    {s, %{auth: :ok}} = C.connect(port, deprecate_eof: false)
    C.query(s, "INSERT INTO #{set} VALUES ('e')")
    assert [{:rows, ["key"], [["e"]]}] = C.query(s, "SELECT `key` FROM #{set} WHERE `key` = 'e'")
    :gen_tcp.close(s)
  end
end
