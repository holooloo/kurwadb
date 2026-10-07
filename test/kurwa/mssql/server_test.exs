defmodule Kurwa.Mssql.ServerTest do
  use ExUnit.Case, async: false

  alias Kurwa.TdsClient, as: C

  setup_all do
    {:ok, server} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Mssql.Server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, port: port}
  end

  setup %{port: port} do
    {s, %{login: login}} = C.connect(port)
    assert :loginack in login
    on_exit(fn -> :gen_tcp.close(s) end)
    {:ok, s: s, set: "tdstest#{System.unique_integer([:positive])}"}
  end

  test "a client without encryption gets none", %{port: port} do
    {s, %{encryption: encryption}} = C.connect(port)
    assert encryption == 0x02
    :gen_tcp.close(s)
  end

  test "insert, select by [key], delete, with true counts", %{s: s, set: set} do
    assert C.counts(C.batch(s, "INSERT INTO #{set} VALUES (N'a'), ('b')")) == [2]

    reply = C.batch(s, "SELECT [key] FROM dbo.#{set} WHERE [key] IN ('a', 'zz')")
    assert C.columns(reply) == ["key"]
    assert C.rows(reply) == [["a"]]

    assert C.counts(C.batch(s, "DELETE FROM #{set} WHERE [key] IN ('a', 'zz')")) == [1]
    assert C.counts(C.batch(s, "DELETE FROM #{set} WHERE [key] = 'a'")) == [0]
  end

  test "IF NOT EXISTS ... INSERT has one winner, and counts it", %{s: s, set: set} do
    sql = "IF NOT EXISTS (SELECT 1 FROM #{set} WHERE [key] = 'k') INSERT INTO #{set} VALUES ('k')"
    assert C.counts(C.batch(s, sql)) == [1]
    assert C.counts(C.batch(s, sql)) == [0]
  end

  test "statements without semicolons, TOP, OUTPUT and SET NOCOUNT", %{s: s, set: set} do
    reply =
      C.batch(
        s,
        "INSERT INTO #{set} OUTPUT inserted.[key] VALUES ('x')\nSELECT TOP 1 [key] FROM #{set} WHERE [key] = 'x'"
      )

    assert C.rows(reply) == [["x"], ["x"]]

    reply = C.batch(s, "SET NOCOUNT ON\nINSERT INTO #{set} VALUES ('y')")
    assert C.counts(reply) == []
  end

  test "a scan is refused with the reason, and the session carries on", %{s: s, set: set} do
    assert [{50_000, message}] = C.errors(C.batch(s, "SELECT * FROM #{set}"))
    assert message =~ "scan"
    assert C.rows(C.batch(s, "SELECT 1")) == [[1]]
  end

  test "what clients ask on their own", %{s: s} do
    [[version, db, product]] =
      C.rows(C.batch(s, "SELECT @@VERSION, DB_NAME(), SERVERPROPERTY('ProductVersion')"))

    assert version =~ "kurwadb"
    assert db == "kurwadb"
    assert product =~ ~r/^16\./
  end

  test "sp_executesql with named parameters", %{s: s, set: set} do
    C.batch(s, "INSERT INTO #{set} VALUES ('p')")

    reply =
      C.executesql(s, "SELECT [key] FROM #{set} WHERE [key] IN (@a, @b)", [
        {"a", "p"},
        {"b", "zz"}
      ])

    assert C.rows(reply) == [["p"]]
    assert {:return_status, 0} in reply
  end

  test "sp_prepexec returns a handle that sp_execute reuses", %{s: s, set: set} do
    C.batch(s, "INSERT INTO #{set} VALUES ('h')")
    reply = C.prepexec(s, "SELECT [key] FROM #{set} WHERE [key] = @k", "k", "h")
    assert C.rows(reply) == [["h"]]
    [handle] = for {:return_value, h} <- reply, do: h

    assert C.rows(C.execute(s, handle, "k", "zz")) == []
    assert C.rows(C.execute(s, handle, "k", "h")) == [["h"]]
  end

  test "transactions: BEGIN/ROLLBACK in a batch and through the transaction manager", %{s: s} do
    assert C.rows(C.batch(s, "BEGIN TRAN SELECT @@TRANCOUNT")) == [[1]]
    reply = C.batch(s, "ROLLBACK")
    assert [{0, message}] = C.infos(reply)
    assert message =~ "no transactions"

    assert {:envchange, 8} in C.transaction(s, :begin)
    assert {:envchange, 9} in C.transaction(s, :commit)
  end

  test "with an auth token, the password is the token", %{port: port} do
    original = Application.get_env(:kurwadb, :auth_token)
    Application.put_env(:kurwadb, :auth_token, "s3cret")
    on_exit(fn -> Application.put_env(:kurwadb, :auth_token, original) end)

    {s, %{login: login}} = C.connect(port, password: "s3cret")
    assert :loginack in login
    :gen_tcp.close(s)

    {s, %{login: login}} = C.connect(port, password: "nope")
    assert [{18_456, _}] = C.errors(login)
    :gen_tcp.close(s)
  end

  test "another database is refused at login", %{port: port} do
    {s, %{login: login}} = C.connect(port, database: "other")
    assert [{4060, _}] = C.errors(login)
    :gen_tcp.close(s)
  end
end
