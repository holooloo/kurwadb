defmodule Kurwa.Pg.ServerTest do
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [unique_key: 1, eventually: 1]

  alias Kurwa.PgClient, as: C

  setup_all do
    {:ok, server} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Pg.Server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, port: port}
  end

  setup %{port: port} do
    {socket, startup} = C.connect(port)
    on_exit(fn -> :gen_tcp.close(socket) end)
    {:ok, socket: socket, startup: startup, set: "pgtest-#{System.unique_integer([:positive])}"}
  end

  describe "schemas" do
    test "CREATE SCHEMA, a set inside it, and DROP SCHEMA once it is empty", %{socket: s} do
      schema = "sch#{System.unique_integer([:positive])}"

      assert C.errors(C.query(s, "CREATE TABLE #{schema}.t (key text)")) == [
               {"3F000", ~s|schema "#{schema}" does not exist|}
             ]

      assert C.tags(C.query(s, "CREATE SCHEMA #{schema}")) == ["CREATE SCHEMA"]
      assert [{"42P06", _}] = C.errors(C.query(s, "CREATE SCHEMA #{schema}"))
      assert C.tags(C.query(s, "CREATE SCHEMA IF NOT EXISTS #{schema}")) == ["CREATE SCHEMA"]
      assert C.tags(C.query(s, "DROP SCHEMA #{schema}")) == ["DROP SCHEMA"]
      assert [{"3F000", _}] = C.errors(C.query(s, "DROP SCHEMA #{schema}"))
      assert C.tags(C.query(s, "DROP SCHEMA IF EXISTS #{schema}")) == ["DROP SCHEMA"]

      C.query(s, "CREATE SCHEMA #{schema}")
      assert C.tags(C.query(s, "CREATE TABLE #{schema}.events (key text)")) == ["CREATE TABLE"]
      assert C.tags(C.query(s, "INSERT INTO #{schema}.events VALUES ('e1')")) == ["INSERT 0 1"]

      assert C.rows(C.query(s, "SELECT key FROM #{schema}.events WHERE key = 'e1'")) == [["e1"]]
      # The same set, by its whole name.
      assert C.rows(C.query(s, ~s|SELECT key FROM "#{schema}.events" WHERE key = 'e1'|)) == [
               ["e1"]
             ]

      refute Kurwa.Namespace.member?("events", "e1") == {:ok, true}

      eventually(fn -> assert [{"2BP01", _}] = C.errors(C.query(s, "DROP SCHEMA #{schema}")) end)
    end

    test "a table's one column is key, and CREATE TABLE says so", %{socket: s, set: set} do
      assert [{"0A000", message}] =
               C.errors(C.query(s, ~s|CREATE TABLE "#{set}" (column1 varchar NULL)|))

      assert message =~ "name the column key instead of column1"

      assert C.tags(C.query(s, ~s|CREATE TABLE "#{set}" (key text, PRIMARY KEY (key))|)) == [
               "CREATE TABLE"
             ]
    end

    test "CREATE DATABASE points at schemas", %{socket: s} do
      assert [{"0A000", message}] = C.errors(C.query(s, "CREATE DATABASE other"))
      assert message =~ "CREATE SCHEMA"
    end
  end

  describe "startup" do
    test "authenticates, reports parameters and a cancel key, and is idle", %{startup: startup} do
      assert {:auth, 0} = hd(startup)
      assert {:parameter_status, {"server_version", "16.0"}} in startup
      assert {:parameter_status, {"client_encoding", "UTF8"}} in startup
      assert Enum.any?(startup, &match?({:backend_key, _, _}, &1))
      assert List.last(startup) == {:ready, ?I}
    end

    test "TLS is declined, so the client carries on in the clear", %{port: port} do
      {socket, answer} = C.ssl_request(port)
      assert answer == "N"
      :gen_tcp.close(socket)
    end

    test "a 3.2 client is told to speak 3.0", %{port: port} do
      {socket, startup} = C.connect(port, minor: 2, params: [{"_pq_.nothing", "1"}])
      assert {:negotiate, 0} in startup
      assert List.last(startup) == {:ready, ?I}
      C.terminate(socket)
    end

    for method <- [:scram, :md5, :password] do
      test "with an auth token, the password is the token: #{method}", %{port: port} do
        original =
          {Application.get_env(:kurwadb, :auth_token), Application.get_env(:kurwadb, :pg_auth)}

        Kurwa.Config.put(:auth_token, "s3cret")
        Kurwa.Config.put(:pg_auth, unquote(method))

        on_exit(fn ->
          Kurwa.Config.put(:auth_token, elem(original, 0))
          Kurwa.Config.put(:pg_auth, elem(original, 1))
        end)

        {socket, startup} = C.connect(port, password: "s3cret")
        assert List.last(startup) == {:ready, ?I}
        C.terminate(socket)

        {socket, startup} = C.connect(port, password: "wrong")
        assert [{"28P01", _}] = C.errors(startup)
        :gen_tcp.close(socket)
      end
    end
  end

  describe "simple query" do
    test "insert, read back, count, delete", %{socket: s, set: set} do
      assert C.tags(C.query(s, "INSERT INTO \"#{set}\" VALUES ('a'), ('b')")) == ["INSERT 0 2"]

      result = C.query(s, "SELECT key FROM \"#{set}\" WHERE key IN ('a', 'b', 'c')")
      assert [{"key", 25, 0}] = C.columns(result)
      assert C.rows(result) == [["a"], ["b"]]
      assert C.tags(result) == ["SELECT 2"]

      assert C.rows(C.query(s, "SELECT count(*) FROM \"#{set}\" WHERE key = 'a'")) == [["1"]]

      # The tag counts what was there, which is how a client knows it consumed a key.
      assert C.tags(C.query(s, "DELETE FROM \"#{set}\" WHERE key IN ('a', 'zzz')")) == [
               "DELETE 1"
             ]

      assert C.tags(C.query(s, "DELETE FROM \"#{set}\" WHERE key = 'a'")) == ["DELETE 0"]
    end

    test "ON CONFLICT DO NOTHING counts, and returns, only the new rows", %{socket: s, set: set} do
      C.query(s, "INSERT INTO \"#{set}\" VALUES ('a')")

      result =
        C.query(
          s,
          "INSERT INTO \"#{set}\" VALUES ('a'), ('b') ON CONFLICT (key) DO NOTHING RETURNING key"
        )

      assert C.tags(result) == ["INSERT 0 1"]
      assert C.rows(result) == [["b"]]

      assert C.tags(C.query(s, "INSERT INTO \"#{set}\" VALUES ('b') ON CONFLICT DO NOTHING")) == [
               "INSERT 0 0"
             ]

      # Without it, an insert of a member is idempotent, not an error.
      assert C.tags(C.query(s, "INSERT INTO \"#{set}\" VALUES ('b')")) == ["INSERT 0 1"]
    end

    test "the default set is the table kurwa", %{socket: s} do
      key = unique_key("pg")
      C.query(s, "INSERT INTO kurwa VALUES ('#{key}')")
      assert Kurwa.member?(key)
      assert C.rows(C.query(s, "SELECT key FROM kurwa WHERE key = '#{key}'")) == [[key]]
      assert C.rows(C.query(s, "SELECT key FROM public.kurwa WHERE key = 'never'")) == []
    end

    test "a scan is refused with a reason, and the connection carries on", %{socket: s, set: set} do
      result = C.query(s, "SELECT * FROM \"#{set}\"")
      assert [{"0A000", message}] = C.errors(result)
      assert message =~ "scan"
      assert List.last(result) == {:ready, ?I}
      assert C.rows(C.query(s, "SELECT 1")) == [["1"]]
    end

    test "an error stops the rest of a multi-statement query", %{socket: s, set: set} do
      result =
        C.query(
          s,
          "INSERT INTO \"#{set}\" VALUES ('x'); SELECT nope(); INSERT INTO \"#{set}\" VALUES ('y')"
        )

      assert C.tags(result) == ["INSERT 0 1"]
      assert [{"42883", _}] = C.errors(result)
      refute Kurwa.Namespace.member?(set, "y") == {:ok, true}
    end

    test "the empty query", %{socket: s} do
      assert :empty_query in C.query(s, "")
    end

    test "functions and ttl", %{socket: s, set: set} do
      assert C.rows(
               C.query(s, "SELECT kurwa_add('#{set}', 'k', 60), kurwa_member('#{set}', 'k')")
             ) == [["t", "t"]]

      [[ttl]] = C.rows(C.query(s, "SELECT kurwa_ttl('#{set}', 'k')"))
      assert String.to_integer(ttl) in 55..60
      assert C.rows(C.query(s, "SELECT kurwa_ttl('#{set}', 'absent')")) == [[nil]]

      C.query(s, "INSERT INTO \"#{set}\" (key, ttl) VALUES ('t', 3600)")
      [["t", ttl]] = C.rows(C.query(s, "SELECT key, ttl FROM \"#{set}\" WHERE key = 't'"))
      assert String.to_integer(ttl) in 3590..3600
    end

    test "settings round-trip, and the reported ones are reported", %{socket: s} do
      result = C.query(s, "SET application_name = 'tests'")
      assert {:parameter_status, {"application_name", "tests"}} in result
      assert C.rows(C.query(s, "SHOW application_name")) == [["tests"]]
      assert C.rows(C.query(s, "SELECT current_setting('server_version_num')")) == [["160000"]]
    end
  end

  describe "key = ANY(...)" do
    test "an array literal and ARRAY[...] in a simple query", %{socket: s, set: set} do
      C.query(s, "INSERT INTO \"#{set}\" VALUES ('a'), ('b c'), ('d')")

      assert C.rows(C.query(s, ~s|SELECT key FROM "#{set}" WHERE key = ANY('{a,"b c",zz}')|)) ==
               [["a"], ["b c"]]

      assert C.tags(C.query(s, ~s|DELETE FROM "#{set}" WHERE key = ANY(ARRAY['d', 'zz'])|)) == [
               "DELETE 1"
             ]
    end

    test "a text[] parameter, described as such, in text and in binary", %{socket: s, set: set} do
      C.query(s, "INSERT INTO \"#{set}\" VALUES ('a'), ('b')")

      C.parse(s, "any", "SELECT key FROM \"#{set}\" WHERE key = ANY($1)")
      C.describe(s, "S", "any")
      C.bind(s, "", "any", [~s|{"a","zz"}|])
      C.execute(s, "")

      # one dimension, no nulls, text elements, two of them from index 1
      binary = <<1::32, 0::32, 25::32, 2::32, 1::32, 1::32, "b", 2::32, "zz">>
      C.bind(s, "", "any", [binary], [1])
      C.execute(s, "")
      result = C.sync_and_wait(s)

      assert {:parameter_description, [1009]} in result
      assert C.rows(result) == [["a"], ["b"]]
    end
  end

  describe "transactions" do
    test "status moves as PostgreSQL's would, and ROLLBACK warns", %{socket: s} do
      assert List.last(C.query(s, "BEGIN")) == {:ready, ?T}
      assert List.last(C.query(s, "SELECT nope()")) == {:ready, ?E}
      assert [{"25P02", _}] = C.errors(C.query(s, "SELECT 1"))

      result = C.query(s, "ROLLBACK")
      assert Enum.any?(result, &match?({:notice, _}, &1))
      assert List.last(result) == {:ready, ?I}
    end
  end

  describe "extended query" do
    test "parse, describe, bind, execute", %{socket: s, set: set} do
      C.query(s, "INSERT INTO \"#{set}\" VALUES ('a')")

      C.parse(s, "member", "SELECT key FROM \"#{set}\" WHERE key = $1")
      C.describe(s, "S", "member")
      C.bind(s, "", "member", ["a"])
      C.execute(s, "")
      C.bind(s, "", "member", ["b"])
      C.execute(s, "")
      result = C.sync_and_wait(s)

      assert :parse_complete in result
      assert {:parameter_description, [25]} in result
      assert [{"key", 25, 0}] = C.columns(result)
      assert C.rows(result) == [["a"]]
      assert C.tags(result) == ["SELECT 1", "SELECT 0"]
    end

    test "a ttl parameter is described as int8", %{socket: s, set: set} do
      C.parse(s, "", "INSERT INTO \"#{set}\" (key, ttl) VALUES ($1, $2)")
      C.describe(s, "S", "")
      result = C.sync_and_wait(s)
      assert {:parameter_description, [25, 20]} in result
      assert :no_data in result
    end

    test "binary results", %{socket: s, set: set} do
      C.parse(s, "", "SELECT kurwa_member($1, $2), kurwa_ttl($1, $2)")
      C.bind(s, "", "", [set, "nope"], [], [1])
      C.describe(s, "P", "")
      C.execute(s, "")
      result = C.sync_and_wait(s)

      assert [{"kurwa_member", 16, 1}, {"kurwa_ttl", 20, 1}] = C.columns(result)
      assert C.rows(result) == [[<<0>>, nil]]
    end

    test "max_rows pages a result with PortalSuspended", %{socket: s, set: set} do
      C.query(s, "INSERT INTO \"#{set}\" VALUES ('a'), ('b'), ('c')")
      C.parse(s, "", "SELECT key FROM \"#{set}\" WHERE key IN ('a', 'b', 'c')")
      C.bind(s, "p", "", [])
      C.execute(s, "p", 2)
      C.execute(s, "p", 2)
      result = C.sync_and_wait(s)

      assert C.rows(result) == [["a"], ["b"], ["c"]]
      assert :portal_suspended in result
      assert C.tags(result) == ["SELECT 3"]
    end

    test "after an error, everything up to Sync is skipped", %{socket: s} do
      C.parse(s, "", "SELECT nope()")
      C.bind(s, "", "", [])
      C.execute(s, "")
      C.parse(s, "", "SELECT 1")
      result = C.sync_and_wait(s)

      assert [{"42883", _}] = C.errors(result)
      refute :parse_complete in tl(Enum.drop_while(result, &(&1 != :parse_complete)))
      assert List.last(result) == {:ready, ?I}
      assert C.rows(C.query(s, "SELECT 1")) == [["1"]]
    end

    test "a named statement must be closed before it is reused", %{socket: s} do
      C.parse(s, "x", "SELECT 1")
      C.parse(s, "x", "SELECT 2")
      assert [{"42P05", _}] = C.errors(C.sync_and_wait(s))

      C.close(s, "S", "x")
      C.parse(s, "x", "SELECT 2")
      assert :parse_complete in C.sync_and_wait(s)
    end

    test "the wrong number of parameters is an error", %{socket: s, set: set} do
      C.parse(s, "", "SELECT key FROM \"#{set}\" WHERE key = $1")
      C.bind(s, "", "", [])
      assert [{"08P01", _}] = C.errors(C.sync_and_wait(s))
    end
  end

  describe "catalog" do
    test "psql's \\dt query lists sets as tables", %{socket: s, set: set} do
      C.query(s, "INSERT INTO \"#{set}\" VALUES ('a')")

      eventually(fn ->
        {:ok, %{sets: sets}} = Kurwa.Namespace.list()
        set in sets
      end)

      sql = """
      SELECT n.nspname as "Schema",
        c.relname as "Name",
        CASE c.relkind WHEN 'r' THEN 'table' END as "Type",
        pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
      FROM pg_catalog.pg_class c
           LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind IN ('r','p','')
        AND c.relname OPERATOR(pg_catalog.~) '^(#{set})$' COLLATE pg_catalog.default
      ORDER BY 1,2;
      """

      result = C.query(s, sql)

      assert [{"Schema", _, _}, {"Name", _, _}, {"Type", _, _}, {"Owner", _, _}] =
               C.columns(result)

      assert C.rows(result) == [["public", set, "table", "test"]]
    end

    test "an unknown catalog query gets no rows, with the columns it asked for", %{socket: s} do
      result =
        C.query(
          s,
          "SELECT t.oid, t.typname AS name FROM pg_catalog.pg_type t WHERE t.typname = 'hstore'"
        )

      assert [{"oid", _, _}, {"name", _, _}] = C.columns(result)
      assert C.rows(result) == []
    end
  end
end
