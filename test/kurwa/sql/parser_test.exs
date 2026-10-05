defmodule Kurwa.Sql.ParserTest do
  use ExUnit.Case, async: true

  alias Kurwa.Sql.Parser

  defp one(sql) do
    assert {:ok, [statement]} = Parser.parse(sql)
    statement
  end

  test "membership by key, in either order of the comparison" do
    assert {:select, %{from: "seen", where: {:keys, [{:lit, "a"}]}}} =
             one("SELECT key FROM seen WHERE key = 'a'")

    assert {:select, %{where: {:keys, [{:param, 1}]}}} =
             one("select key from seen where $1 = key")
  end

  test "IN lists, LIMIT and count(*)" do
    assert {:select, %{items: [{:count_star, "count"}], where: {:keys, keys}, limit: {:lit, 2}}} =
             one("SELECT count(*) FROM seen WHERE key IN ('a', 'b', $1) LIMIT 2")

    assert keys == [{:lit, "a"}, {:lit, "b"}, {:param, 1}]
  end

  test "the default set is the table kurwa, and public. is accepted" do
    assert {:select, %{from: :default}} = one("SELECT key FROM kurwa WHERE key = 'a'")
    assert {:select, %{from: nil}} = one("SELECT 1")
    assert {:select, %{from: "seen"}} = one("SELECT key FROM public.seen WHERE key = 'a'")
    assert {:error, "3F000", _} = Parser.parse("SELECT key FROM other.seen WHERE key = 'a'")
  end

  test "quoted identifiers carry the characters a set name may have" do
    assert {:select, %{from: "my-set.v2"}} = one(~s(SELECT key FROM "my-set.v2" WHERE key = 'a'))
  end

  test "inserts with and without columns, ttl, ON CONFLICT and RETURNING" do
    assert {:insert, "seen", nil, [[{:lit, "a"}], [{:lit, "b"}]], nil} =
             one("INSERT INTO seen VALUES ('a'), ('b')")

    assert {:insert, "seen", ["key", "ttl"], [[{:param, 1}, {:param, 2}]],
            [{{:col, "key"}, "key"}]} =
             one(
               "INSERT INTO seen (key, ttl) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING key"
             )

    assert {:error, "42703", _} = Parser.parse("INSERT INTO seen (value) VALUES ('a')")
  end

  test "a delete needs a key, an update is refused with a reason" do
    assert {:delete, "seen", {:keys, [{:lit, "a"}]}, nil} =
             one("DELETE FROM seen WHERE key = 'a'")

    assert {:error, "0A000", message} = Parser.parse("DELETE FROM seen")
    assert message =~ "scan"
    assert {:error, "0A000", _} = Parser.parse("UPDATE seen SET key = 'b'")
    assert {:error, "0A000", _} = Parser.parse("DROP TABLE seen")
  end

  test "functions, casts, EXISTS and niladic functions" do
    assert {:select,
            %{
              items: [
                {{:call, "kurwa_add", [{:lit, "s"}, {:cast, {:param, 1}, "text"}]}, "kurwa_add"}
              ]
            }} =
             one("SELECT kurwa_add('s', $1::text)")

    assert {:select, %{items: [{{:exists, %{from: "seen"}}, "exists"}]}} =
             one("SELECT EXISTS (SELECT 1 FROM seen WHERE key = $1)")

    assert {:select, %{items: [{{:call, "current_user", []}, "current_user"}]}} =
             one("SELECT current_user")

    assert {:select, %{items: [{{:call, "version", []}, "v"}]}} =
             one("SELECT pg_catalog.version() AS v")
  end

  test "several statements in one query, comments, and the empty query" do
    assert {:ok, [{:utility, :begin, "BEGIN"}, {:select, _}, {:utility, :commit, "COMMIT"}]} =
             Parser.parse("BEGIN; -- start\nSELECT 1; /* end */ COMMIT;")

    assert {:ok, [:empty]} = Parser.parse("")
    assert {:ok, [:empty]} = Parser.parse("  -- nothing\n")
  end

  test "strings: doubled quotes, and E'' escapes" do
    assert {:select, %{items: [{{:lit, "it's"}, _}]}} = one("SELECT 'it''s'")
    assert {:select, %{items: [{{:lit, "a\nb"}, _}]}} = one("SELECT E'a\\nb'")
  end

  test "settings, as drivers send them" do
    assert {:set, {"application_name", "app"}} = one("SET application_name = 'app'")
    assert {:set, {"extra_float_digits", "3"}} = one("SET extra_float_digits TO 3")
    assert {:set, {"timezone", "UTC"}} = one("SET SESSION TimeZone = 'UTC'")
    assert {:show, "transaction isolation level"} = one("SHOW TRANSACTION ISOLATION LEVEL")
  end

  test "catalog queries are passed through whole" do
    sql = "SELECT c.relname FROM pg_catalog.pg_class c"
    assert {:ok, [{:catalog, ^sql}]} = Parser.parse(sql)
  end

  test "the MySQL dialect: backticks and positional parameters" do
    assert {:ok, [{:select, %{from: "my-set", where: {:keys, [{:param, 1}, {:param, 2}]}}}]} =
             Parser.parse("SELECT `key` FROM `my-set` WHERE key IN (?, ?)", :mysql)
  end

  test "syntax errors name the token" do
    assert {:error, "42601", message} =
             Parser.parse("SELECT key FROM seen WHERE key = 'a' garbage")

    assert message =~ "garbage"
  end
end
