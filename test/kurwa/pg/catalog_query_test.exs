defmodule Kurwa.Pg.CatalogQueryTest do
  # The catalog queries DBeaver 26 sends, verbatim from its log, answered
  # from the registry.
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [eventually: 2]

  alias Kurwa.Pg.Catalog

  @session %{user: "dbeaver"}

  setup_all do
    schema = "cq#{System.unique_integer([:positive])}"
    :ok = Kurwa.Registry.register_schema(schema)
    :ok = Kurwa.Namespace.add(schema <> ".events", "e1")
    :ok = Kurwa.Namespace.add("cqplain", "p1")

    eventually(fn -> (schema <> ".events") in sets() end, 5_000)

    eventually(fn -> "cqplain" in sets() end, 5_000)

    {:ok, schema: schema}
  end

  defp sets do
    {:ok, %{sets: sets}} = Kurwa.Namespace.list()
    sets
  end

  defp answer(sql, params \\ []) do
    {:rows, columns, rows, _tag} = Catalog.answer(sql, @session, params)
    names = Enum.map(columns, &elem(&1, 0))
    assert Enum.map(Catalog.columns(sql), &elem(&1, 0)) == names
    Enum.map(rows, &Map.new(Enum.zip(names, &1)))
  end

  test "search_path from pg_settings" do
    assert [%{"reset_val" => ~s("$user", public)}] =
             answer("SELECT reset_val FROM pg_settings WHERE name = 'search_path'")
  end

  test "schemas, with n.* expanded", %{schema: schema} do
    rows =
      answer("""
      SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n
      LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass
      """)

    names = Enum.map(rows, & &1["nspname"])
    assert "public" in names and schema in names and "pg_catalog" in names
    assert %{"nspowner" => "10", "description" => nil} = hd(rows)
  end

  test "a schema's oid by name", %{schema: schema} do
    sql = "SELECT s.oid as schema_id\nfrom pg_catalog.pg_namespace s\nWHERE s.nspname =$1"
    assert [%{"schema_id" => oid}] = answer(sql, [schema])
    assert oid == Kurwa.Pg.Catalog.Tables.namespace_oid(schema)
  end

  test "a schema's tables, then their columns", %{schema: schema} do
    oid = Kurwa.Pg.Catalog.Tables.namespace_oid(schema)

    tables =
      answer(
        """
        SELECT c.oid,c.*,d.description,pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_expr,  pg_catalog.pg_get_partkeydef(c.oid) as partition_key 
        FROM pg_catalog.pg_class c
        LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_class'::regclass
        WHERE c.relnamespace=$1 AND c.relkind not in ('i','I','c')
        """,
        [oid]
      )

    assert [%{"relname" => "events", "relkind" => "r", "relnamespace" => ^oid} = table] = tables

    columns =
      answer(
        """
        SELECT c.relname,a.*,pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) as def_value,dsc.description,dep.objid
        FROM pg_catalog.pg_attribute a
        INNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid)
        LEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum)
        LEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
        LEFT OUTER JOIN pg_depend dep on dep.refobjid = a.attrelid AND dep.deptype = 'i' and dep.refobjsubid = a.attnum and dep.classid = dep.refclassid
        WHERE NOT a.attisdropped AND c.relkind not in ('i','I','c') AND c.oid=$1
        ORDER BY a.attnum
        """,
        [table["oid"]]
      )

    assert [%{"relname" => "events", "attname" => "key", "atttypid" => "25", "def_value" => nil}] =
             columns
  end

  test "public holds the plain sets" do
    tables =
      answer("SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relnamespace = 2200 ORDER BY 1")

    names = Enum.map(tables, & &1["relname"])
    assert "cqplain" in names and "kurwa" in names
    refute "events" in names
    assert names == Enum.sort(names)
  end

  test "a type by oid, with format_type" do
    assert [%{"typname" => "text", "base_type_name" => nil, "relkind" => nil}] =
             answer(
               """
               SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name FROM pg_catalog.pg_type t
               LEFT OUTER JOIN pg_class c ON c.oid=t.typrelid
               LEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid
               WHERE t.oid=$1 
               """,
               [25]
             )
  end

  test "the database, roles, and a catalog kurwadb has nothing in" do
    assert [%{"datname" => "kurwadb", "datistemplate" => "f"}] =
             answer("SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE datname=$1", [
               "kurwadb"
             ])

    assert [] = answer("SELECT db.datname FROM pg_catalog.pg_database db WHERE datistemplate")

    assert [%{"rolname" => "dbeaver"}] =
             answer("SELECT a.oid,a.* FROM pg_catalog.pg_roles a  ORDER BY a.rolname")

    assert [] = answer("SELECT c.oid,c.* FROM pg_catalog.pg_collation c \nORDER BY c.oid")
  end

  test "information_schema.tables", %{schema: schema} do
    rows =
      answer(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'",
        [schema]
      )

    assert rows == [%{"table_name" => "events"}]
  end
end
