defmodule Kurwa.Pg.CatalogTest do
  use ExUnit.Case, async: true

  alias Kurwa.Pg.Catalog

  test "select-list names follow PostgreSQL: alias, column, function, ?column?" do
    # The relation query psql 18 sends for \d, verbatim.
    sql = """
    SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
    FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
    LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
    WHERE c.oid = '1';
    """

    assert Catalog.select_list(sql) == ~w(relchecks relkind relhasindex relhasrules relhastriggers
             relrowsecurity relforcerowsecurity relhasoids relispartition ?column? reltablespace case
             relpersistence relreplident amname)
  end

  test "a subquery's FROM does not end the select list" do
    sql =
      ~s|SELECT a.attname, (SELECT d.x FROM pg_catalog.pg_attrdef d WHERE d.y = 1), a.attnotnull AS "Not Null" FROM pg_catalog.pg_attribute a|

    assert Catalog.select_list(sql) == ["attname", "?column?", "Not Null"]
  end

  test "a set's oid is stable" do
    assert Catalog.oid("seen") == Catalog.oid("seen")
    refute Catalog.oid("seen") == Catalog.oid("other")
  end
end
