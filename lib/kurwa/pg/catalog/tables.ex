defmodule Kurwa.Pg.Catalog.Tables do
  @moduledoc """
  The system catalogs as rows, made up from what kurwadb has: one database,
  the schemas and sets in the registry, one role, a handful of types.

  Every set is an ordinary table with one column, `key text`, in the schema
  its name says (`analytics.events`) or in `public`. Values are text, the way
  the catalogs are read over the wire, or nil for NULL; booleans are "t"/"f".

  A table not listed here is a catalog kurwadb has nothing in - `pg_index`,
  `pg_proc`, `pg_description` - and has no rows.
  """

  alias Kurwa.Pg.Catalog

  @user_oid "10"
  @database_oid "5"
  @namespaces %{"pg_catalog" => "11", "public" => "2200", "information_schema" => "13183"}

  @doc "The oid of a schema."
  def namespace_oid(schema),
    do: Map.get(@namespaces, schema) || to_string(Catalog.oid("/" <> schema))

  @doc """
  `{columns, rows}` for a catalog table, each row a map from column to value,
  or nil for a table kurwadb does not know. `ctx` has the session user and
  functions for the sets and schemas, called only by tables that need them.
  """
  def table(name, ctx) do
    case name do
      "pg_namespace" ->
        {~w(oid nspname nspowner nspacl), namespaces(ctx)}

      "pg_class" ->
        {class_columns(), classes(ctx)}

      "pg_attribute" ->
        {attribute_columns(), attributes(ctx)}

      "pg_type" ->
        {type_columns(), types()}

      "pg_database" ->
        {database_columns(), [database(ctx)]}

      "pg_roles" ->
        {role_columns(), [role(ctx)]}

      "pg_user" ->
        {~w(usename usesysid usecreatedb usesuper userepl usebypassrls passwd valuntil useconfig),
         [user(ctx)]}

      "pg_authid" ->
        {role_columns(), [role(ctx)]}

      "pg_settings" ->
        {setting_columns(), settings(ctx)}

      "pg_am" ->
        {~w(oid amname amhandler amtype), access_methods()}

      "pg_tablespace" ->
        {~w(oid spcname spcowner spcacl spcoptions), tablespaces()}

      "pg_tables" ->
        {pg_tables_columns(), pg_tables(ctx)}

      "information_schema.schemata" ->
        {schemata_columns(), schemata(ctx)}

      "information_schema.tables" ->
        {info_tables_columns(), info_tables(ctx)}

      "information_schema.columns" ->
        {info_columns_columns(), info_columns(ctx)}

      _ ->
        nil
    end
  end

  # ------------------------------------------------------------- namespaces

  defp namespaces(ctx) do
    for schema <- ["pg_catalog", "information_schema" | user_schemas(ctx)] do
      %{
        "oid" => namespace_oid(schema),
        "nspname" => schema,
        "nspowner" => @user_oid,
        "nspacl" => nil
      }
    end
  end

  defp user_schemas(ctx), do: Enum.uniq(["public" | ctx.schemas.()])

  # ---------------------------------------------------------------- classes

  defp class_columns,
    do: ~w(oid relname relnamespace reltype reloftype relowner relam relfilenode reltablespace
           relpages reltuples relallvisible reltoastrelid relhasindex relisshared relpersistence
           relkind relnatts relchecks relhasrules relhastriggers relhassubclass relrowsecurity
           relforcerowsecurity relispopulated relreplident relispartition relrewrite relfrozenxid
           relminmxid relacl reloptions relpartbound)

  defp classes(ctx) do
    for name <- ctx.sets.() do
      {schema, table} = Catalog.split(name)
      oid = to_string(Catalog.oid(name))

      %{
        "oid" => oid,
        "relname" => table,
        "relnamespace" => namespace_oid(schema),
        "reltype" => "0",
        "reloftype" => "0",
        "relowner" => @user_oid,
        "relam" => "2",
        "relfilenode" => oid,
        "reltablespace" => "0",
        "relpages" => "0",
        "reltuples" => "-1",
        "relallvisible" => "0",
        "reltoastrelid" => "0",
        "relhasindex" => "f",
        "relisshared" => "f",
        "relpersistence" => "p",
        "relkind" => "r",
        "relnatts" => "1",
        "relchecks" => "0",
        "relhasrules" => "f",
        "relhastriggers" => "f",
        "relhassubclass" => "f",
        "relrowsecurity" => "f",
        "relforcerowsecurity" => "f",
        "relispopulated" => "t",
        "relreplident" => "d",
        "relispartition" => "f",
        "relrewrite" => "0",
        "relfrozenxid" => "0",
        "relminmxid" => "1",
        "relacl" => nil,
        "reloptions" => nil,
        "relpartbound" => nil
      }
    end
  end

  # ------------------------------------------------------------- attributes

  defp attribute_columns,
    do: ~w(attrelid attname atttypid attlen attnum attcacheoff atttypmod attndims attbyval
           attalign attstorage attcompression attnotnull atthasdef atthasmissing attidentity
           attgenerated attisdropped attislocal attinhcount attstattarget attcollation attacl
           attoptions attfdwoptions attmissingval)

  defp attributes(ctx) do
    for name <- ctx.sets.() do
      %{
        "attrelid" => to_string(Catalog.oid(name)),
        "attname" => "key",
        "atttypid" => "25",
        "attlen" => "-1",
        "attnum" => "1",
        "attcacheoff" => "-1",
        "atttypmod" => "-1",
        "attndims" => "0",
        "attbyval" => "f",
        "attalign" => "i",
        "attstorage" => "x",
        "attcompression" => "",
        "attnotnull" => "t",
        "atthasdef" => "f",
        "atthasmissing" => "f",
        "attidentity" => "",
        "attgenerated" => "",
        "attisdropped" => "f",
        "attislocal" => "t",
        "attinhcount" => "0",
        "attstattarget" => "-1",
        "attcollation" => "100",
        "attacl" => nil,
        "attoptions" => nil,
        "attfdwoptions" => nil,
        "attmissingval" => nil
      }
    end
  end

  # ------------------------------------------------------------------ types

  defp type_columns,
    do: ~w(oid typname typnamespace typowner typlen typbyval typtype typcategory typispreferred
           typisdefined typdelim typrelid typsubscript typelem typarray typinput typoutput
           typreceive typsend typmodin typmodout typanalyze typalign typstorage typnotnull
           typbasetype typtypmod typndims typcollation typdefaultbin typdefault typacl)

  # oid, name, length, by value, category, align, storage, collation, array oid
  @types [
    {16, "bool", 1, true, "B", "c", "p", 0, 1000},
    {17, "bytea", -1, false, "U", "i", "x", 0, 1001},
    {18, "char", 1, true, "Z", "c", "p", 0, 1002},
    {19, "name", 64, false, "S", "c", "p", 950, 1003},
    {20, "int8", 8, true, "N", "d", "p", 0, 1016},
    {21, "int2", 2, true, "N", "s", "p", 0, 1005},
    {23, "int4", 4, true, "N", "i", "p", 0, 1007},
    {25, "text", -1, false, "S", "i", "x", 100, 1009},
    {26, "oid", 4, true, "N", "i", "p", 0, 1028},
    {114, "json", -1, false, "U", "i", "x", 0, 199},
    {700, "float4", 4, true, "N", "i", "p", 0, 1021},
    {701, "float8", 8, true, "N", "d", "p", 0, 1022},
    {1042, "bpchar", -1, false, "S", "i", "x", 100, 1014},
    {1043, "varchar", -1, false, "S", "i", "x", 100, 1015},
    {1082, "date", 4, true, "D", "i", "p", 0, 1182},
    {1083, "time", 8, true, "D", "d", "p", 0, 1183},
    {1114, "timestamp", 8, true, "D", "d", "p", 0, 1115},
    {1184, "timestamptz", 8, true, "D", "d", "p", 0, 1185},
    {1186, "interval", 16, false, "T", "d", "p", 0, 1187},
    {1700, "numeric", -1, false, "N", "i", "m", 0, 1231},
    {2950, "uuid", 16, false, "U", "c", "p", 0, 2951},
    {3802, "jsonb", -1, false, "U", "i", "x", 0, 3807}
  ]

  @doc "The name of a type oid, for format_type()."
  def type_name(oid) do
    case Enum.find(@types, fn {o, _, _, _, _, _, _, _, _} -> to_string(o) == to_string(oid) end) do
      {_, "bpchar", _, _, _, _, _, _, _} -> "character"
      {_, "varchar", _, _, _, _, _, _, _} -> "character varying"
      {_, "int4", _, _, _, _, _, _, _} -> "integer"
      {_, "int8", _, _, _, _, _, _, _} -> "bigint"
      {_, "bool", _, _, _, _, _, _, _} -> "boolean"
      {_, name, _, _, _, _, _, _, _} -> name
      nil -> nil
    end
  end

  defp types do
    for {oid, name, len, byval, category, align, storage, collation, array} <- @types do
      %{
        "oid" => to_string(oid),
        "typname" => name,
        "typnamespace" => "11",
        "typowner" => @user_oid,
        "typlen" => to_string(len),
        "typbyval" => bool(byval),
        "typtype" => "b",
        "typcategory" => category,
        "typispreferred" => bool(name in ~w(text float8 bool oid timestamptz)),
        "typisdefined" => "t",
        "typdelim" => ",",
        "typrelid" => "0",
        "typsubscript" => "-",
        "typelem" => "0",
        "typarray" => to_string(array),
        "typinput" => name <> "in",
        "typoutput" => name <> "out",
        "typreceive" => name <> "recv",
        "typsend" => name <> "send",
        "typmodin" => "-",
        "typmodout" => "-",
        "typanalyze" => "-",
        "typalign" => align,
        "typstorage" => storage,
        "typnotnull" => "f",
        "typbasetype" => "0",
        "typtypmod" => "-1",
        "typndims" => "0",
        "typcollation" => to_string(collation),
        "typdefaultbin" => nil,
        "typdefault" => nil,
        "typacl" => nil
      }
    end
  end

  # ------------------------------------------------- database, role, settings

  defp database_columns,
    do: ~w(oid datname datdba encoding datlocprovider datistemplate datallowconn dathasloginevt
           datconnlimit datfrozenxid datminmxid dattablespace datcollate datctype datlocale
           daticurules datcollversion datacl)

  defp database(_ctx) do
    %{
      "oid" => @database_oid,
      "datname" => "kurwadb",
      "datdba" => @user_oid,
      "encoding" => "6",
      "datlocprovider" => "c",
      "datistemplate" => "f",
      "datallowconn" => "t",
      "dathasloginevt" => "f",
      "datconnlimit" => "-1",
      "datfrozenxid" => "0",
      "datminmxid" => "1",
      "dattablespace" => "1663",
      "datcollate" => "C.UTF-8",
      "datctype" => "C.UTF-8",
      "datlocale" => nil,
      "daticurules" => nil,
      "datcollversion" => nil,
      "datacl" => nil
    }
  end

  defp role_columns,
    do: ~w(oid rolname rolsuper rolinherit rolcreaterole rolcreatedb rolcanlogin rolreplication
           rolconnlimit rolpassword rolvaliduntil rolbypassrls rolconfig)

  defp role(ctx) do
    %{
      "oid" => @user_oid,
      "rolname" => ctx.user,
      "rolsuper" => "t",
      "rolinherit" => "t",
      "rolcreaterole" => "t",
      "rolcreatedb" => "t",
      "rolcanlogin" => "t",
      "rolreplication" => "f",
      "rolconnlimit" => "-1",
      "rolpassword" => "********",
      "rolvaliduntil" => nil,
      "rolbypassrls" => "t",
      "rolconfig" => nil
    }
  end

  defp user(ctx) do
    %{
      "usename" => ctx.user,
      "usesysid" => @user_oid,
      "usecreatedb" => "t",
      "usesuper" => "t",
      "userepl" => "f",
      "usebypassrls" => "t",
      "passwd" => "********",
      "valuntil" => nil,
      "useconfig" => nil
    }
  end

  defp setting_columns,
    do: ~w(name setting unit category short_desc extra_desc context vartype source min_val
           max_val enumvals boot_val reset_val sourcefile sourceline pending_restart)

  defp settings(ctx) do
    [
      {"search_path", ~s("$user", public)},
      {"server_version", "16.0"},
      {"server_version_num", "160000"},
      {"server_encoding", "UTF8"},
      {"client_encoding", "UTF8"},
      {"DateStyle", "ISO, MDY"},
      {"TimeZone", "UTC"},
      {"standard_conforming_strings", "on"},
      {"integer_datetimes", "on"},
      {"max_identifier_length", "63"},
      {"default_transaction_isolation", "read committed"},
      {"transaction_isolation", "read committed"},
      {"application_name", Map.get(ctx, :application_name, "")}
    ]
    |> Enum.map(fn {name, value} ->
      %{
        "name" => name,
        "setting" => value,
        "unit" => nil,
        "category" => "kurwadb",
        "short_desc" => "",
        "extra_desc" => nil,
        "context" => "user",
        "vartype" => "string",
        "source" => "default",
        "min_val" => nil,
        "max_val" => nil,
        "enumvals" => nil,
        "boot_val" => value,
        "reset_val" => value,
        "sourcefile" => nil,
        "sourceline" => nil,
        "pending_restart" => "f"
      }
    end)
  end

  defp access_methods do
    [
      %{"oid" => "2", "amname" => "heap", "amhandler" => "heap_tableam_handler", "amtype" => "t"},
      %{"oid" => "403", "amname" => "btree", "amhandler" => "bthandler", "amtype" => "i"}
    ]
  end

  defp tablespaces do
    for {oid, name} <- [{"1663", "pg_default"}, {"1664", "pg_global"}] do
      %{
        "oid" => oid,
        "spcname" => name,
        "spcowner" => @user_oid,
        "spcacl" => nil,
        "spcoptions" => nil
      }
    end
  end

  # ------------------------------------------------------------------ views

  defp pg_tables_columns,
    do: ~w(schemaname tablename tableowner tablespace hasindexes hasrules hastriggers rowsecurity)

  defp pg_tables(ctx) do
    for name <- ctx.sets.() do
      {schema, table} = Catalog.split(name)

      %{
        "schemaname" => schema,
        "tablename" => table,
        "tableowner" => ctx.user,
        "tablespace" => nil,
        "hasindexes" => "f",
        "hasrules" => "f",
        "hastriggers" => "f",
        "rowsecurity" => "f"
      }
    end
  end

  defp schemata_columns,
    do: ~w(catalog_name schema_name schema_owner default_character_set_catalog
           default_character_set_schema default_character_set_name sql_path)

  defp schemata(ctx) do
    for schema <- ["pg_catalog", "information_schema" | user_schemas(ctx)] do
      %{
        "catalog_name" => "kurwadb",
        "schema_name" => schema,
        "schema_owner" => ctx.user,
        "default_character_set_catalog" => nil,
        "default_character_set_schema" => nil,
        "default_character_set_name" => nil,
        "sql_path" => nil
      }
    end
  end

  defp info_tables_columns,
    do: ~w(table_catalog table_schema table_name table_type self_referencing_column_name
           reference_generation user_defined_type_catalog user_defined_type_schema
           user_defined_type_name is_insertable_into is_typed commit_action)

  defp info_tables(ctx) do
    for name <- ctx.sets.() do
      {schema, table} = Catalog.split(name)

      %{
        "table_catalog" => "kurwadb",
        "table_schema" => schema,
        "table_name" => table,
        "table_type" => "BASE TABLE",
        "self_referencing_column_name" => nil,
        "reference_generation" => nil,
        "user_defined_type_catalog" => nil,
        "user_defined_type_schema" => nil,
        "user_defined_type_name" => nil,
        "is_insertable_into" => "YES",
        "is_typed" => "NO",
        "commit_action" => nil
      }
    end
  end

  defp info_columns_columns,
    do: ~w(table_catalog table_schema table_name column_name ordinal_position column_default
           is_nullable data_type character_maximum_length character_octet_length
           numeric_precision numeric_scale datetime_precision collation_name udt_catalog
           udt_schema udt_name is_identity is_generated is_updatable)

  defp info_columns(ctx) do
    for name <- ctx.sets.() do
      {schema, table} = Catalog.split(name)

      %{
        "table_catalog" => "kurwadb",
        "table_schema" => schema,
        "table_name" => table,
        "column_name" => "key",
        "ordinal_position" => "1",
        "column_default" => nil,
        "is_nullable" => "NO",
        "data_type" => "text",
        "character_maximum_length" => nil,
        "character_octet_length" => "1073741824",
        "numeric_precision" => nil,
        "numeric_scale" => nil,
        "datetime_precision" => nil,
        "collation_name" => nil,
        "udt_catalog" => "kurwadb",
        "udt_schema" => "pg_catalog",
        "udt_name" => "text",
        "is_identity" => "NO",
        "is_generated" => "NEVER",
        "is_updatable" => "YES"
      }
    end
  end

  defp bool(true), do: "t"
  defp bool(false), do: "f"
end
