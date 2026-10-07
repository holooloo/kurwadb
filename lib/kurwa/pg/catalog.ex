defmodule Kurwa.Pg.Catalog do
  @moduledoc """
  Enough of `pg_catalog` for `psql` to list and describe sets.

  `\\dt` and `\\d name` are not commands on the wire: psql turns them into
  queries against the system catalogs, and prints what comes back. There are no
  catalogs here, so this recognises those queries and answers them from the set
  registry - every set is a table in `public` with one column, `key text`.

  Anything else that reads the catalogs gets an empty result with the columns it
  asked for, by name. That is what lets a tool probe for something kurwadb
  does not have and carry on, instead of failing on an error it did not expect.
  Every column is answered as text, which psql and the drivers we test read
  fine; a client that insists on catalog types will need more than this.
  """

  alias Kurwa.Namespace
  alias Kurwa.Pg.Catalog.Query
  alias Kurwa.Sql.Parser

  require Logger

  @doc "The columns a catalog query would return, from its select list."
  def columns(sql) do
    with false <- psql?(sql),
         {:ok, columns, _rows} <- Query.run(sql, Query.ctx("", fn -> [] end, fn -> [] end), []) do
      columns
    else
      _ -> sql |> select_list() |> Enum.map(&{&1, :text})
    end
  end

  @doc "Answers a catalog query as `{:rows, columns, rows, tag}`."
  def answer(sql, session, params \\ []) do
    ctx =
      Query.ctx(
        Map.get(session, :user, "kurwadb"),
        fn -> [Parser.default_table() | known_sets()] end,
        &schemas/0
      )

    {columns, rows} =
      with false <- psql?(sql),
           {:ok, columns, rows} <- Query.run(sql, ctx, params) do
        {columns, rows}
      else
        _ ->
          columns = sql |> select_list() |> Enum.map(&{&1, :text})
          {columns, rows(sql, columns, session)}
      end

    {:rows, columns, rows, "SELECT #{length(rows)}"}
  end

  # The queries psql sends for \dt, \d and \dn, answered as psql expects.
  defp psql?(sql) do
    listing?(sql) or lookup?(sql) or schemas?(sql) or
      (sql =~ ~r/FROM pg_catalog\.pg_class c\b/ and sql =~ ~r/\bc\.oid = '\d+'/) or
      (sql =~ "pg_catalog.pg_attribute a" and sql =~ ~r/\ba\.attrelid = '\d+'/)
  end

  defp schemas do
    {:ok, schemas} = Kurwa.Registry.schemas()
    schemas
  end

  # \dt [pattern]: one row per set.
  defp rows(sql, _columns, session) when is_binary(sql) do
    cond do
      listing?(sql) ->
        for name <- matching(sql),
            {schema, table} = split(name),
            do: [schema, table, "table", session.user]

      lookup?(sql) ->
        for name <- matching(sql),
            {schema, table} = split(name),
            do: [Integer.to_string(oid(name)), schema, table]

      schemas?(sql) ->
        for schema <- matching_schemas(sql), do: [schema, session.user]

      true ->
        describe(sql)
    end
  end

  # \dn: SELECT n.nspname AS "Name", pg_get_userbyid(n.nspowner) AS "Owner"
  defp schemas?(sql),
    do: sql =~ ~r/SELECT\s+n\.nspname AS "Name",\s*pg_catalog\.pg_get_userbyid\(n\.nspowner\)/s

  defp matching_schemas(sql) do
    {:ok, schemas} = Kurwa.Registry.schemas()
    names = Enum.uniq(["public" | schemas])

    case Regex.run(~r/nspname OPERATOR\(pg_catalog\.~\) '((?:[^']|'')*)'/, sql) do
      [_, pattern] -> filter(names, pattern)
      nil -> names
    end
    |> Enum.sort()
  end

  @doc """
  A set name as PostgreSQL sees it: `{schema, table}`. A dotted name is a
  table in a schema; any other is in public.
  """
  def split(:default), do: {"public", Parser.default_table()}

  def split(name) do
    case String.split(name, ".", parts: 2) do
      [schema, table] -> {schema, table}
      [table] -> {"public", table}
    end
  end

  defp listing?(sql), do: sql =~ "pg_get_userbyid(c.relowner)" and sql =~ "c.relkind IN"

  defp lookup?(sql),
    do: sql =~ ~r/SELECT\s+c\.oid,\s*n\.nspname,\s*c\.relname\s+FROM\s+pg_catalog\.pg_class/s

  # What psql asks about a relation once it has its oid. A set is an ordinary
  # heap table with no indexes, rules, triggers or policies, and one column.
  @relation %{
    "relchecks" => "0",
    "relkind" => "r",
    "relhasindex" => "f",
    "relhasrules" => "f",
    "relhastriggers" => "f",
    "relrowsecurity" => "f",
    "relforcerowsecurity" => "f",
    "relhasoids" => "f",
    "relispartition" => "f",
    "reltablespace" => "0",
    "relpersistence" => "p",
    "relreplident" => "d",
    "amname" => "heap"
  }

  @column %{
    "attname" => "key",
    "format_type" => "text",
    "attnotnull" => "t",
    "attidentity" => "",
    "attgenerated" => "",
    "attcollation" => nil,
    "attstorage" => "x",
    "attstattarget" => nil,
    "attcompression" => "",
    "col_description" => nil,
    # the default expression, from a subquery psql leaves unnamed
    "?column?" => nil
  }

  defp describe(sql) do
    cond do
      sql =~ ~r/FROM pg_catalog\.pg_class c\b/ and sql =~ ~r/\bc\.oid = '\d+'/ ->
        if by_oid(sql, ~r/\bc\.oid = '(\d+)'/), do: [row(sql, @relation)], else: []

      sql =~ "pg_catalog.pg_attribute a" and sql =~ ~r/\ba\.attrelid = '\d+'/ ->
        if by_oid(sql, ~r/\ba\.attrelid = '(\d+)'/), do: [row(sql, @column)], else: []

      true ->
        Logger.debug("kurwadb pg: answering catalog query with no rows: #{sql}")
        []
    end
  end

  # The set whose oid the query names, or nil.
  defp by_oid(sql, regex) do
    with [_, oid] <- Regex.run(regex, sql),
         name when name != nil <-
           Enum.find(
             [Parser.default_table() | known_sets()],
             &(Integer.to_string(oid(&1)) == oid)
           ) do
      name
    else
      _ -> nil
    end
  end

  defp row(sql, values), do: Enum.map(select_list(sql), &Map.get(values, &1, ""))

  # psql's patterns are POSIX regexes: `relname OPERATOR(pg_catalog.~) '^(...)$'`
  # for the table, and the same on nspname when the pattern names a schema.
  defp matching(sql) do
    names = [Parser.default_table() | known_sets()]

    names =
      case Regex.run(~r/relname OPERATOR\(pg_catalog\.~\) '((?:[^']|'')*)'/, sql) do
        [_, pattern] -> Enum.filter(names, &(&1 |> split() |> elem(1) |> matches?(pattern)))
        nil -> names
      end

    case Regex.run(~r/nspname OPERATOR\(pg_catalog\.~\) '((?:[^']|'')*)'/, sql) do
      [_, pattern] -> Enum.filter(names, &(&1 |> split() |> elem(0) |> matches?(pattern)))
      nil -> names
    end
    |> Enum.sort_by(&split/1)
  end

  defp filter(names, pattern), do: Enum.filter(names, &matches?(&1, pattern))

  defp matches?(name, pattern) do
    case Regex.compile(String.replace(pattern, "''", "'")) do
      {:ok, regex} -> Regex.match?(regex, name)
      {:error, _} -> false
    end
  end

  @doc "A stable oid for a set, so psql's follow-up queries can name it."
  def oid(name), do: 16_384 + :erlang.phash2(name, 1_000_000_000)

  defp known_sets do
    case Namespace.list() do
      {:ok, %{sets: sets}} -> sets
      _ -> []
    end
  end

  # ------------------------------------------------------------- select list

  @doc """
  The output column names of a query's top-level select list, named the way
  PostgreSQL names them: the alias if there is one, else the last identifier
  of a column reference or the name of a function, else `?column?`.
  """
  def select_list(sql) do
    case top_level_items(sql) do
      [] -> ["?column?"]
      items -> Enum.map(items, &item_name/1)
    end
  end

  @doc "The raw text of each top-level select-list item, as written."
  def select_items(sql), do: sql |> top_level_items() |> Enum.map(&String.trim/1)

  defp top_level_items(sql) do
    case Regex.run(~r/^\s*(?:\/\*.*?\*\/\s*)*SELECT\s+(?:DISTINCT\s+)?/is, sql, return: :index) do
      [{start, len}] ->
        body = binary_part(sql, start + len, byte_size(sql) - start - len)
        body |> take_until_from(0, []) |> split_commas(0, [], [])

      nil ->
        []
    end
  end

  # Characters up to the FROM that belongs to this SELECT, skipping parentheses
  # and quotes so a subquery's FROM does not end the list early.
  defp take_until_from(<<>>, _depth, acc), do: finish(acc)

  defp take_until_from(<<q, rest::binary>>, depth, acc) when q in [?', ?"] do
    {quoted, rest} = quoted(rest, q, [q])
    take_until_from(rest, depth, [quoted | acc])
  end

  defp take_until_from(<<?(, rest::binary>>, depth, acc),
    do: take_until_from(rest, depth + 1, [?( | acc])

  defp take_until_from(<<?), rest::binary>>, depth, acc),
    do: take_until_from(rest, depth - 1, [?) | acc])

  # The list ends at the first top-level clause keyword: FROM usually, but a
  # SELECT with no FROM can go straight to LIMIT, WHERE or the end.
  @ends ~w(from where limit order group having union into for offset window)

  defp take_until_from(<<c, rest::binary>>, 0, acc) when c in ~c" \t\r\n" do
    word = rest |> String.split(~r/[^A-Za-z]/, parts: 2) |> hd() |> String.downcase()
    after_word = binary_part(rest, byte_size(word), byte_size(rest) - byte_size(word))

    if word in @ends and (after_word == "" or String.first(after_word) =~ ~r/[\s(;]/),
      do: finish(acc),
      else: take_until_from(rest, 0, [c | acc])
  end

  defp take_until_from(<<c, rest::binary>>, depth, acc),
    do: take_until_from(rest, depth, [c | acc])

  defp quoted(<<q, q, rest::binary>>, q, acc), do: quoted(rest, q, [q, q | acc])
  defp quoted(<<q, rest::binary>>, q, acc), do: {finish([q | acc]), rest}
  defp quoted(<<c, rest::binary>>, q, acc), do: quoted(rest, q, [c | acc])
  defp quoted(<<>>, _q, acc), do: {finish(acc), <<>>}

  defp finish(acc), do: acc |> Enum.reverse() |> IO.iodata_to_binary()

  defp split_commas(<<>>, _depth, current, items), do: Enum.reverse([finish(current) | items])

  defp split_commas(<<q, rest::binary>>, depth, current, items) when q in [?', ?"] do
    {quoted, rest} = quoted(rest, q, [q])
    split_commas(rest, depth, [quoted | current], items)
  end

  defp split_commas(<<?(, rest::binary>>, depth, current, items),
    do: split_commas(rest, depth + 1, [?( | current], items)

  defp split_commas(<<?), rest::binary>>, depth, current, items),
    do: split_commas(rest, depth - 1, [?) | current], items)

  defp split_commas(<<?,, rest::binary>>, 0, current, items),
    do: split_commas(rest, 0, [], [finish(current) | items])

  defp split_commas(<<c, rest::binary>>, depth, current, items),
    do: split_commas(rest, depth, [c | current], items)

  defp item_name(item) do
    item = String.trim(item)

    cond do
      match = Regex.run(~r/\s+AS\s+"((?:[^"]|"")+)"\s*$/is, item) ->
        match |> List.last() |> String.replace(~s(""), ~s("))

      match = Regex.run(~r/\s+AS\s+([A-Za-z_][A-Za-z0-9_$]*)\s*$/is, item) ->
        match |> List.last() |> String.downcase()

      match = Regex.run(~r/^(?:[A-Za-z_][A-Za-z0-9_]*\.)*([A-Za-z_][A-Za-z0-9_]*)$/, item) ->
        match |> List.last() |> String.downcase()

      match = Regex.run(~r/^(?:[A-Za-z_][A-Za-z0-9_]*\.)*([A-Za-z_][A-Za-z0-9_]*)\s*\(/, item) ->
        match |> List.last() |> String.downcase()

      Regex.match?(~r/^CASE\b/i, item) ->
        "case"

      true ->
        "?column?"
    end
  end
end
