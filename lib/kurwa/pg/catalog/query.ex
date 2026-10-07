defmodule Kurwa.Pg.Catalog.Query do
  @moduledoc """
  Runs a query against the catalog tables in `Kurwa.Pg.Catalog.Tables`.

  Tools do not send one fixed set of catalog queries: DBeaver, DataGrip and
  the drivers each build theirs from fragments, joined and filtered as the
  moment needs. Recognising them one string at a time does not keep up, so
  this evaluates the shape they share instead:

      SELECT items FROM table alias [[LEFT|INNER] JOIN table alias ON a.x = b.y]...
      [WHERE conjunction] [ORDER BY ...] [LIMIT n]

  `alias.*` expands to the table's columns. Joins match on the equalities in
  their ON clause. WHERE keeps a row unless a condition it understands - a
  comparison, IN, IS NULL, LIKE, a regex match, a boolean column - says no; a
  condition it does not understand lets the row through, so the answer errs
  towards listing too much rather than hiding a set. Functions the tools call
  in the select list are answered where kurwadb has an answer (current_database,
  pg_get_userbyid, format_type, ...) and NULL elsewhere.

  `run/3` returns `:unknown` when the query reads a catalog table that is not
  modelled, so the caller can fall back to an empty result.
  """

  alias Kurwa.Pg.Catalog
  alias Kurwa.Pg.Catalog.Tables
  alias Kurwa.Sql.Lexer

  @clause_ends ~w(where order group limit union having offset for window)
  @join_words ~w(left right full inner cross join natural)

  @doc "`{:ok, columns, rows}` or `:unknown`."
  def run(sql, ctx, params) do
    with {:ok, tokens} <- Lexer.tokenize(sql, :pg),
         {:ok, query} <- parse(tokens),
         {:ok, sources} <- load(query.from, ctx) do
      envs = sources |> join() |> Enum.filter(&where?(query.where, &1, params))
      {columns, values} = project(query.items, sources, envs, params, ctx)
      rows = values |> order(query.order, columns) |> limit(query.limit)
      {:ok, columns, rows}
    else
      _ -> :unknown
    end
  rescue
    _ -> :unknown
  end

  # ---------------------------------------------------------------- parsing

  defp parse(tokens) do
    tokens = Enum.reject(tokens, &(&1 == {:op, ";"}))

    case tokens do
      [{:ident, "select"} | rest] ->
        rest = drop_distinct(rest)
        {items, rest} = until(rest, &(&1 == {:ident, "from"}))

        case rest do
          [{:ident, "from"} | rest] ->
            {from, rest} = until(rest, &clause_end?/1)
            {where, rest} = clause(rest, "where")
            {_group, rest} = clause(rest, "group")
            {order, rest} = clause(rest, "order")
            {limit, _rest} = clause(rest, "limit")

            with {:ok, from} <- sources(from) do
              {:ok,
               %{
                 items: split(items, {:op, ","}),
                 from: from,
                 where: where,
                 order: order(order),
                 limit: limit(limit)
               }}
            end

          _ ->
            :error
        end

      _ ->
        :error
    end
  end

  defp drop_distinct([{:ident, "distinct"} | rest]), do: rest
  defp drop_distinct(rest), do: rest

  defp clause_end?({:ident, word}), do: word in @clause_ends
  defp clause_end?(_), do: false

  defp clause([{:ident, word} | rest], word) do
    rest =
      case rest do
        [{:ident, "by"} | rest] -> rest
        rest -> rest
      end

    until(rest, &(clause_end?(&1) and &1 != {:ident, word}))
  end

  defp clause(rest, _word), do: {[], rest}

  defp order([]), do: []

  defp order(tokens) do
    for item <- split(tokens, {:op, ","}) do
      case Enum.reverse(item) do
        [{:ident, "desc"} | expr] -> {Enum.reverse(expr), :desc}
        [{:ident, "asc"} | expr] -> {Enum.reverse(expr), :asc}
        _ -> {item, :asc}
      end
    end
  end

  defp limit([{:number, n} | _]) when is_integer(n), do: n
  defp limit(_), do: nil

  # FROM: a first table, then joins. Each source is {table, alias, kind, on}.
  defp sources(tokens) do
    {first, rest} = until(tokens, &join_start?/1)

    with {:ok, table, alias} <- table_ref(first) do
      joins(rest, [{table, alias, :first, []}])
    end
  end

  defp join_start?({:op, ","}), do: true
  defp join_start?({:ident, word}), do: word in @join_words
  defp join_start?(_), do: false

  defp joins([], acc), do: {:ok, Enum.reverse(acc)}

  defp joins(tokens, acc) do
    {kind, rest} = join_kind(tokens)
    {ref, rest} = until(rest, &(&1 == {:ident, "on"} or join_start?(&1)))

    {on, rest} =
      case rest do
        [{:ident, "on"} | rest] -> until(rest, &join_start?/1)
        rest -> {[], rest}
      end

    case table_ref(ref) do
      {:ok, table, alias} -> joins(rest, [{table, alias, kind, on} | acc])
      :error -> :error
    end
  end

  defp join_kind([{:op, ","} | rest]), do: {:cross, rest}
  defp join_kind([{:ident, "cross"}, {:ident, "join"} | rest]), do: {:cross, rest}
  defp join_kind([{:ident, "natural"} | rest]), do: join_kind(rest)
  defp join_kind([{:ident, "inner"}, {:ident, "join"} | rest]), do: {:inner, rest}
  defp join_kind([{:ident, "join"} | rest]), do: {:inner, rest}

  defp join_kind([{:ident, side} | rest]) when side in ~w(left right full) do
    case rest do
      [{:ident, "outer"}, {:ident, "join"} | rest] -> {:left, rest}
      [{:ident, "join"} | rest] -> {:left, rest}
    end
  end

  # [schema .] name [AS] [alias]; a subquery or a function is a table kurwadb
  # does not model.
  defp table_ref(tokens) do
    tokens = Enum.reject(tokens, &(&1 == {:ident, "lateral"}))

    {name, rest} =
      case tokens do
        [{_, schema}, {:op, "."}, {t, table} | rest] when t in [:ident, :qident] ->
          if schema == "pg_catalog", do: {table, rest}, else: {schema <> "." <> table, rest}

        [{t, table} | rest] when t in [:ident, :qident] ->
          {table, rest}

        _ ->
          {:subquery, tokens}
      end

    case {name, rest} do
      {:subquery, _} -> :error
      {name, [{:op, "("} | _]} -> {:ok, {:function, name}, name}
      {name, [{:ident, "as"}, {_, alias} | _]} -> {:ok, name, alias}
      {name, [{t, alias} | _]} when t in [:ident, :qident] -> {:ok, name, alias}
      {name, []} -> {:ok, name, name}
      _ -> :error
    end
  end

  # ---------------------------------------------------------------- loading

  defp load(from, ctx) do
    Enum.reduce_while(from, {:ok, []}, fn {table, alias, kind, on}, {:ok, acc} ->
      case Tables.table(table, ctx) do
        {columns, rows} ->
          {:cont,
           {:ok, acc ++ [%{alias: alias, kind: kind, on: on, columns: columns, rows: rows}]}}

        nil when kind in [:left] ->
          {:cont, {:ok, acc ++ [%{alias: alias, kind: kind, on: on, columns: [], rows: []}]}}

        nil ->
          {:halt, :unknown}
      end
    end)
  end

  # ---------------------------------------------------------------- joining

  # An env maps each alias to its row (a map) or nil for an unmatched left join.
  defp join([first | rest]) do
    envs = for row <- first.rows, do: %{first.alias => row}

    Enum.reduce(rest, envs, fn source, envs ->
      Enum.flat_map(envs, fn env ->
        matches = Enum.filter(source.rows, &where?(source.on, Map.put(env, source.alias, &1), []))

        case {matches, source.kind} do
          {[], :left} -> [Map.put(env, source.alias, nil)]
          {matches, _} -> Enum.map(matches, &Map.put(env, source.alias, &1))
        end
      end)
    end)
  end

  # ------------------------------------------------------------- conditions

  defp where?([], _env, _params), do: true

  defp where?(tokens, env, params) do
    tokens
    |> split({:ident, "and"})
    |> Enum.all?(fn condition -> condition(strip_parens(condition), env, params) != false end)
  end

  # true, false, or :unknown (which lets the row through).
  defp condition(tokens, env, params) do
    if Enum.member?(top_level(tokens), {:ident, "or"}) do
      tokens
      |> split({:ident, "or"})
      |> Enum.map(&condition(strip_parens(&1), env, params))
      |> Enum.reduce(false, fn
        true, _ -> true
        _, true -> true
        :unknown, _ -> :unknown
        _, acc -> acc
      end)
    else
      simple(tokens, env, params)
    end
  end

  defp simple([{:ident, "not"} | rest], env, params) do
    case condition(strip_parens(rest), env, params) do
      true -> false
      false -> true
      :unknown -> :unknown
    end
  end

  defp simple(tokens, env, params) do
    top = top_level_indexed(tokens)

    cond do
      i = find(top, &match?({:op, op} when op in ~w(= <> != < > <= >= ~ !~ ~*), &1)) ->
        {left, [{:op, op} | right]} = Enum.split(tokens, i)
        compare(op, value(left, env, params), value(right, env, params))

      i = find(top, &(&1 == {:ident, "operator"})) ->
        {left, [_, {:op, "("} | rest]} = Enum.split(tokens, i)
        {inside, [{:op, ")"} | right]} = until(rest, &(&1 == {:op, ")"}))
        op = inside |> List.last() |> elem(1)
        compare(op, value(left, env, params), value(right, env, params))

      i = find(top, &(&1 == {:ident, "in"})) ->
        {left, [_ | right]} = Enum.split(tokens, i)
        {left, negate} = negated(left)
        member(value(left, env, params), right, env, params) |> negate.()

      i = find(top, &(&1 == {:ident, "like"} or &1 == {:ident, "ilike"})) ->
        {left, [{:ident, kind} | right]} = Enum.split(tokens, i)
        {left, negate} = negated(left)
        like(kind, value(left, env, params), value(right, env, params)) |> negate.()

      i = find(top, &(&1 == {:ident, "is"})) ->
        {left, [_ | right]} = Enum.split(tokens, i)
        v = value(left, env, params)

        case right do
          [{:ident, "null"}] -> known(v, &is_nil/1)
          [{:ident, "not"}, {:ident, "null"}] -> known(v, &(&1 != nil))
          [{:ident, "true"}] -> known(v, &(&1 == "t"))
          [{:ident, "false"}] -> known(v, &(&1 == "f"))
          _ -> :unknown
        end

      true ->
        case value(tokens, env, params) do
          "t" -> true
          "f" -> false
          nil -> false
          _ -> :unknown
        end
    end
  end

  defp negated(left) do
    case Enum.reverse(left) do
      [{:ident, "not"} | rest] -> {Enum.reverse(rest), &negate/1}
      _ -> {left, & &1}
    end
  end

  defp negate(true), do: false
  defp negate(false), do: true
  defp negate(:unknown), do: :unknown

  defp known(:unknown, _f), do: :unknown
  defp known(v, f), do: f.(v)

  defp compare(_op, :unknown, _), do: :unknown
  defp compare(_op, _, :unknown), do: :unknown
  defp compare(_op, nil, _), do: false
  defp compare(_op, _, nil), do: false
  defp compare("=", a, b), do: equal?(a, b)
  defp compare(op, a, b) when op in ["<>", "!="], do: not equal?(a, b)
  defp compare("~", a, b), do: regex?(a, b, "")
  defp compare("~*", a, b), do: regex?(a, b, "i")
  defp compare("!~", a, b), do: not regex?(a, b, "")
  defp compare(op, a, b), do: ordered(op, sort_key(a), sort_key(b))

  defp ordered("<", a, b), do: a < b
  defp ordered(">", a, b), do: a > b
  defp ordered("<=", a, b), do: a <= b
  defp ordered(">=", a, b), do: a >= b

  defp equal?(a, b), do: to_string(a) == to_string(b)

  defp regex?(a, b, flags) do
    case Regex.compile(to_string(b), flags) do
      {:ok, regex} -> Regex.match?(regex, to_string(a))
      _ -> :unknown
    end
  end

  defp member(:unknown, _list, _env, _params), do: :unknown
  defp member(nil, _list, _env, _params), do: false

  defp member(v, tokens, env, params) do
    case strip_parens(tokens) do
      [{:ident, "select"} | _] ->
        :unknown

      inner ->
        values = inner |> split({:op, ","}) |> Enum.map(&value(&1, env, params))
        if :unknown in values, do: :unknown, else: Enum.any?(values, &equal?(v, &1))
    end
  end

  defp like(_kind, :unknown, _), do: :unknown
  defp like(_kind, _, :unknown), do: :unknown
  defp like(_kind, nil, _), do: false
  defp like(_kind, _, nil), do: false

  defp like(kind, v, pattern) do
    regex =
      pattern
      |> to_string()
      |> Regex.escape()
      |> String.replace("%", ".*")
      |> String.replace("_", ".")

    Regex.match?(Regex.compile!("^" <> regex <> "$", if(kind == "ilike", do: "i", else: "")), v)
  end

  # ------------------------------------------------------------- expressions

  # A value: text, nil for NULL, or :unknown for what this cannot evaluate.
  defp value(tokens, env, params, ctx \\ %{}) do
    tokens = tokens |> strip_casts() |> strip_collate() |> strip_parens()

    case tokens do
      [{:param, n}] ->
        case Enum.at(params, n - 1, :unknown) do
          :unknown -> :unknown
          nil -> nil
          v -> to_string(v)
        end

      [{:string, s}] ->
        s

      [{:number, n}] ->
        to_string(n)

      [{:ident, "null"}] ->
        nil

      [{:ident, b}] when b in ~w(true false) ->
        if b == "true", do: "t", else: "f"

      [{:ident, f}] when f in ~w(current_user session_user user current_role) ->
        Map.get(ctx, :user, :unknown)

      [{:ident, "current_catalog"}] ->
        "kurwadb"

      [{:ident, "case"} | rest] ->
        case_expr(rest, env, params, ctx)

      [{t1, a}, {:op, "."}, {t2, c}] when t1 in [:ident, :qident] and t2 in [:ident, :qident] ->
        column(env, a, c)

      [{t, c}] when t in [:ident, :qident] ->
        column(env, nil, c)

      [{_, schema}, {:op, "."}, {:ident, f}, {:op, "("} | rest] when schema == "pg_catalog" ->
        function(f, rest, env, params, ctx)

      [{:ident, f}, {:op, "("} | rest] ->
        function(f, rest, env, params, ctx)

      _ ->
        :unknown
    end
  end

  defp column(env, nil, c) do
    env
    |> Map.values()
    |> Enum.find_value(:unknown, fn
      %{^c => v} -> {:found, v}
      _ -> nil
    end)
    |> case do
      {:found, v} -> v
      other -> other
    end
  end

  defp column(env, alias, c) do
    case Map.fetch(env, alias) do
      {:ok, nil} -> nil
      {:ok, row} -> Map.get(row, c, :unknown)
      :error -> :unknown
    end
  end

  defp function(name, rest, env, params, ctx) do
    {inside, _} = until(rest, &(&1 == {:op, ")"}), true)
    args = if inside == [], do: [], else: split(inside, {:op, ","})
    arg = fn i -> value(Enum.at(args, i, []), env, params, ctx) end

    case name do
      "current_database" -> "kurwadb"
      "current_schema" -> "public"
      "version" -> "PostgreSQL 16.0 (kurwadb)"
      "pg_get_userbyid" -> Map.get(ctx, :user, :unknown)
      "format_type" -> format_type(arg.(0))
      "nullif" -> nullif(arg.(0), arg.(1))
      "coalesce" -> args |> Enum.map(&value(&1, env, params, ctx)) |> Enum.find(&(&1 != nil))
      "lower" -> with v when is_binary(v) <- arg.(0), do: String.downcase(v)
      "upper" -> with v when is_binary(v) <- arg.(0), do: String.upcase(v)
      "pg_table_is_visible" -> "t"
      "pg_type_is_visible" -> "t"
      "has_schema_privilege" -> "t"
      "has_table_privilege" -> "t"
      "has_database_privilege" -> "t"
      "pg_database_size" -> "0"
      "pg_total_relation_size" -> "0"
      "pg_relation_size" -> "0"
      "pg_table_size" -> "0"
      "pg_indexes_size" -> "0"
      _ -> nil
    end
  end

  defp format_type(:unknown), do: :unknown
  defp format_type(nil), do: nil
  defp format_type(oid), do: Tables.type_name(oid)

  defp nullif(a, b) when a == :unknown or b == :unknown, do: :unknown
  defp nullif(a, b), do: if(equal?(a, b), do: nil, else: a)

  # CASE x WHEN v THEN r ... [ELSE e] END, and CASE WHEN cond THEN r ... END.
  defp case_expr(tokens, env, params, ctx) do
    {body, _} = until(tokens, &(&1 == {:ident, "end"}), true)
    {subject, arms} = until(body, &(&1 == {:ident, "when"}))
    {arms, otherwise} = until(arms, &(&1 == {:ident, "else"}))

    result =
      arms
      |> split({:ident, "when"})
      |> Enum.reject(&(&1 == []))
      |> Enum.find_value(fn arm ->
        {test, [{:ident, "then"} | then]} = until(arm, &(&1 == {:ident, "then"}))

        hit =
          if subject == [],
            do: condition(test, env, params) == true,
            else: equal?(value(subject, env, params, ctx), value(test, env, params, ctx))

        if hit, do: {:hit, value(then, env, params, ctx)}
      end)

    case {result, otherwise} do
      {{:hit, v}, _} -> v
      {nil, [{:ident, "else"} | e]} -> value(e, env, params, ctx)
      {nil, _} -> nil
    end
  end

  # ------------------------------------------------------------- projecting

  defp project(items, sources, envs, params, ctx) do
    vctx = %{user: ctx.user}

    expanded =
      Enum.flat_map(items, fn item ->
        case item do
          [{:op, "*"}] ->
            for s <- sources, c <- s.columns, do: {c, fn env -> field(env, s.alias, c) end}

          [{_, alias}, {:op, "."}, {:op, "*"}] ->
            source = Enum.find(sources, &(&1.alias == alias))
            for c <- source.columns, do: {c, fn env -> field(env, alias, c) end}

          tokens ->
            {expr, name} = item_name(tokens)
            [{name, fn env -> known_or_nil(value(expr, env, params, vctx)) end}]
        end
      end)

    columns = for {name, _} <- expanded, do: {name, :text}
    rows = for env <- envs, do: for({_, f} <- expanded, do: f.(env))
    {columns, rows}
  end

  defp field(env, alias, c) do
    case Map.get(env, alias) do
      nil -> nil
      row -> Map.get(row, c)
    end
  end

  defp known_or_nil(:unknown), do: nil
  defp known_or_nil(v), do: v

  # The output name, as PostgreSQL gives it: the alias, else the column or
  # function name, else ?column?.
  defp item_name(tokens) do
    case Enum.reverse(tokens) do
      [{t, alias}, {:ident, "as"} | expr] when t in [:ident, :qident] ->
        {Enum.reverse(expr), alias}

      [{t, alias}, last | expr]
      when t in [:ident, :qident] and last != {:op, "."} and
             last != {:op, "::"} ->
        if alias in ~w(end) or (match?({:op, _}, last) and last != {:op, ")"}),
          do: {tokens, default_name(tokens)},
          else: {Enum.reverse([last | expr]), alias}

      _ ->
        {tokens, default_name(tokens)}
    end
  end

  defp default_name(tokens) do
    case strip_casts(tokens) do
      [{t, c}] when t in [:ident, :qident] -> c
      [{_, _}, {:op, "."}, {t, c}] when t in [:ident, :qident] -> c
      [{_, _}, {:op, "."}, {:ident, f}, {:op, "("} | _] -> f
      [{:ident, "case"} | _] -> "case"
      [{:ident, f}, {:op, "("} | _] -> f
      _ -> "?column?"
    end
  end

  # ------------------------------------------------------- ordering, limits

  defp order(rows, [], _columns), do: rows

  defp order(rows, keys, columns) do
    names = Enum.map(columns, &elem(&1, 0))

    indexed =
      Enum.map(keys, fn {expr, dir} ->
        index =
          case expr do
            [{:number, n}] -> n - 1
            [{_, c}] -> Enum.find_index(names, &(&1 == c))
            [{_, _}, {:op, "."}, {_, c}] -> Enum.find_index(names, &(&1 == c))
            _ -> nil
          end

        {index, dir}
      end)
      |> Enum.reject(fn {index, _} -> index == nil end)

    Enum.sort(rows, fn a, b ->
      Enum.reduce_while(indexed, true, fn {i, dir}, _ ->
        ka = sort_key(Enum.at(a, i))
        kb = sort_key(Enum.at(b, i))

        cond do
          ka == kb -> {:cont, true}
          dir == :asc -> {:halt, ka < kb}
          true -> {:halt, ka > kb}
        end
      end)
    end)
  end

  defp sort_key(nil), do: {2, ""}

  defp sort_key(v) do
    case Integer.parse(to_string(v)) do
      {n, ""} -> {0, n}
      _ -> {1, to_string(v)}
    end
  end

  defp limit(rows, nil), do: rows
  defp limit(rows, n), do: Enum.take(rows, n)

  # --------------------------------------------------------------- tokens

  # Tokens up to the first top-level one for which `stop?` holds.
  defp until(tokens, stop?, inclusive_depth? \\ false),
    do: until(tokens, stop?, inclusive_depth?, 0, [])

  defp until([], _stop?, _i, _depth, acc), do: {Enum.reverse(acc), []}

  defp until([t | rest] = tokens, stop?, i, depth, acc) do
    cond do
      depth == 0 and stop?.(t) -> {Enum.reverse(acc), tokens}
      t == {:op, "("} -> until(rest, stop?, i, depth + 1, [t | acc])
      t == {:op, ")"} and depth == 0 and i -> {Enum.reverse(acc), tokens}
      t == {:op, ")"} -> until(rest, stop?, i, depth - 1, [t | acc])
      t == {:ident, "case"} -> until(rest, stop?, i, depth + 1, [t | acc])
      t == {:ident, "end"} and depth > 0 -> until(rest, stop?, i, depth - 1, [t | acc])
      true -> until(rest, stop?, i, depth, [t | acc])
    end
  end

  defp split(tokens, separator) do
    case until(tokens, &(&1 == separator)) do
      {part, []} -> [part]
      {part, [_ | rest]} -> [part | split(rest, separator)]
    end
  end

  defp top_level(tokens), do: tokens |> top_level_indexed() |> Enum.map(fn {t, _} -> t end)

  defp top_level_indexed(tokens) do
    {acc, _} =
      tokens
      |> Enum.with_index()
      |> Enum.reduce({[], 0}, fn {t, i}, {acc, depth} ->
        cond do
          t == {:op, "("} or t == {:ident, "case"} -> {acc, depth + 1}
          t == {:op, ")"} or (t == {:ident, "end"} and depth > 0) -> {acc, depth - 1}
          depth == 0 -> {[{t, i} | acc], depth}
          true -> {acc, depth}
        end
      end)

    Enum.reverse(acc)
  end

  defp find(indexed, pred) do
    Enum.find_value(indexed, fn {t, i} -> if pred.(t), do: i end)
  end

  defp strip_parens([{:op, "("} | _] = tokens) do
    case until(tl(tokens), fn _ -> false end, true) do
      {inner, [{:op, ")"}]} -> strip_parens(inner)
      _ -> tokens
    end
  end

  defp strip_parens(tokens), do: tokens

  # x::regclass, x::pg_catalog.text: the cast is dropped, the value kept.
  defp strip_casts(tokens) do
    case until(tokens, &(&1 == {:op, "::"})) do
      {before, []} -> before
      {before, _} -> before
    end
  end

  defp strip_collate(tokens) do
    {before, _} = until(tokens, &(&1 == {:ident, "collate"}))
    before
  end

  @doc false
  def ctx(user, sets, schemas), do: %{user: user, sets: sets, schemas: schemas}

  @doc false
  def namespace_oid(schema), do: Tables.namespace_oid(schema)

  _ = &Catalog.oid/1
end
