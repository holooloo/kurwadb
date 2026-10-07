defmodule Kurwa.Sql.Parser do
  @moduledoc """
  The SQL kurwadb understands, parsed into a small tree.

  A set is a table with one column, `key`. The default set is the table
  `kurwa`, which parses as `:default` - not nil, which is a SELECT with no
  FROM; any other table is the named set of that name. What can be said
  about a table is exactly what the store can answer without a scan:

      INSERT INTO seen VALUES ('a'), ('b')             -- add
      INSERT INTO seen (key, ttl) VALUES ('a', 3600)   -- add, expiring in seconds
      SELECT key FROM seen WHERE key = 'a'             -- member: one row or none
      SELECT key FROM seen WHERE key IN ('a', 'b')     -- the members among these
      SELECT key FROM seen WHERE key = ANY($1)         -- the same, with a text[] parameter
      SELECT count(*) FROM seen WHERE key = $1         -- 1 or 0
      SELECT EXISTS (SELECT 1 FROM seen WHERE key = $1)
      DELETE FROM seen WHERE key IN ('a', 'b')

  plus functions for the things a table cannot say - `kurwa_add(set, key[,
  ttl])`, `kurwa_member`, `kurwa_delete`, `kurwa_ttl`, `kurwa_count()` - and the
  utility statements drivers send on their own (`SET`, `SHOW`, `BEGIN`,
  `COMMIT`, `DISCARD`). A query against the system catalogs is passed through
  whole as `{:catalog, sql}`, for the frontend to answer.

  Anything else is an error that says what is missing, and a `SELECT` without a
  `WHERE` on the key says why: it would be a scan, and there are none.
  """

  alias Kurwa.Sql.Lexer

  @default_table "kurwa"
  @default_schemas ~w(public dbo kurwadb)
  @system_schemas ~w(pg_catalog information_schema sys pg_toast)
  @tsql_starts ~w(select insert delete update set use begin commit rollback if declare exec execute print save create drop merge)
  # A query is a catalog query when it reads a catalog table - not when it
  # merely calls pg_catalog.version(), which drivers do on their own.
  @catalog ~r/\b(FROM|JOIN)\s+(pg_catalog\.|information_schema\.|sys\.|master\.|pg_[a-z_]+\b)/i

  @type error :: {:error, sqlstate :: binary(), message :: binary()}

  @doc "Parses one query string - which may hold several statements - into a list of statements."
  @spec parse(binary(), :pg | :mysql | :tsql) :: {:ok, [term()]} | error()
  def parse(sql, dialect \\ :pg) do
    if Regex.match?(@catalog, sql) do
      {:ok, [{:catalog, sql}]}
    else
      with {:ok, tokens} <- lex(sql, dialect) do
        tokens
        |> then(fn tokens ->
          if dialect == :tsql, do: split_tsql(tokens, 0, [], []), else: split(tokens, [], [])
        end)
        |> Enum.reduce_while({:ok, []}, fn statement, {:ok, acc} ->
          case statement(statement) do
            {:ok, ast} -> {:cont, {:ok, [ast | acc]}}
            {:error, _, _} = error -> {:halt, error}
          end
        end)
        |> case do
          {:ok, statements} -> {:ok, Enum.reverse(statements)}
          error -> error
        end
      end
    end
  end

  @doc "The table name of the default set."
  def default_table, do: @default_table

  @doc """
  Parses one statement from tokens already split off - what the procedure
  parser hands over for each data statement in a body.
  """
  def statement_tokens(tokens), do: statement(tokens)

  @doc "Parses one expression - a literal, parameter, call, cast - from the front of `tokens`."
  def expression(tokens), do: expr(tokens)

  @doc "The keywords that start a T-SQL statement, for splitting without semicolons."
  def tsql_starts, do: @tsql_starts

  @doc "Whether a guard `SELECT ... WHERE key = x` names the key a one-row insert adds."
  def guards_insert?(guard, table, columns, row), do: guard_matches?(guard, table, columns, row)

  defp lex(sql, dialect) do
    case Lexer.tokenize(sql, dialect) do
      {:ok, tokens} -> {:ok, tokens}
      {:error, message} -> {:error, "42601", message}
    end
  end

  # An empty query is one empty statement, so the frontend can answer it with
  # EmptyQueryResponse; empty pieces between semicolons are dropped.
  defp split([], [], []), do: [[]]
  defp split([], [], done), do: Enum.reverse(done)
  defp split([], current, done), do: Enum.reverse([Enum.reverse(current) | done])
  defp split([{:op, ";"} | rest], [], done), do: split(rest, [], done)

  defp split([{:op, ";"} | rest], current, done),
    do: split(rest, [], [Enum.reverse(current) | done])

  defp split([token | rest], current, done), do: split(rest, [token | current], done)

  # T-SQL does not need semicolons between statements: "SET NOCOUNT ON SELECT 1"
  # is two. So a batch also splits before a statement keyword at the top level -
  # except right after IF (...), where the keyword is the IF's body.

  defp split_tsql([], _depth, [], []), do: [[]]
  defp split_tsql([], _depth, [], done), do: Enum.reverse(done)
  defp split_tsql([], _depth, current, done), do: Enum.reverse([Enum.reverse(current) | done])
  defp split_tsql([{:op, ";"} | rest], 0, [], done), do: split_tsql(rest, 0, [], done)

  defp split_tsql([{:op, ";"} | rest], 0, current, done),
    do: split_tsql(rest, 0, [], [Enum.reverse(current) | done])

  defp split_tsql([{:op, "("} = t | rest], depth, current, done),
    do: split_tsql(rest, depth + 1, [t | current], done)

  defp split_tsql([{:op, ")"} = t | rest], depth, current, done),
    do: split_tsql(rest, max(depth - 1, 0), [t | current], done)

  defp split_tsql([{:ident, kw} = t | rest], 0, current, done)
       when kw in @tsql_starts and current != [] do
    if body_of_if?(current) or continues?(current, kw),
      do: split_tsql(rest, 0, [t | current], done),
      else: split_tsql(rest, 0, [t], [Enum.reverse(current) | done])
  end

  defp split_tsql([t | rest], depth, current, done),
    do: split_tsql(rest, depth, [t | current], done)

  # IF NOT EXISTS (...) <statement>: the statement right after the condition.
  defp body_of_if?([{:op, ")"} | _] = current), do: List.last(current) == {:ident, "if"}
  defp body_of_if?(_), do: false

  # Keywords that continue the statement they follow rather than start one:
  # SET NOCOUNT ON, BEGIN TRAN, COMMIT TRAN, SET TRANSACTION ..., DELETE inside OUTPUT.
  defp continues?([{:ident, prev} | _], kw)
       when prev in ~w(transaction isolation) and kw == "set", do: true

  defp continues?(current, "set"), do: List.last(current) == {:ident, "update"}
  defp continues?(_, _), do: false

  # ---------------------------------------------------------------- statements

  defp statement([]), do: {:ok, :empty}

  defp statement([{:ident, "select"} | rest]) do
    with {:ok, select, []} <- select(rest) do
      {:ok, select}
    else
      {:ok, _select, [token | _]} -> unexpected(token)
      error -> error
    end
  end

  defp statement([{:ident, "insert"}, {:ident, "into"} | rest]), do: insert(rest)

  # MySQL: INSERT IGNORE is ON CONFLICT DO NOTHING, and REPLACE is an insert,
  # since there is nothing on a key to replace.
  defp statement([{:ident, "insert"}, {:ident, "ignore"} | rest]) do
    with {:ok, {:insert, table, columns, rows, returning, _}} <-
           statement([{:ident, "insert"} | rest]),
         do: {:ok, {:insert, table, columns, rows, returning, :nothing}}
  end

  defp statement([{:ident, "replace"}, {:ident, "into"} | rest]), do: insert(rest)

  defp statement([{:ident, "use"}, {_, database}]), do: {:ok, {:use, database}}

  defp statement([{:ident, d}, {_, _} = table]) when d in ~w(describe desc) do
    with {:ok, set, []} <- table([table]), do: {:ok, {:describe_table, set}}
  end

  defp statement([{:ident, d}, {_, _} = schema, {:op, "."}, {_, _} = table])
       when d in ~w(describe desc) do
    with {:ok, set, []} <- table([schema, {:op, "."}, table]), do: {:ok, {:describe_table, set}}
  end

  defp statement([{:ident, "delete"}, {:ident, "from"} | rest]), do: delete(rest)

  defp statement([{:ident, "create"}, {:ident, "table"} | rest]) do
    {if_not_exists, rest} =
      case rest do
        [{:ident, "if"}, {:ident, "not"}, {:ident, "exists"} | rest] -> {true, rest}
        rest -> {false, rest}
      end

    with :ok <- key_column_only(rest),
         {:ok, table, _columns} <- table(rest),
         do: {:ok, {:create_table, table, if_not_exists}}
  end

  defp statement([{:ident, "create"}, {:ident, "schema"} | rest]) do
    {if_not_exists, rest} =
      case rest do
        [{:ident, "if"}, {:ident, "not"}, {:ident, "exists"} | rest] -> {true, rest}
        rest -> {false, rest}
      end

    with {:ok, schema, rest} <- schema_name(rest) do
      case rest do
        [] -> {:ok, {:create_schema, schema, if_not_exists}}
        [{:ident, "authorization"}, {_, _}] -> {:ok, {:create_schema, schema, if_not_exists}}
        [token | _] -> unexpected(token)
      end
    end
  end

  defp statement([{:ident, "drop"}, {:ident, "schema"} | rest]) do
    {if_exists, rest} =
      case rest do
        [{:ident, "if"}, {:ident, "exists"} | rest] -> {true, rest}
        rest -> {false, rest}
      end

    with {:ok, schema, rest} <- schema_name(rest) do
      case rest do
        [] -> {:ok, {:drop_schema, schema, if_exists}}
        [{:ident, "restrict"}] -> {:ok, {:drop_schema, schema, if_exists}}
        [{:ident, "cascade"}] -> drop_table_error()
        [token | _] -> unexpected(token)
      end
    end
  end

  defp statement([{:ident, "drop"}, {:ident, "table"} | _]), do: drop_table_error()

  defp statement([{:ident, "create"}, {:ident, "database"} | _]),
    do: error("0A000", "kurwadb is one database; CREATE SCHEMA makes a namespace for sets")

  defp statement([{:ident, "update"} | _]),
    do: error("0A000", "a key has no columns to update: delete it and insert the new one")

  defp statement([{:ident, kw} | _]) when kw in ~w(begin start),
    do: {:ok, {:utility, :begin, "BEGIN"}}

  defp statement([{:ident, kw} | _]) when kw in ~w(commit end),
    do: {:ok, {:utility, :commit, "COMMIT"}}

  defp statement([{:ident, kw} | _]) when kw in ~w(rollback abort),
    do: {:ok, {:utility, :rollback, "ROLLBACK"}}

  defp statement([{:ident, "savepoint"} | _]), do: {:ok, {:utility, :savepoint, "SAVEPOINT"}}
  defp statement([{:ident, "save"} | _]), do: {:ok, {:utility, :savepoint, "SAVE TRANSACTION"}}

  # T-SQL's idempotent insert: IF NOT EXISTS (SELECT ... FROM t WHERE key = x)
  # INSERT INTO t VALUES (x) - which is ON CONFLICT DO NOTHING, and goes through
  # add_new, so one of any number of racing batches inserts.
  defp statement([
         {:ident, "if"},
         {:ident, "not"},
         {:ident, "exists"},
         {:op, "("},
         {:ident, "select"} | rest
       ]) do
    with {:ok, {:select, guard}, [{:op, ")"} | body]} <- select(rest),
         {:ok, {:insert, table, columns, [row], returning, _}} <- statement(body),
         true <- guard_matches?(guard, table, columns, row) do
      {:ok, {:insert, table, columns, [row], returning, :nothing}}
    else
      {:error, _, _} = error ->
        error

      _ ->
        error(
          "0A000",
          "the only IF is IF NOT EXISTS (SELECT ... FROM t WHERE [key] = x) INSERT INTO t VALUES (x)"
        )
    end
  end

  # IF EXISTS (...) DELETE: a DELETE already does nothing to a key that is not there.
  defp statement([{:ident, "if"}, {:ident, "exists"}, {:op, "("}, {:ident, "select"} | rest]) do
    with {:ok, {:select, _guard}, [{:op, ")"}, {:ident, "delete"} | _] = after_guard} <-
           select(rest),
         [{:op, ")"} | body] <- after_guard do
      statement(body)
    else
      {:error, _, _} = error -> error
      _ -> error("0A000", "the only IF EXISTS is IF EXISTS (...) DELETE ...")
    end
  end

  defp statement([{:ident, "print"} | rest]) do
    with {:ok, expr, []} <- expr(rest), do: {:ok, {:print, expr}}
  end

  defp statement([{:ident, kw} | _]) when kw in ~w(exec execute),
    do:
      error(
        "0A000",
        "stored procedures are called over the SQL Server protocol; they are .sql files in procedures_dir"
      )

  defp statement([{:ident, "declare"} | _]),
    do:
      error(
        "0A000",
        "DECLARE belongs to T-SQL batches and procedures, over the SQL Server protocol"
      )

  defp statement([{:ident, "release"} | _]), do: {:ok, {:utility, :release, "RELEASE"}}

  defp statement([{:ident, "set"} | rest]), do: {:ok, {:set, setting(rest)}}
  defp statement([{:ident, "reset"} | _]), do: {:ok, {:utility, :reset, "RESET"}}

  defp statement([{:ident, "discard"} | rest]),
    do: {:ok, {:utility, :discard, "DISCARD " <> words(rest)}}

  defp statement([{:ident, "deallocate"} | rest]), do: {:ok, {:deallocate, deallocate(rest)}}

  defp statement([{:ident, "show"} | rest]) do
    case rest do
      [] -> error("42601", "SHOW needs a parameter name")
      words -> {:ok, {:show, words |> Enum.map(&text/1) |> Enum.join(" ")}}
    end
  end

  defp statement([token | _]), do: unexpected(token)

  # ------------------------------------------------------------------- select

  defp select(tokens) do
    with {:ok, top, tokens} <- top(tokens),
         {:ok, items, rest} <- items(tokens, []),
         {:ok, from, rest} <- from(rest),
         {:ok, where, rest} <- where(rest),
         {:ok, limit, rest} <- limit(rest) do
      {:ok, {:select, %{items: items, from: from, where: where, limit: limit || top}}, rest}
    end
  end

  # T-SQL: SELECT TOP n or TOP (n).
  defp top([{:ident, "top"}, {:op, "("} | rest]) do
    with {:ok, expr, [{:op, ")"} | rest]} <- expr(rest), do: {:ok, expr, rest}
  end

  defp top([{:ident, "top"} | rest]) do
    with {:ok, expr, rest} <- primary(rest), do: {:ok, expr, rest}
  end

  defp top(tokens), do: {:ok, nil, tokens}

  defp guard_matches?(%{from: table, where: {:keys, [key]}}, table, columns, row) do
    index = Enum.find_index(columns || ["key"], &(&1 == "key"))
    index != nil and Enum.at(row, index) == key
  end

  defp guard_matches?(_guard, _table, _columns, _row), do: false

  defp items([{:op, "*"} | rest], []), do: after_item(rest, [{:star, "key"}])

  defp items(tokens, acc) do
    with {:ok, expr, rest} <- expr(tokens) do
      {alias_name, rest} = alias_name(rest)
      after_item(rest, [{expr, alias_name || column_name(expr)} | acc])
    end
  end

  defp after_item([{:op, ","} | rest], acc), do: items(rest, acc)
  defp after_item(rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp alias_name([{:ident, "as"}, name | rest]), do: {text(name), rest}

  defp alias_name([{type, name} | rest])
       when (type == :ident and
               name not in ~w(from where limit order group union having values into output on top)) or
              type == :qident,
       do: {name, rest}

  defp alias_name(rest), do: {nil, rest}

  # The column names PostgreSQL itself would give these expressions.
  defp column_name({:col, name}), do: name
  defp column_name({:call, name, _}), do: name
  defp column_name(:count_star), do: "count"
  defp column_name({:exists, _}), do: "exists"
  defp column_name({:sysvar, name}), do: "@@" <> name
  defp column_name({:uservar, name}), do: "@" <> name
  defp column_name({:cast, expr, _type}), do: column_name(expr)
  defp column_name(_), do: "?column?"

  defp from([{:ident, "from"} | rest]) do
    with {:ok, table, rest} <- table(rest), do: {:ok, table, rest}
  end

  defp from(rest), do: {:ok, nil, rest}

  # `key` is a reserved word in MySQL, so there it is written `key`.
  defp where([{:ident, "where"}, {:qident, "key"} | rest]),
    do: where([{:ident, "where"}, {:ident, "key"} | rest])

  defp where([{:ident, "where"} | rest]) do
    case rest do
      # What node-postgres and psycopg send for a list: one array parameter.
      [{:ident, "key"}, {:op, "="}, {:ident, "any"}, {:op, "("} | rest] ->
        with {:ok, expr, [{:op, ")"} | rest]} <- expr(rest) do
          {:ok, {:any, expr}, rest}
        else
          {:ok, _, _} -> error("42601", "expected ) after ANY (...")
          error -> error
        end

      [{:ident, "key"}, {:op, "="} | rest] ->
        with {:ok, expr, rest} <- expr(rest), do: {:ok, {:keys, [expr]}, rest}

      [{:ident, "key"}, {:ident, "in"}, {:op, "("} | rest] ->
        with {:ok, exprs, rest} <- list(rest, []), do: {:ok, {:keys, exprs}, rest}

      _ ->
        with {:ok, expr, [{:op, "="}, {:ident, "key"} | rest]} <- expr(rest) do
          {:ok, {:keys, [expr]}, rest}
        else
          _ -> error("0A000", "the only condition is on the key: WHERE key = ... or key IN (...)")
        end
    end
  end

  defp where(rest), do: {:ok, nil, rest}

  defp limit([{:ident, "limit"} | rest]) do
    with {:ok, expr, rest} <- expr(rest), do: {:ok, expr, rest}
  end

  defp limit(rest), do: {:ok, nil, rest}

  # ------------------------------------------------------------------- insert

  defp insert(tokens) do
    with {:ok, table, rest} <- table(tokens),
         {:ok, columns, rest} <- columns(rest),
         {:ok, output, rest} <- output(rest),
         {:ok, rows, rest} <- values(rest),
         {:ok, conflict, returning, rest} <- conflict_and_returning(rest),
         :ok <- finished(rest),
         :ok <- arity(columns, rows) do
      {:ok, {:insert, table, columns, rows, returning || output, conflict}}
    end
  end

  # T-SQL's OUTPUT inserted.[key] / deleted.[key], which is RETURNING.
  defp output([{:ident, "output"} | rest]), do: items(rest, [])
  defp output(rest), do: {:ok, nil, rest}

  defp columns([{:op, "("} | rest]) do
    with {:ok, names, rest} <- names(rest, []) do
      case Enum.reject(names, &(&1 in ["key", "ttl"])) do
        [] ->
          if "key" in names,
            do: {:ok, names, rest},
            else: error("42703", "an insert needs the key column")

        [other | _] ->
          error("42703", "column \"#{other}\" does not exist: a set has key, and ttl in seconds")
      end
    end
  end

  defp columns(rest), do: {:ok, nil, rest}

  defp names([name, {:op, ","} | rest], acc), do: names(rest, [text(name) | acc])
  defp names([name, {:op, ")"} | rest], acc), do: {:ok, Enum.reverse([text(name) | acc]), rest}
  defp names(_, _), do: error("42601", "expected a column list")

  defp values([{:ident, "values"} | rest]), do: rows(rest, [])
  defp values([token | _]), do: unexpected(token)
  defp values([]), do: error("42601", "expected VALUES")

  defp rows([{:op, "("} | rest], acc) do
    with {:ok, exprs, rest} <- list(rest, []) do
      case rest do
        [{:op, ","} | rest] -> rows(rest, [exprs | acc])
        rest -> {:ok, Enum.reverse([exprs | acc]), rest}
      end
    end
  end

  defp rows(_, _), do: error("42601", "expected a row of values")

  defp conflict_and_returning([{:ident, "on"}, {:ident, "duplicate"} | _]),
    do: error("0A000", "ON DUPLICATE KEY UPDATE: a key has nothing to update; use INSERT IGNORE")

  # ON CONFLICT [(key)] DO NOTHING makes the insert conditional, so its count
  # says which rows were new - the SQL spelling of SET NX. DO UPDATE has nothing
  # to update on a key.
  defp conflict_and_returning(tokens) do
    conflict =
      case tokens do
        [{:ident, "on"}, {:ident, "conflict"} | rest] ->
          if Enum.any?(rest, &(&1 == {:ident, "update"})),
            do:
              error("0A000", "ON CONFLICT DO UPDATE: a key has nothing to update; use DO NOTHING"),
            else: {:nothing, skip_to(rest, "nothing")}

        rest ->
          {nil, rest}
      end

    with {conflict, rest} when conflict in [nil, :nothing] <- conflict do
      case rest do
        [{:ident, "returning"} | rest] ->
          with {:ok, items, rest} <- items(rest, []), do: {:ok, conflict, items, rest}

        rest ->
          {:ok, conflict, nil, rest}
      end
    end
  end

  defp skip_to([{:ident, word} | rest], word), do: rest
  defp skip_to([_ | rest], word), do: skip_to(rest, word)
  defp skip_to([], _word), do: []

  defp arity(columns, rows) do
    width = length(columns || ["key"])

    cond do
      columns == nil and Enum.all?(rows, &(length(&1) in [1, 2])) -> :ok
      Enum.all?(rows, &(length(&1) == width)) -> :ok
      true -> error("42601", "VALUES rows do not match the column list")
    end
  end

  # ------------------------------------------------------------------- delete

  defp delete(tokens) do
    with {:ok, table, rest} <- table(tokens),
         {:ok, output, rest} <- output(rest),
         {:ok, where, rest} <- where(rest),
         {:ok, _conflict, returning, rest} <- conflict_and_returning(rest),
         :ok <- finished(rest) do
      returning = returning || output

      case where do
        nil ->
          error("0A000", "DELETE without WHERE key = ... would be a scan, and there are none")

        where ->
          {:ok, {:delete, table, where, returning}}
      end
    end
  end

  # -------------------------------------------------------------- expressions

  defp expr(tokens) do
    with {:ok, expr, rest} <- primary(tokens), do: casts(expr, rest)
  end

  defp casts(expr, [{:op, "::"}, type, {:op, "["}, {:op, "]"} | rest]),
    do: casts({:cast, expr, text(type) <> "[]"}, rest)

  defp casts(expr, [{:op, "::"}, type | rest]), do: casts({:cast, expr, text(type)}, rest)
  defp casts(expr, rest), do: {:ok, expr, rest}

  defp primary([{:string, s} | rest]), do: {:ok, {:lit, s}, rest}
  defp primary([{:number, n} | rest]), do: {:ok, {:lit, n}, rest}
  defp primary([{:op, "-"}, {:number, n} | rest]), do: {:ok, {:lit, -n}, rest}
  defp primary([{:param, n} | rest]), do: {:ok, {:param, n}, rest}
  defp primary([{:ident, "null"} | rest]), do: {:ok, {:lit, nil}, rest}

  # MySQL system variables, @@name or @@session.name / @@global.name, and user
  # variables, @name, which are never set here and so are NULL.
  defp primary([{:op, "@"}, {:op, "@"}, {:ident, scope}, {:op, "."}, {_, name} | rest])
       when scope in ~w(session global local),
       do: {:ok, {:sysvar, String.downcase(name)}, rest}

  defp primary([{:op, "@"}, {:op, "@"}, {_, name} | rest]),
    do: {:ok, {:sysvar, String.downcase(name)}, rest}

  defp primary([{:op, "@"}, {_, name} | rest]), do: {:ok, {:uservar, name}, rest}
  defp primary([{:ident, "true"} | rest]), do: {:ok, {:lit, true}, rest}
  defp primary([{:ident, "false"} | rest]), do: {:ok, {:lit, false}, rest}

  defp primary([{:ident, "array"}, {:op, "["}, {:op, "]"} | rest]), do: {:ok, {:array, []}, rest}

  defp primary([{:ident, "array"}, {:op, "["} | rest]) do
    with {:ok, exprs, rest} <- list(rest, [], "]"), do: {:ok, {:array, exprs}, rest}
  end

  defp primary([{:ident, "count"}, {:op, "("}, {:op, "*"}, {:op, ")"} | rest]),
    do: {:ok, :count_star, rest}

  defp primary([{:ident, "exists"}, {:op, "("}, {:ident, "select"} | rest]) do
    with {:ok, {:select, select}, [{:op, ")"} | rest]} <- select(rest) do
      {:ok, {:exists, select}, rest}
    else
      {:ok, _, _} -> error("42601", "expected ) after EXISTS (SELECT ...")
      error -> error
    end
  end

  defp primary([{:ident, "cast"}, {:op, "("} | rest]) do
    with {:ok, expr, [{:ident, "as"}, type, {:op, ")"} | rest]} <- expr(rest) do
      {:ok, {:cast, expr, text(type)}, rest}
    else
      _ -> error("42601", "expected CAST(expr AS type)")
    end
  end

  defp primary([{:op, "("} | rest]) do
    with {:ok, expr, [{:op, ")"} | rest]} <- expr(rest) do
      {:ok, expr, rest}
    else
      {:ok, _, _} -> error("42601", "expected )")
      error -> error
    end
  end

  # Niladic functions the SQL standard spells without parentheses - and
  # PostgreSQL accepts current_schema() with them.
  defp primary([{:ident, name}, {:op, "("}, {:op, ")"} | rest])
       when name in ~w(current_user session_user current_schema current_catalog user),
       do: {:ok, {:call, name, []}, rest}

  defp primary([{:ident, name} | rest])
       when name in ~w(current_user session_user current_schema current_catalog user),
       do: {:ok, {:call, name, []}, rest}

  defp primary([{type, name}, {:op, "."}, {_, fname}, {:op, "("} | rest])
       when type in [:ident, :qident] and name in ["pg_catalog", "public"],
       do: call(fname, rest)

  defp primary([{:named, name} | rest]), do: {:ok, {:param, name}, rest}
  defp primary([{_, name}, {:op, "("} | rest]), do: call(name, rest)

  # A qualified column - inserted.key, t.[key] - is the column.
  defp primary([{t1, _qualifier}, {:op, "."}, {t2, name} | rest])
       when t1 in [:ident, :qident] and t2 in [:ident, :qident],
       do: {:ok, {:col, name}, rest}

  defp primary([{type, name} | rest]) when type in [:ident, :qident],
    do: {:ok, {:col, name}, rest}

  defp primary([token | _]), do: unexpected(token)
  defp primary([]), do: error("42601", "unexpected end of statement")

  defp call(name, [{:op, ")"} | rest]), do: {:ok, {:call, name, []}, rest}

  defp call(name, rest) do
    with {:ok, args, rest} <- list(rest, []), do: {:ok, {:call, name, args}, rest}
  end

  # A comma-separated list of expressions, ending at ")" - or "]" for ARRAY[...].
  defp list(tokens, acc, close \\ ")") do
    with {:ok, expr, rest} <- expr(tokens) do
      case rest do
        [{:op, ","} | rest] -> list(rest, [expr | acc], close)
        [{:op, ^close} | rest] -> {:ok, Enum.reverse([expr | acc]), rest}
        [token | _] -> unexpected(token)
        [] -> error("42601", "expected #{close}")
      end
    end
  end

  # A set is one column, key text (and ttl, which is not stored as a column).
  # A table designer's column1 would be accepted here and fail on the first
  # INSERT, so it fails here instead, saying what to name it.
  defp key_column_only(tokens) do
    case Enum.drop_while(tokens, &(&1 != {:op, "("})) do
      [{:op, "("} | _] = definition ->
        names =
          definition
          |> drop_parens_keep()
          |> Enum.map(&List.first/1)
          |> Enum.reject(
            &(&1 in [
                nil
                | Enum.map(~w(primary constraint unique check foreign), fn w -> {:ident, w} end)
              ])
          )
          |> Enum.map(fn {_, name} -> name end)

        case Enum.reject(names, &(&1 in ~w(key ttl))) do
          [] ->
            :ok

          [name | _] ->
            error(
              "0A000",
              "a set has one column, key text; name the column key instead of #{name}: " <>
                "CREATE TABLE t (key text)"
            )
        end

      _ ->
        :ok
    end
  end

  # The top-level comma-separated items inside the first parentheses.
  defp drop_parens_keep([{:op, "("} | rest]), do: items_in(rest, 0, [], [])

  defp items_in([], _depth, current, acc), do: Enum.reverse([Enum.reverse(current) | acc])

  defp items_in([{:op, ")"} | _], 0, current, acc),
    do: Enum.reverse([Enum.reverse(current) | acc])

  defp items_in([{:op, ","} | rest], 0, current, acc),
    do: items_in(rest, 0, [], [Enum.reverse(current) | acc])

  defp items_in([{:op, "("} = t | rest], d, current, acc),
    do: items_in(rest, d + 1, [t | current], acc)

  defp items_in([{:op, ")"} = t | rest], d, current, acc),
    do: items_in(rest, d - 1, [t | current], acc)

  defp items_in([t | rest], d, current, acc), do: items_in(rest, d, [t | current], acc)

  defp drop_table_error do
    error(
      "0A000",
      "DROP TABLE would delete a set's keys, and there is no scan to find them; " <>
        "SELECT kurwa_forget('name') stops listing a set and leaves its keys"
    )
  end

  # ------------------------------------------------------------------- tables

  # schema.table is accepted for the schema psql and drivers assume.
  # kurwadb.dbo.seen: SQL Server's three-part name, with this database.
  defp table([{d, "kurwadb"}, {:op, "."}, {s, _} = schema, {:op, "."}, {t, _} = name | rest])
       when d in [:ident, :qident] and s in [:ident, :qident] and t in [:ident, :qident],
       do: table([schema, {:op, "."}, name | rest])

  # public (PostgreSQL's) and dbo (SQL Server's) are the default schema, and
  # kurwadb (MySQL's database) names it too: their tables are plain sets. Any
  # other schema is part of the set's name - analytics.events is the set
  # "analytics.events" - so a set made over HTTP with a dot in its name shows
  # up in SQL inside a schema.
  defp table([{s, schema}, {:op, "."}, {t, name} | rest])
       when s in [:ident, :qident] and t in [:ident, :qident] do
    cond do
      schema in @default_schemas -> table([{t, name} | rest])
      schema in @system_schemas -> no_schema(schema)
      true -> table([{:qident, schema <> "." <> name} | rest])
    end
  end

  defp table([{type, name} | rest]) when type in [:ident, :qident] do
    rest = skip_definition(rest)

    cond do
      name == @default_table -> {:ok, :default, rest}
      Kurwa.Key.valid_name?(name) -> {:ok, name, rest}
      true -> error("42602", "\"#{name}\" is not a valid set name")
    end
  end

  defp table([token | _]), do: unexpected(token)
  defp table([]), do: error("42601", "expected a table name")

  # CREATE TABLE seen (key text primary key): the definition says nothing a set
  # can use, so it is skipped rather than parsed.
  defp skip_definition([{:op, "("} | _] = tokens) do
    if create_definition?(tokens), do: drop_parens(tokens, 0), else: tokens
  end

  defp skip_definition(tokens), do: tokens

  defp create_definition?([{:op, "("}, {_, _}, {:ident, type} | _])
       when type in ~w(text varchar char character bytea int integer bigint),
       do: true

  defp create_definition?(_), do: false

  defp drop_parens([{:op, "("} | rest], depth), do: drop_parens(rest, depth + 1)
  defp drop_parens([{:op, ")"} | rest], 1), do: rest
  defp drop_parens([{:op, ")"} | rest], depth), do: drop_parens(rest, depth - 1)
  defp drop_parens([_ | rest], depth), do: drop_parens(rest, depth)
  defp drop_parens([], _depth), do: []

  defp no_schema(schema),
    do: error("3F000", "schema \"#{schema}\" holds no sets: it is a system schema")

  defp schema_name([{type, name} | rest]) when type in [:ident, :qident] do
    cond do
      name in @default_schemas or name in @system_schemas ->
        {:ok, name, rest}

      Kurwa.Key.valid_name?(name) and not String.contains?(name, ".") ->
        {:ok, name, rest}

      true ->
        error("42602", "\"#{name}\" is not a valid schema name")
    end
  end

  defp schema_name([token | _]), do: unexpected(token)
  defp schema_name([]), do: error("42601", "expected a schema name")

  @doc "Schemas that are the default one: their tables are plain set names."
  def default_schemas, do: @default_schemas

  # ---------------------------------------------------------------- utilities

  defp setting(tokens) do
    tokens =
      case tokens do
        [{:ident, scope} | rest] when scope in ~w(session local) -> rest
        rest -> rest
      end

    case tokens do
      [name, {:op, "="} | value] -> {text(name), words(value)}
      [name, {:ident, "to"} | value] -> {text(name), words(value)}
      other -> {words(other), ""}
    end
  end

  defp deallocate([{:ident, "prepare"} | rest]), do: deallocate(rest)
  defp deallocate([{:ident, "all"}]), do: :all
  defp deallocate([name]), do: text(name)
  defp deallocate(_), do: :all

  defp words(tokens), do: tokens |> Enum.map(&text/1) |> Enum.join(" ")

  defp text({:ident, s}), do: s
  defp text({:qident, s}), do: s
  defp text({:string, s}), do: s
  defp text({:number, n}), do: to_string(n)
  defp text({:param, n}), do: "$#{n}"
  defp text({:op, op}), do: op
  defp text({:named, name}), do: "@" <> name
  defp text({_kind, value}), do: to_string(value)

  defp finished([]), do: :ok
  defp finished([token | _]), do: unexpected(token)

  defp unexpected(token), do: error("42601", "syntax error at or near \"#{text(token)}\"")

  defp error(code, message), do: {:error, code, message}
end
