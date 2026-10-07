defmodule Kurwa.Sql.Procedural do
  @moduledoc """
  The procedural part of T-SQL that a stored procedure needs, and a batch
  too: variables, conditions, blocks, calls - around the data statements
  `Kurwa.Sql.Parser` already understands.

      CREATE PROCEDURE dbo.consume @key NVARCHAR(200), @fresh BIT OUTPUT AS
      BEGIN
        SET NOCOUNT ON;
        IF NOT EXISTS (SELECT 1 FROM tokens WHERE [key] = @key)
          INSERT INTO tokens VALUES (@key);
        SET @fresh = @@ROWCOUNT;
        IF @fresh = 0 THROW 50001, N'already consumed', 1;
        RETURN 0;
      END

  What it understands: parameters with defaults and OUTPUT; `DECLARE`, `SET`,
  `SELECT @v = expr`; `IF` / `ELSE` and `BEGIN ... END`; conditions with
  `= <> < > <= >=`, `AND OR NOT`, `IS [NOT] NULL`, `[NOT] IN (...)`, `EXISTS`,
  `+` and `-`; `RETURN`, `THROW`, `RAISERROR`, `PRINT`; `EXEC` of another
  procedure, by position or name, with OUTPUT and `EXEC @rc = ...`, nested up
  to 32 deep as in SQL Server; and every data statement a set answers.
  `IF NOT EXISTS (SELECT ... WHERE [key] = x) INSERT ... VALUES (x)` is still
  one atomic `add_new`, not a check and then an insert.

  What it refuses, by name: `WHILE`, cursors, `TRY/CATCH`, temporary tables
  and table variables (tables with columns, which this store does not have),
  `CASE`, dynamic SQL.

  Running a body produces events, not wire bytes, so any SQL frontend can turn
  them into its own replies:

      {:result, statement, result, depth}   what Exec.run gave; depth 0 is the
                                        batch or the procedure called, deeper is
                                        inside an EXEC
      {:notice, text}                   PRINT, RAISERROR below 11, warnings
      {:error, number, message}         an error; the body stops after THROW and
                                        after a statement that failed
      {:raised, number, message}        RAISERROR at 11 or above: reported, and
                                        the body carries on, as in SQL Server
      {:proc, name, return_code, depth} a nested EXEC finished, called at depth

  A statement that fails stops the procedure, as with `SET XACT_ABORT ON` -
  the setting most procedures are written to run under.
  """

  alias Kurwa.Sql.{Exec, Lexer, Parser}

  @max_depth 32

  # ================================================================ parsing

  @doc "Parses a batch - or a procedure body - into statements."
  def parse_body(sql) when is_binary(sql) do
    with {:ok, tokens} <- lex(sql) do
      body(tokens)
    end
  end

  @doc """
  Parses a procedure definition: `CREATE [OR ALTER] PROC[EDURE] name params AS body`.
  Returns `{:ok, %{name, params, body, source}}`.
  """
  def parse_procedure(sql) do
    with {:ok, tokens} <- lex(sql),
         {:ok, rest} <- create(tokens),
         {:ok, name, rest} <- proc_name(rest),
         {:ok, params, rest} <- proc_params(rest),
         {:ok, statements} <- body(rest) do
      {:ok, %{name: name, params: params, body: statements, source: sql}}
    end
  end

  defp lex(sql) do
    case Lexer.tokenize(sql, :tsql) do
      {:ok, tokens} -> {:ok, tokens}
      {:error, message} -> {:error, "42601", message}
    end
  end

  defp create([{:ident, "create"}, {:ident, "or"}, {:ident, "alter"}, {:ident, p} | rest])
       when p in ~w(proc procedure),
       do: {:ok, rest}

  defp create([{:ident, "create"}, {:ident, p} | rest]) when p in ~w(proc procedure),
    do: {:ok, rest}

  defp create(_), do: error("42601", "a procedure file holds CREATE PROCEDURE name ... AS ...")

  defp proc_name([{_, schema}, {:op, "."}, {t, name} | rest])
       when schema in ["dbo"] and t in [:ident, :qident],
       do: {:ok, String.downcase(name), rest}

  defp proc_name([{t, name} | rest]) when t in [:ident, :qident],
    do: {:ok, String.downcase(name), rest}

  defp proc_name(_), do: error("42601", "expected a procedure name")

  # Parameters run up to AS, optionally in parentheses.
  defp proc_params([{:op, "("} | rest]) do
    with {:ok, params, [{:op, ")"} | rest]} <- params(rest, []),
         [{:ident, "as"} | rest] <- rest do
      {:ok, params, rest}
    else
      _ -> error("42601", "expected ( parameters ) AS")
    end
  end

  defp proc_params(tokens) do
    with {:ok, params, [{:ident, "as"} | rest]} <- params(tokens, []) do
      {:ok, params, rest}
    else
      {:ok, _, _} -> error("42601", "expected AS after the parameters")
      error -> error
    end
  end

  defp params([{:named, name} | rest], acc) do
    with {:ok, type, rest} <- type(rest) do
      {default, rest} =
        case rest do
          [{:op, "="} | rest] ->
            case Parser.expression(rest) do
              {:ok, expr, rest} -> {{:default, expr}, rest}
              _ -> {:none, rest}
            end

          rest ->
            {:none, rest}
        end

      {output?, rest} =
        case rest do
          [{:ident, o} | rest] when o in ~w(output out) -> {true, rest}
          rest -> {false, rest}
        end

      rest =
        case rest do
          [{:ident, "readonly"} | rest] -> rest
          rest -> rest
        end

      param = %{name: name, type: type, default: default, output: output?}

      case rest do
        [{:op, ","} | rest] -> params(rest, [param | acc])
        rest -> {:ok, Enum.reverse([param | acc]), rest}
      end
    end
  end

  defp params(rest, acc), do: {:ok, Enum.reverse(acc), rest}

  # A type: a name, maybe (n), (max) or (p, s). Only its family matters here.
  defp type([{t, name} | rest]) when t in [:ident, :qident] do
    rest =
      case rest do
        [{:op, "("} | more] -> drop_parens(more, 1)
        rest -> rest
      end

    family =
      cond do
        name in ~w(int integer bigint smallint tinyint) -> :int
        name == "bit" -> :bit
        name in ~w(nvarchar varchar nchar char text ntext sysname uniqueidentifier) -> :text
        name in ~w(table cursor) -> :unsupported
        true -> :other
      end

    if family == :unsupported,
      do:
        error("0A000", "#{name} variables are tables or cursors, which this store does not have"),
      else: {:ok, family, rest}
  end

  defp type(_), do: error("42601", "expected a type")

  defp drop_parens([{:op, "("} | rest], depth), do: drop_parens(rest, depth + 1)
  defp drop_parens([{:op, ")"} | rest], 1), do: rest
  defp drop_parens([{:op, ")"} | rest], depth), do: drop_parens(rest, depth - 1)
  defp drop_parens([_ | rest], depth), do: drop_parens(rest, depth)
  defp drop_parens([], _depth), do: []

  # ------------------------------------------------------------ statements

  defp body(tokens) do
    case statements(tokens, false, []) do
      {:ok, statements, []} ->
        {:ok, statements}

      {:ok, _statements, [token | _]} ->
        error("42601", "unexpected #{inspect(token)} after the last statement")

      error ->
        error
    end
  end

  # Statements up to the end of input, or to END when inside a block.
  defp statements([], false, acc), do: {:ok, Enum.reverse(acc), []}
  defp statements([], true, _acc), do: error("42601", "BEGIN without END")
  defp statements([{:op, ";"} | rest], block?, acc), do: statements(rest, block?, acc)
  defp statements([{:ident, "end"} | rest], true, acc), do: {:ok, Enum.reverse(acc), rest}
  defp statements([{:ident, "go"} | rest], block?, acc), do: statements(rest, block?, acc)

  defp statements(tokens, block?, acc) do
    with {:ok, statement, rest} <- one(tokens) do
      statements(rest, block?, [statement | acc])
    end
  end

  defp one([{:ident, "begin"}, {:ident, t} | _] = tokens)
       when t in ~w(tran transaction distributed), do: data(tokens)

  defp one([{:ident, "begin"}, {:ident, t} | _]) when t in ~w(try catch),
    do: error("0A000", "TRY/CATCH is not supported: a failing statement stops the procedure")

  defp one([{:ident, "begin"} | rest]) do
    with {:ok, statements, rest} <- statements(rest, true, []),
         do: {:ok, {:block, statements}, rest}
  end

  defp one([{:ident, "if"} | rest]) do
    with {:ok, condition, rest} <- condition(rest),
         {:ok, then, rest} <- one(rest) do
      {otherwise, rest} =
        case skip_semicolons(rest) do
          [{:ident, "else"} | rest] ->
            case one(rest) do
              {:ok, otherwise, rest} -> {otherwise, rest}
              error -> {error, rest}
            end

          _ ->
            {nil, rest}
        end

      case otherwise do
        {:error, _, _} = error -> error
        otherwise -> {:ok, conditional(condition, then, otherwise), rest}
      end
    end
  end

  defp one([{:ident, w} | _]) when w in ~w(while goto waitfor),
    do: error("0A000", "#{String.upcase(w)} is not supported in procedures")

  defp one([{:ident, "declare"} | rest]) do
    with {:ok, declarations, rest} <- declarations(rest, []),
         do: {:ok, {:declare, declarations}, rest}
  end

  defp one([{:ident, "set"}, {:named, name}, {:op, "="} | rest]) do
    with {:ok, expr, rest} <- condition(rest), do: {:ok, {:set_var, name, expr}, rest}
  end

  # SELECT @a = expr, @b = expr: assignment, not a result set.
  defp one([{:ident, "select"}, {:named, _}, {:op, "="} | _] = tokens) do
    with {:ok, assignments, rest} <- assignments(tl(tokens), []) do
      case rest do
        [{:ident, "from"} | _] ->
          error("0A000", "SELECT @variable = ... FROM is not supported; use IF EXISTS")

        rest ->
          {:ok, {:assign, assignments}, rest}
      end
    end
  end

  defp one([{:ident, "return"} | rest]) do
    if ends_statement?(rest) do
      {:ok, {:return, nil}, rest}
    else
      with {:ok, expr, rest} <- condition(rest), do: {:ok, {:return, expr}, rest}
    end
  end

  defp one([{:ident, "throw"} | rest]) do
    with {:ok, number, [{:op, ","} | rest]} <- condition(rest),
         {:ok, message, [{:op, ","} | rest]} <- condition(rest),
         {:ok, state, rest} <- condition(rest) do
      {:ok, {:throw, number, message, state}, rest}
    else
      {:error, _, _} = error -> error
      _ -> error("42601", "THROW takes number, message, state")
    end
  end

  defp one([{:ident, "raiserror"}, {:op, "("} | rest]) do
    with {:ok, message, [{:op, ","} | rest]} <- condition(rest),
         {:ok, severity, [{:op, ","} | rest]} <- condition(rest),
         {:ok, state, rest} <- condition(rest),
         {_args, [{:op, ")"} | rest]} <- extra_args(rest) do
      rest =
        case rest do
          [{:ident, "with"}, {:ident, _} | rest] -> rest
          rest -> rest
        end

      {:ok, {:raiserror, message, severity, state}, rest}
    else
      {:error, _, _} = error -> error
      _ -> error("42601", "RAISERROR takes (message, severity, state)")
    end
  end

  defp one([{:ident, "print"} | rest]) do
    with {:ok, expr, rest} <- condition(rest), do: {:ok, {:print, expr}, rest}
  end

  defp one([{:ident, e}, {:named, rc}, {:op, "="} | rest]) when e in ~w(exec execute) do
    with {:ok, {:exec, nil, name, args}, rest} <- exec(rest),
         do: {:ok, {:exec, rc, name, args}, rest}
  end

  defp one([{:ident, e} | rest]) when e in ~w(exec execute), do: exec(rest)

  defp one([{:ident, "create"}, {:ident, kind} | _] = tokens) when kind in ~w(table schema),
    do: data(tokens)

  defp one([{:ident, "create"} | _]),
    do:
      error(
        "0A000",
        "CREATE inside a batch is not supported: procedures are files deployed with the node"
      )

  defp one(tokens), do: data(tokens)

  # A data statement: its tokens up to the next statement, then the SQL parser.
  defp data(tokens) do
    {mine, rest} = take(tokens, 0, [])

    case Parser.statement_tokens(mine) do
      {:ok, statement} -> {:ok, {:sql, statement}, rest}
      {:error, _, _} = error -> error
    end
  end

  # Where a statement without a semicolon ends: before a keyword that starts
  # another one at the top level, or END / ELSE.
  @starts Parser.tsql_starts() ++ ~w(end else return throw raiserror while go)

  defp take([], _depth, acc), do: {Enum.reverse(acc), []}
  defp take([{:op, ";"} | rest], 0, acc), do: {Enum.reverse(acc), rest}
  defp take([{:op, "("} = t | rest], d, acc), do: take(rest, d + 1, [t | acc])
  defp take([{:op, ")"} = t | rest], d, acc), do: take(rest, max(d - 1, 0), [t | acc])

  defp take([{:ident, kw} | _] = tokens, 0, acc) when kw in @starts and acc != [],
    do: {Enum.reverse(acc), tokens}

  defp take([t | rest], d, acc), do: take(rest, d, [t | acc])

  defp ends_statement?([]), do: true
  defp ends_statement?([{:op, ";"} | _]), do: true
  defp ends_statement?([{:ident, kw} | _]), do: kw in @starts
  defp ends_statement?(_), do: false

  defp skip_semicolons([{:op, ";"} | rest]), do: skip_semicolons(rest)
  defp skip_semicolons(rest), do: rest

  defp declarations([{:named, name} | rest], acc) do
    rest =
      case rest do
        [{:ident, "as"} | rest] -> rest
        rest -> rest
      end

    with {:ok, type, rest} <- type(rest) do
      {value, rest} =
        case rest do
          [{:op, "="} | rest] ->
            case condition(rest) do
              {:ok, expr, rest} -> {expr, rest}
              _ -> {{:lit, nil}, rest}
            end

          rest ->
            {{:lit, nil}, rest}
        end

      case rest do
        [{:op, ","} | rest] -> declarations(rest, [{name, type, value} | acc])
        rest -> {:ok, Enum.reverse([{name, type, value} | acc]), rest}
      end
    end
  end

  defp declarations(_, _), do: error("42601", "expected @name type after DECLARE")

  defp assignments([{:named, name}, {:op, "="} | rest], acc) do
    with {:ok, expr, rest} <- condition(rest) do
      case rest do
        [{:op, ","} | rest] -> assignments(rest, [{name, expr} | acc])
        rest -> {:ok, Enum.reverse([{name, expr} | acc]), rest}
      end
    end
  end

  defp assignments(_, _), do: error("42601", "expected @name = value")

  defp extra_args([{:op, ","} | rest]) do
    {_, rest} = Enum.split_while(rest, &(&1 != {:op, ")"}))
    {[], rest}
  end

  defp extra_args(rest), do: {[], rest}

  # EXEC name [arg [, arg]...]: an arg is value or @param = value, then maybe OUTPUT.
  defp exec(tokens) do
    with {:ok, name, rest} <- proc_name(tokens) do
      if ends_statement?(rest) do
        {:ok, {:exec, nil, name, []}, rest}
      else
        with {:ok, args, rest} <- exec_args(rest, []), do: {:ok, {:exec, nil, name, args}, rest}
      end
    end
  end

  defp exec_args(tokens, acc) do
    {target, tokens} =
      case tokens do
        [{:named, param}, {:op, "="} | rest] -> {param, rest}
        rest -> {nil, rest}
      end

    value =
      case tokens do
        [{:ident, "default"} | rest] -> {:ok, :default, rest}
        tokens -> condition(tokens)
      end

    with {:ok, value, rest} <- value do
      {output?, rest} =
        case rest do
          [{:ident, o} | rest] when o in ~w(output out) -> {true, rest}
          rest -> {false, rest}
        end

      arg = %{param: target, value: value, output: output?}

      case rest do
        [{:op, ","} | rest] -> exec_args(rest, [arg | acc])
        rest -> {:ok, Enum.reverse([arg | acc]), rest}
      end
    end
  end

  # IF NOT EXISTS (SELECT ... WHERE [key] = x) INSERT INTO t VALUES (x), with
  # no ELSE, is the atomic conditional add - kept as one statement.
  defp conditional(
         {:not, {:exists, guard}},
         {:sql, {:insert, table, columns, [row], returning, _}},
         nil
       ) do
    if Parser.guards_insert?(guard, table, columns, row),
      do: {:sql, {:insert, table, columns, [row], returning, :nothing}},
      else:
        {:if, {:not, {:exists, guard}}, {:sql, {:insert, table, columns, [row], returning, nil}},
         nil}
  end

  defp conditional(condition, then, otherwise), do: {:if, condition, then, otherwise}

  # ------------------------------------------------------------ conditions

  @comparisons ~w(= <> != < > <= >=)

  @doc false
  def condition(tokens), do: disjunction(tokens)

  defp disjunction(tokens) do
    with {:ok, left, rest} <- conjunction(tokens), do: more_or(left, rest)
  end

  defp more_or(left, [{:ident, "or"} | rest]) do
    with {:ok, right, rest} <- conjunction(rest), do: more_or({:or, left, right}, rest)
  end

  defp more_or(left, rest), do: {:ok, left, rest}

  defp conjunction(tokens) do
    with {:ok, left, rest} <- negation(tokens), do: more_and(left, rest)
  end

  defp more_and(left, [{:ident, "and"} | rest]) do
    with {:ok, right, rest} <- negation(rest), do: more_and({:and, left, right}, rest)
  end

  defp more_and(left, rest), do: {:ok, left, rest}

  defp negation([{:ident, "not"} | rest]) do
    with {:ok, expr, rest} <- negation(rest), do: {:ok, {:not, expr}, rest}
  end

  defp negation(tokens), do: comparison(tokens)

  defp comparison(tokens) do
    with {:ok, left, rest} <- sum(tokens) do
      case rest do
        [{:op, op} | rest] when op in @comparisons ->
          with {:ok, right, rest} <- sum(rest), do: {:ok, {:cmp, op, left, right}, rest}

        [{:ident, "is"}, {:ident, "not"}, {:ident, "null"} | rest] ->
          {:ok, {:not, {:is_null, left}}, rest}

        [{:ident, "is"}, {:ident, "null"} | rest] ->
          {:ok, {:is_null, left}, rest}

        [{:ident, "not"}, {:ident, "in"}, {:op, "("} | rest] ->
          with {:ok, list, rest} <- list(rest, []), do: {:ok, {:not, {:in, left, list}}, rest}

        [{:ident, "in"}, {:op, "("} | rest] ->
          with {:ok, list, rest} <- list(rest, []), do: {:ok, {:in, left, list}, rest}

        rest ->
          {:ok, left, rest}
      end
    end
  end

  defp list(tokens, acc) do
    with {:ok, expr, rest} <- sum(tokens) do
      case rest do
        [{:op, ","} | rest] -> list(rest, [expr | acc])
        [{:op, ")"} | rest] -> {:ok, Enum.reverse([expr | acc]), rest}
        _ -> error("42601", "expected ) after IN (...")
      end
    end
  end

  defp sum(tokens) do
    with {:ok, left, rest} <- term(tokens), do: more_sum(left, rest)
  end

  defp more_sum(left, [{:op, op} | rest]) when op in ["+", "-"] do
    with {:ok, right, rest} <- term(rest), do: more_sum({:arith, op, left, right}, rest)
  end

  defp more_sum(left, rest), do: {:ok, left, rest}

  # A parenthesised condition, or whatever the SQL parser calls an expression -
  # literals, @variables, @@variables, calls, CAST, EXISTS (SELECT ...).
  defp term([{:op, "("}, {:ident, "select"} | _] = tokens), do: Parser.expression(tokens)

  defp term([{:op, "("} | rest]) do
    with {:ok, expr, [{:op, ")"} | rest]} <- condition(rest) do
      {:ok, expr, rest}
    else
      {:ok, _, _} -> error("42601", "expected )")
      error -> error
    end
  end

  defp term([{:op, "-"}, {:named, _} = var | rest]) do
    with {:ok, expr, rest} <- Parser.expression([var | rest]),
         do: {:ok, {:arith, "-", {:lit, 0}, expr}, rest}
  end

  defp term([{:ident, "case"} | _]), do: error("0A000", "CASE is not supported: use IF / ELSE")
  defp term(tokens), do: Parser.expression(tokens)

  # ================================================================ running

  @doc """
  Runs a batch's statements. `ctx` carries the session, a function that keeps
  it current as statements complete (`:observe`), and the procedures to call.
  Returns `{events, session}`.
  """
  def run_batch(statements, ctx) do
    # An sp_executesql batch sees its parameters as variables.
    vars = Map.new(Map.get(ctx, :vars, %{}), fn {name, value} -> {name, {:other, value}} end)
    state = %{vars: vars, session: ctx.session, events: [], depth: 0, ctx: ctx}

    state =
      try do
        run(statements, state)
      catch
        {:stop, state} -> state
        {:return, _code, state} -> state
      end

    {Enum.reverse(state.events), state.session}
  end

  @doc """
  Calls procedure `proc` with `args` - `[%{param, value, output}]`, values
  already evaluated. Returns `{events, return_code, outputs, session}`, where
  `outputs` are `{param, type, value}` for each OUTPUT parameter.
  """
  def call(proc, args, ctx, depth \\ 0) do
    state = %{vars: %{}, session: ctx.session, events: [], depth: depth, ctx: ctx}

    case bind(proc, args, state) do
      {:ok, state} ->
        {code, state} =
          try do
            {0, run(proc.body, state)}
          catch
            {:return, code, state} -> {code, state}
            {:stop, state} -> {nil, state}
          end

        outputs =
          for %{name: name, type: type, output: true} <- proc.params,
              do: {name, type, var(state, name)}

        {Enum.reverse(state.events), code, outputs, state.session}

      {:error, number, message} ->
        {[{:error, number, message}], nil, [], ctx.session}
    end
  end

  # Arguments to parameters: by position until the first named one, then by name.
  defp bind(proc, args, state) do
    {positional, named} = Enum.split_while(args, &(&1.param == nil))

    if length(positional) > length(proc.params) do
      {:error, 8144, "Procedure or function #{proc.name} has too many arguments specified."}
    else
      given =
        Map.merge(
          proc.params |> Enum.zip(positional) |> Map.new(fn {p, a} -> {p.name, a.value} end),
          Map.new(named, &{&1.param, &1.value})
        )

      Enum.reduce_while(proc.params, {:ok, state}, fn param, {:ok, state} ->
        case {Map.fetch(given, param.name), param.default} do
          {{:ok, value}, _} when value != :default ->
            {:cont, {:ok, put_var(state, param.name, param.type, value)}}

          {_, {:default, expr}} ->
            {:ok, value} = Exec.evaluate(expr, %{}, state.session)
            {:cont, {:ok, put_var(state, param.name, param.type, value)}}

          {_, :none} ->
            {:halt,
             {:error, 201,
              "Procedure or function '#{proc.name}' expects parameter '@#{param.name}', which was not supplied."}}
        end
      end)
    end
  end

  defp run(statements, state) when is_list(statements),
    do: Enum.reduce(statements, state, &step/2)

  defp step({:block, statements}, state), do: run(statements, state)

  defp step({:if, condition, then, otherwise}, state) do
    cond do
      truthy?(eval(condition, state)) -> step(then, state)
      otherwise != nil -> step(otherwise, state)
      true -> state
    end
  end

  defp step({:declare, declarations}, state) do
    Enum.reduce(declarations, state, fn {name, type, expr}, state ->
      put_var(state, name, type, eval(expr, state))
    end)
  end

  defp step({:set_var, name, expr}, state) do
    unless Map.has_key?(state.vars, name),
      do: stop(state, 137, "Must declare the scalar variable \"@#{name}\".")

    put_var(state, name, elem(state.vars[name], 0), eval(expr, state))
  end

  defp step({:assign, assignments}, state) do
    Enum.reduce(assignments, state, fn {name, expr}, state ->
      step({:set_var, name, expr}, state)
    end)
  end

  defp step({:return, nil}, state), do: throw({:return, 0, state})
  defp step({:return, expr}, state), do: throw({:return, to_int(eval(expr, state)), state})

  defp step({:throw, number, message, _state}, state) do
    stop(state, to_int(eval(number, state)), to_string(eval(message, state)))
  end

  defp step({:raiserror, message, severity, _state}, state) do
    text = to_string(eval(message, state))

    if to_int(eval(severity, state)) <= 10,
      do: event(state, {:notice, text}),
      else: event(state, {:raised, 50_000, text})
  end

  defp step({:print, expr}, state), do: event(state, {:notice, to_string(eval(expr, state))})

  defp step({:exec, rc, name, args}, state) do
    if state.depth >= @max_depth,
      do:
        stop(state, 217, "Maximum stored procedure nesting level exceeded (limit #{@max_depth}).")

    proc =
      case state.ctx.procedure.(name) do
        nil -> stop(state, 2812, "Could not find stored procedure '#{name}'.")
        proc -> proc
      end

    values =
      Enum.map(args, fn arg ->
        %{arg | value: if(arg.value == :default, do: :default, else: eval(arg.value, state))}
      end)

    {events, code, outputs, session} =
      call(proc, values, %{state.ctx | session: state.session}, state.depth + 1)

    state = %{state | session: session, events: Enum.reverse(events) ++ state.events}

    if code == nil, do: throw({:stop, state})

    state = event(state, {:proc, name, code, state.depth})
    state = if rc, do: put_var(state, rc, :int, code), else: state

    # OUTPUT arguments take the procedure's final values, matched by name or place.
    outputs = Map.new(outputs, fn {param, _type, value} -> {param, value} end)

    args
    |> Enum.with_index()
    |> Enum.reduce(state, fn {arg, i}, state ->
      param = arg.param || (Enum.at(proc.params, i) || %{name: nil}).name

      case {arg.output, arg.value} do
        {true, {:param, var}} when is_binary(var) ->
          put_var(
            state,
            var,
            elem(Map.get(state.vars, var, {:other, nil}), 0),
            Map.get(outputs, param)
          )

        _ ->
          state
      end
    end)
  end

  defp step({:sql, statement}, state) do
    result = Exec.run(statement, params(state), state.session)
    notices = Exec.take_notices()
    session = state.ctx.observe.(statement, result, state.session)
    state = %{state | session: session}
    state = Enum.reduce(notices, state, &event(&2, {:notice, &1}))

    case result do
      {:error, code, message} -> stop(state, {:sqlstate, code}, message)
      result -> event(state, {:result, statement, result, state.depth})
    end
  end

  # ------------------------------------------------------------ evaluation

  defp eval({:or, a, b}, s), do: truthy?(eval(a, s)) or truthy?(eval(b, s))
  defp eval({:and, a, b}, s), do: truthy?(eval(a, s)) and truthy?(eval(b, s))
  defp eval({:not, a}, s), do: not truthy?(eval(a, s))
  defp eval({:is_null, a}, s), do: eval(a, s) == nil

  defp eval({:in, a, list}, s) do
    value = eval(a, s)
    value != nil and Enum.any?(list, &(compare(value, eval(&1, s)) == :eq))
  end

  defp eval({:cmp, op, a, b}, s) do
    case compare(eval(a, s), eval(b, s)) do
      nil -> nil
      order -> holds?(op, order)
    end
  end

  defp eval({:arith, op, a, b}, s) do
    case {eval(a, s), eval(b, s), op} do
      {nil, _, _} -> nil
      {_, nil, _} -> nil
      {x, y, "+"} when is_number(x) and is_number(y) -> x + y
      {x, y, "-"} when is_number(x) and is_number(y) -> x - y
      {x, y, "+"} -> to_string(x) <> to_string(y)
      {_, _, "-"} -> stop(s, 8114, "Error converting data type to numeric.")
    end
  end

  defp eval({:param, name}, s) when is_binary(name) do
    case Map.fetch(s.vars, name) do
      {:ok, {_type, value}} -> value
      :error -> stop(s, 137, "Must declare the scalar variable \"@#{name}\".")
    end
  end

  defp eval(expr, s) do
    case Exec.evaluate(expr, params(s), s.session) do
      {:ok, value} -> value
      {:error, code, message} -> stop(s, {:sqlstate, code}, message)
    end
  end

  defp params(state), do: Map.new(state.vars, fn {name, {_type, value}} -> {name, value} end)

  defp compare(nil, _), do: nil
  defp compare(_, nil), do: nil
  defp compare(a, b) when is_boolean(a), do: compare(if(a, do: 1, else: 0), b)
  defp compare(a, b) when is_boolean(b), do: compare(a, if(b, do: 1, else: 0))
  defp compare(a, b) when is_number(a) and is_binary(b), do: compare(a, number(b))
  defp compare(a, b) when is_binary(a) and is_number(b), do: compare(number(a), b)
  defp compare(a, b) when a == b, do: :eq
  defp compare(a, b) when a < b, do: :lt
  defp compare(_, _), do: :gt

  defp number(s) do
    case Float.parse(s) do
      {n, ""} -> n
      _ -> s
    end
  end

  defp holds?("=", order), do: order == :eq
  defp holds?(op, order) when op in ["<>", "!="], do: order != :eq
  defp holds?("<", order), do: order == :lt
  defp holds?(">", order), do: order == :gt
  defp holds?("<=", order), do: order != :gt
  defp holds?(">=", order), do: order != :lt

  defp truthy?(nil), do: false
  defp truthy?(false), do: false
  defp truthy?(0), do: false
  defp truthy?(_), do: true

  # ------------------------------------------------------------- variables

  defp put_var(state, name, type, value) do
    case coerce(type, value) do
      {:error, shown} ->
        stop(state, 245, "Conversion failed when converting the value '#{shown}' to #{type}.")

      coerced ->
        %{state | vars: Map.put(state.vars, name, {type, coerced})}
    end
  end

  defp var(state, name) do
    case Map.fetch(state.vars, name) do
      {:ok, {_type, value}} -> value
      :error -> nil
    end
  end

  defp coerce(_type, nil), do: nil
  defp coerce(:int, v) when is_integer(v), do: v
  defp coerce(:int, true), do: 1
  defp coerce(:int, false), do: 0
  defp coerce(:int, v) when is_float(v), do: trunc(v)

  defp coerce(:int, v) when is_binary(v) do
    case Integer.parse(String.trim(v)) do
      {n, ""} -> n
      _ -> {:error, v}
    end
  end

  defp coerce(:bit, v) when v in [true, 1, "1", "true", "TRUE"], do: true
  defp coerce(:bit, v) when v in [false, 0, "0", "false", "FALSE"], do: false
  defp coerce(:bit, v) when is_number(v), do: v != 0
  defp coerce(:text, v) when is_binary(v), do: v
  defp coerce(:text, true), do: "1"
  defp coerce(:text, false), do: "0"
  defp coerce(:text, v), do: to_string(v)
  defp coerce(_type, v), do: v

  defp to_int(v) do
    case coerce(:int, v) do
      n when is_integer(n) -> n
      _ -> 0
    end
  end

  # ---------------------------------------------------------------- events

  defp event(state, event), do: %{state | events: [event | state.events]}

  defp stop(state, number, message), do: throw({:stop, event(state, {:error, number, message})})

  defp error(code, message), do: {:error, code, message}
end
