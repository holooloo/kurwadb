defmodule Kurwa.Mysql.Server do
  @moduledoc """
  A MySQL server, one process per connection, so `mysql`, `mariadb` and the
  MySQL connectors talk to kurwadb as they are.

  The statements are `Kurwa.Sql`'s, in its `:mysql` dialect - backticks, `?`
  parameters, `"strings"`, `INSERT IGNORE` - over the same executor the
  PostgreSQL frontend uses, so a set is the same table here: one column, `key`
  (written `` `key` ``, since it is a reserved word in MySQL). This module is
  the conversation around them: the handshake, authentication, `COM_QUERY` with
  text result sets and multiple statements, `COM_STMT_PREPARE` / `EXECUTE`
  with binary ones, and the `SHOW` statements and system variables that the
  `mysql` client and the connectors ask for on their own.

  Authentication is `caching_sha2_password`, MySQL 8's default, or
  `mysql_native_password` if the client offers that; the password is
  `auth_token`. TLS is not offered, which every client's default
  (`ssl-mode=PREFERRED`) accepts, and caching_sha2's fast path needs none.

  Transactions are what they are in the other frontends: `START TRANSACTION`,
  `COMMIT` and `ROLLBACK` are accepted, the status flag follows them, and a
  `ROLLBACK` raises a warning - `SHOW WARNINGS` says that nothing was rolled
  back, because every statement already took effect.
  """

  use ThousandIsland.Handler

  alias Kurwa.Mysql.{Auth, Proto, Types}
  alias Kurwa.Sql.{Exec, Parser}

  @version Mix.Project.config()[:version]
  @server_version "8.0.36-kurwadb-#{@version}"
  @database "kurwadb"
  @plugin "caching_sha2_password"

  @impl ThousandIsland.Handler
  def handle_connection(socket, _state) do
    id = System.unique_integer([:positive]) |> rem(2_000_000_000)
    nonce = Auth.nonce()
    {out, _} = Proto.frame([Proto.handshake(@server_version, id, nonce, @plugin)], 0)
    ThousandIsland.Socket.send(socket, out)

    {:continue,
     %{
       phase: :auth,
       buffer: <<>>,
       nonce: nonce,
       caps: 0,
       id: id,
       user: nil,
       session: nil,
       in_transaction: false,
       warnings: [],
       statements: %{},
       next_statement: 1
     }}
  end

  @impl ThousandIsland.Handler
  def handle_data(data, socket, state) do
    {result, out} = loop(state.buffer <> data, %{state | buffer: <<>>}, [])
    if out != [], do: ThousandIsland.Socket.send(socket, out)
    result
  end

  defp loop(buffer, state, out) do
    case Proto.packet(buffer) do
      {:ok, seq, payload, rest} ->
        case packet(payload, seq, state) do
          {:reply, payloads, state} -> loop(rest, state, [out, frame(payloads, seq + 1)])
          {:close, payloads, state} -> {{:close, state}, [out, frame(payloads, seq + 1)]}
          {:noreply, state} -> loop(rest, state, out)
        end

      :more ->
        {{:continue, %{state | buffer: buffer}}, out}
    end
  end

  defp frame(payloads, seq), do: payloads |> Proto.frame(seq) |> elem(0)

  # -------------------------------------------------------------------- auth

  defp packet(payload, _seq, %{phase: :auth} = state) do
    case Proto.handshake_response(payload) do
      {:ok, response} ->
        state = %{
          state
          | caps: response.caps,
            user: response.user,
            session: session(state, response)
        }

        cond do
          response.database not in [nil, @database] ->
            {:close, [unknown_database(response.database)], state}

          Kurwa.Config.auth_token() == nil ->
            {:reply, [ok(state)], %{state | phase: :command}}

          # A client whose own default plugin differs from the one announced
          # sends an empty response and waits to be asked again, in its plugin.
          response.auth == "" and
              response.plugin in ["caching_sha2_password", "mysql_native_password"] ->
            {:reply, [Proto.auth_switch(response.plugin, state.nonce)],
             %{state | phase: {:switch, response.plugin}}}

          (response.plugin || @plugin) in ["caching_sha2_password", "mysql_native_password"] ->
            authenticate(response.plugin || @plugin, response.auth, state)

          true ->
            {:reply, [Proto.auth_switch(@plugin, state.nonce)],
             %{state | phase: {:switch, @plugin}}}
        end

      {:error, :ssl} ->
        {:close, [Proto.err(1045, "28000", "kurwadb does not offer TLS on the MySQL port")],
         state}

      {:error, _} ->
        {:close, [Proto.err(1043, "08S01", "Bad handshake")], state}
    end
  end

  # The answer to an AuthSwitchRequest is the bare auth response.
  defp packet(payload, _seq, %{phase: {:switch, plugin}} = state),
    do: authenticate(plugin, payload, state)

  defp packet(payload, _seq, %{phase: :command} = state),
    do: Kurwa.Metrics.measure(:mysql, fn -> command(payload, state) end)

  defp authenticate(plugin, response, state) do
    if Auth.valid?(plugin, response, state.nonce, to_string(Kurwa.Config.auth_token())) do
      state = %{state | phase: :command}

      case plugin do
        "caching_sha2_password" -> {:reply, [Proto.fast_auth_success(), ok(state)], state}
        _ -> {:reply, [ok(state)], state}
      end
    else
      message =
        "Access denied for user '#{state.user}'@'%' (using password: #{if response == "", do: "NO", else: "YES"})"

      {:close, [Proto.err(1045, "28000", message)], state}
    end
  end

  defp session(state, response) do
    %{
      user: response.user,
      database: @database,
      pid: state.id,
      version: @server_version,
      settings: %{},
      sysvars: sysvars(state.id)
    }
  end

  # The system variables clients read: the mysql client asks for
  # version_comment on connect, connectors for the rest.
  defp sysvars(id) do
    %{
      "version" => @server_version,
      "version_comment" => "kurwadb #{@version}, a distributed set that stores keys",
      "version_compile_os" => "BEAM",
      "version_compile_machine" => to_string(:erlang.system_info(:system_architecture)),
      "autocommit" => 1,
      "auto_increment_increment" => 1,
      "character_set_client" => "utf8mb4",
      "character_set_connection" => "utf8mb4",
      "character_set_results" => "utf8mb4",
      "character_set_server" => "utf8mb4",
      "character_set_database" => "utf8mb4",
      "collation_connection" => "utf8mb4_0900_ai_ci",
      "collation_server" => "utf8mb4_0900_ai_ci",
      "collation_database" => "utf8mb4_0900_ai_ci",
      "init_connect" => "",
      "interactive_timeout" => 28_800,
      "wait_timeout" => 28_800,
      "net_write_timeout" => 60,
      "net_buffer_length" => 16_384,
      "max_allowed_packet" => 67_108_864,
      "license" => "MIT",
      "lower_case_table_names" => 0,
      "performance_schema" => 0,
      "query_cache_size" => 0,
      "query_cache_type" => "OFF",
      "sql_mode" =>
        "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
      "system_time_zone" => "UTC",
      "time_zone" => "+00:00",
      "transaction_isolation" => "READ-COMMITTED",
      "tx_isolation" => "READ-COMMITTED",
      "transaction_read_only" => 0,
      "tx_read_only" => 0,
      "pseudo_thread_id" => id,
      "server_id" => 1,
      "default_storage_engine" => "kurwadb"
    }
  end

  # ---------------------------------------------------------------- commands

  defp command(<<0x01, _::binary>>, state), do: {:close, [], state}
  defp command(<<0x0E, _::binary>>, state), do: {:reply, [ok(state)], state}

  defp command(<<0x1F, _::binary>>, state),
    do: {:reply, [ok(state)], %{state | statements: %{}, in_transaction: false}}

  defp command(<<0x02, database::binary>>, state) do
    if database == @database,
      do: {:reply, [ok(state)], state},
      else: {:reply, [unknown_database(database)], state}
  end

  defp command(<<0x03, sql::binary>>, state), do: query(sql, state)
  defp command(<<0x16, sql::binary>>, state), do: prepare(sql, state)
  defp command(<<0x17, body::binary>>, state), do: execute(body, state)

  defp command(<<0x19, id::32-little, _::binary>>, state),
    do: {:noreply, %{state | statements: Map.delete(state.statements, id)}}

  defp command(<<0x1A, _id::32-little, _::binary>>, state), do: {:reply, [ok(state)], state}

  # COM_STMT_SEND_LONG_DATA has no reply; a key long enough to need it is
  # longer than this store wants, so it is dropped and the execute will say so.
  defp command(<<0x18, _::binary>>, state), do: {:noreply, state}

  # COM_SET_OPTION (multi-statements on or off) is answered with EOF.
  defp command(<<0x1B, _::binary>>, state), do: {:reply, [Proto.eof(status(state))], state}

  # COM_FIELD_LIST, deprecated: no fields to list.
  defp command(<<0x04, _::binary>>, state), do: {:reply, [terminator(state, false)], state}

  defp command(<<code, _::binary>>, state),
    do: {:reply, [Proto.err(1047, "08S01", "Unknown command #{code}")], state}

  defp command(<<>>, state), do: {:reply, [Proto.err(1047, "08S01", "Empty command")], state}

  # ------------------------------------------------------------------ query

  # Statements are split on the raw text, not after parsing, so each keeps its
  # source - which is what MySQL names an unaliased column by.
  defp query(sql, state) do
    pieces = split(sql)

    if length(pieces) > 1 and not Proto.multi_statements?(state.caps) do
      {:reply, [error("42601", "multiple statements need CLIENT_MULTI_STATEMENTS")], state}
    else
      run_all(pieces, state, [])
    end
  end

  defp run_all([piece | more], state, acc) do
    outcome =
      case Parser.parse(piece, :mysql) do
        {:ok, [statement]} -> run(statement, [], state)
        {:ok, _} -> {:error, error("42601", "could not split the statements"), state}
        {:error, code, message} -> {:error, error(code, message), state}
      end

    case outcome do
      {:error, packet, state} ->
        {:reply, acc ++ [packet], state}

      {:result, result, state} ->
        packets = text_result(name_columns(result, piece), state, more != [])

        if more == [],
          do: {:reply, acc ++ packets, state},
          else: run_all(more, state, acc ++ packets)
    end
  end

  # MySQL names a column with no alias by the text that produced it -
  # SELECT DATABASE() has a column called DATABASE() - except a plain column
  # reference, which is named by the column. Dictionary cursors key rows by
  # these names, so they follow MySQL rather than PostgreSQL.
  defp name_columns({:rows, columns, rows, tag}, sql) do
    items = if sql =~ ~r/^\s*select\b/i, do: Kurwa.Pg.Catalog.select_items(sql), else: []

    columns =
      if length(items) == length(columns) and items != ["*"] do
        Enum.zip_with(columns, items, fn {name, type}, item -> {mysql_name(item, name), type} end)
      else
        columns
      end

    {:rows, columns, rows, tag}
  end

  defp name_columns(result, _sql), do: result

  @alias ~r/\s(AS\s+)?(`[^`]+`|"[^"]+"|'[^']+'|[A-Za-z_][A-Za-z0-9_$]*)$/i
  @column ~r/^(?:(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*)\.)*(?:`([^`]+)`|([A-Za-z_][A-Za-z0-9_$]*))$/

  defp mysql_name(item, parsed) do
    cond do
      # an alias, with or without AS: the parser already has it
      item =~ @alias -> parsed
      # a column, possibly qualified: its own name
      match = Regex.run(@column, item) -> Enum.find(tl(match), &(&1 not in [nil, ""]))
      true -> item
    end
  end

  # Splits a query into statements at top-level semicolons, with MySQL's
  # quoting and comments, so that a ';' in a string is not a split.
  defp split(sql) do
    sql
    |> do_split([], [])
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> case do
      [] -> [""]
      pieces -> pieces
    end
  end

  defp do_split(<<>>, current, done), do: Enum.reverse([finish(current) | done])

  defp do_split(<<?;, rest::binary>>, current, done),
    do: do_split(rest, [], [finish(current) | done])

  defp do_split(<<q, rest::binary>>, current, done) when q in [?', ?", ?`] do
    {quoted, rest} = quoted(rest, q, [q])
    do_split(rest, [quoted | current], done)
  end

  defp do_split(<<"--", rest::binary>>, current, done), do: comment(rest, current, done, "--")
  defp do_split(<<"#", rest::binary>>, current, done), do: comment(rest, current, done, "#")

  defp do_split(<<"/*", rest::binary>>, current, done) do
    case :binary.split(rest, "*/") do
      [body, rest] -> do_split(rest, ["/*" <> body <> "*/" | current], done)
      [body] -> do_split(<<>>, ["/*" <> body | current], done)
    end
  end

  defp do_split(<<c, rest::binary>>, current, done), do: do_split(rest, [c | current], done)

  defp comment(rest, current, done, lead) do
    case :binary.split(rest, "\n") do
      [body, rest] -> do_split(rest, [lead <> body <> "\n" | current], done)
      [body] -> do_split(<<>>, [lead <> body | current], done)
    end
  end

  defp quoted(<<?\\, c, rest::binary>>, q, acc), do: quoted(rest, q, [c, ?\\ | acc])
  defp quoted(<<q, q, rest::binary>>, q, acc), do: quoted(rest, q, [q, q | acc])
  defp quoted(<<q, rest::binary>>, q, acc), do: {finish([q | acc]), rest}
  defp quoted(<<c, rest::binary>>, q, acc), do: quoted(rest, q, [c | acc])
  defp quoted(<<>>, _q, acc), do: {finish(acc), <<>>}

  defp finish(acc), do: acc |> Enum.reverse() |> IO.iodata_to_binary()

  # Runs one statement, MySQL's own ones first.
  defp run({:show, words}, _params, state) do
    case show(words, state) do
      {:error, code, message} -> {:error, error(code, message), state}
      rows -> {:result, rows, state}
    end
  end

  defp run({:use, database}, _params, state) do
    if database == @database,
      do: {:result, {:command, "USE"}, state},
      else: {:error, unknown_database(database), state}
  end

  defp run(:empty, _params, state),
    do: {:error, Proto.err(1065, "42000", "Query was empty"), state}

  defp run(statement, params, state) do
    result =
      case Exec.run(statement, params, state.session) do
        {:catalog, sql} -> Kurwa.Pg.Catalog.answer(sql, state.session)
        other -> other
      end

    warnings = Exec.take_notices()
    state = %{state | warnings: warnings}

    case result do
      {:error, code, message} ->
        {:error, error(code, message), state}

      {:command, tag} ->
        {:result, {:command, tag}, transition(state, statement)}

      other ->
        {:result, other, state}
    end
  end

  defp transition(state, {:utility, :begin, _}), do: %{state | in_transaction: true}

  defp transition(state, {:utility, kind, _}) when kind in [:commit, :rollback],
    do: %{state | in_transaction: false}

  defp transition(state, _), do: state

  defp text_result({:rows, columns, rows, _tag}, state, more?) do
    [Proto.lenenc_int(length(columns))] ++
      Enum.map(columns, fn {name, type} -> Proto.column(name, type) end) ++
      eof_unless_deprecated(state) ++
      Enum.map(rows, fn row -> row |> Enum.map(&Types.text/1) |> Proto.text_row() end) ++
      [terminator(state, more?)]
  end

  defp text_result(result, state, more?), do: [ok(state, affected(result), more?)]

  # ---------------------------------------------------------------- prepared

  defp prepare(sql, state) do
    with {:ok, [statement]} <- Parser.parse(sql, :mysql) do
      {params, columns} =
        case statement do
          {:catalog, sql} -> {[], Kurwa.Pg.Catalog.columns(sql)}
          {:show, _} -> {[], nil}
          other -> Exec.describe(other)
        end

      id = state.next_statement
      entry = %{statement: statement, params: length(params), columns: columns, types: []}
      columns = columns || []

      packets =
        [Proto.prepare_ok(id, length(columns), length(params))] ++
          if(params == [],
            do: [],
            else:
              Enum.map(params, fn _ -> Proto.column("?", :text) end) ++
                eof_unless_deprecated(state)
          ) ++
          if(columns == [],
            do: [],
            else:
              Enum.map(columns, fn {n, t} -> Proto.column(n, t) end) ++
                eof_unless_deprecated(state)
          )

      {:reply, packets,
       %{state | statements: Map.put(state.statements, id, entry), next_statement: id + 1}}
    else
      {:ok, _many} ->
        {:reply, [error("42601", "a prepared statement holds one statement")], state}

      {:error, code, message} ->
        {:reply, [error(code, message)], state}
    end
  end

  defp execute(<<id::32-little, _::binary>> = body, state) do
    case Map.fetch(state.statements, id) do
      :error ->
        {:reply,
         [
           Proto.err(
             1243,
             "HY000",
             "Unknown prepared statement handler (#{id}) given to mysqld_stmt_execute"
           )
         ], state}

      {:ok, entry} ->
        case Proto.execute(body, entry.params, entry.types) do
          {:ok, ^id, values, types} ->
            statements = Map.put(state.statements, id, %{entry | types: types})
            state = %{state | statements: statements}

            case run(entry.statement, values, state) do
              {:error, packet, state} -> {:reply, [packet], state}
              {:result, result, state} -> {:reply, binary_result(result, state), state}
            end

          _ ->
            {:reply, [Proto.err(1210, "HY000", "Incorrect arguments to mysqld_stmt_execute")],
             state}
        end
    end
  end

  defp binary_result({:rows, columns, rows, _tag}, state) do
    types = Enum.map(columns, &elem(&1, 1))

    [Proto.lenenc_int(length(columns))] ++
      Enum.map(columns, fn {name, type} -> Proto.column(name, type) end) ++
      eof_unless_deprecated(state) ++
      Enum.map(rows, fn row ->
        row
        |> Enum.zip(types)
        |> Enum.map(fn {v, t} -> Types.binary(v, t) end)
        |> Proto.binary_row()
      end) ++
      [terminator(state, false)]
  end

  defp binary_result(result, state), do: [ok(state, affected(result), false)]

  # -------------------------------------------------------------------- SHOW

  defp show(words, state) do
    case String.split(words, " ", trim: true) do
      [db] when db in ~w(databases schemas) ->
        rows("Database", [["information_schema"], [@database]])

      ["tables" | _] = w ->
        rows(["Tables_in_#{@database}"], Enum.map(like(sets(), w), &[&1]))

      ["full", "tables" | _] = w ->
        rows(
          ["Tables_in_#{@database}", "Table_type"],
          Enum.map(like(sets(), w), &[&1, "BASE TABLE"])
        )

      [scope, "variables" | w] when scope in ~w(session global local) ->
        variables(["variables" | w], state)

      ["variables" | _] = w ->
        variables(w, state)

      ["warnings" | _] ->
        rows(["Level", "Code", "Message"], Enum.map(state.warnings, &["Warning", "1105", &1]))

      ["count(*)", "warnings"] ->
        rows(["@@session.warning_count"], [[Integer.to_string(length(state.warnings))]])

      ["errors" | _] ->
        rows(["Level", "Code", "Message"], [])

      [kind | rest] when kind in ~w(columns fields) ->
        describe(rest)

      ["full", kind | rest] when kind in ~w(columns fields) ->
        describe(rest)

      ["create", "table", table | _] ->
        create =
          "CREATE TABLE `#{table}` (\n  `key` varchar(255) NOT NULL,\n  PRIMARY KEY (`key`)\n) ENGINE=kurwadb"

        rows(["Table", "Create Table"], [[table, create]])

      [kind | _]
      when kind in ~w(status engines collation plugins processlist index indexes keys triggers events) ->
        rows(["Variable_name", "Value"], [])

      ["character", "set" | _] ->
        rows(["Charset", "Description", "Default collation", "Maxlen"], [
          ["utf8mb4", "UTF-8 Unicode", "utf8mb4_0900_ai_ci", "4"]
        ])

      _ ->
        {:error, "42601", "SHOW #{String.upcase(words)} is not something kurwadb can show"}
    end
  end

  defp describe(_rest) do
    columns = for name <- ~w(Field Type Null Key Default Extra), do: {name, :text}
    {:rows, columns, [["key", "varchar(255)", "NO", "PRI", nil, ""]], "SELECT 1"}
  end

  defp variables(words, state) do
    vars = state.session.sysvars |> Enum.map(fn {k, v} -> [k, to_string(v)] end) |> Enum.sort()
    rows(["Variable_name", "Value"], Enum.filter(vars, fn [k, _] -> k in like([k], words) end))
  end

  # SHOW ... LIKE 'pattern', with % and _.
  defp like(names, words) do
    case Enum.drop_while(words, &(&1 != "like")) do
      ["like", pattern | _] ->
        regex =
          pattern
          |> Regex.escape()
          |> String.replace("%", ".*")
          |> String.replace("_", ".")
          |> then(&Regex.compile!("^" <> &1 <> "$", "i"))

        Enum.filter(names, &Regex.match?(regex, &1))

      _ ->
        names
    end
  end

  defp rows(name, rows) when is_binary(name), do: rows([name], rows)

  defp rows(names, rows),
    do: {:rows, Enum.map(names, &{&1, :text}), rows, "SELECT #{length(rows)}"}

  defp sets do
    case Kurwa.Namespace.list() do
      {:ok, %{sets: sets}} -> Enum.sort([Parser.default_table() | sets])
      _ -> [Parser.default_table()]
    end
  end

  # ----------------------------------------------------------------- packets

  defp status(state, more? \\ false), do: Proto.status(state.in_transaction, more?)

  defp ok(state, affected \\ 0, more? \\ false),
    do: Proto.ok(affected, status(state, more?), length(state.warnings))

  defp eof_unless_deprecated(state),
    do: if(Proto.deprecate_eof?(state.caps), do: [], else: [Proto.eof(status(state))])

  defp terminator(state, more?) do
    if Proto.deprecate_eof?(state.caps),
      do: Proto.ok(0, status(state, more?), length(state.warnings), 0xFE),
      else: Proto.eof(status(state, more?), length(state.warnings))
  end

  defp affected({:command, "INSERT 0 " <> n}), do: String.to_integer(n)
  defp affected({:command, "DELETE " <> n}), do: String.to_integer(n)
  defp affected(_), do: 0

  defp unknown_database(name), do: Proto.err(1049, "42000", "Unknown database '#{name}'")

  # SQLSTATE from the executor, as the MySQL error code and state a client
  # expects for the same failure.
  defp error(sqlstate, message) do
    {code, state} =
      case sqlstate do
        "42601" -> {1064, "42000"}
        "0A000" -> {1235, "42000"}
        "42883" -> {1305, "42000"}
        "42703" -> {1054, "42S22"}
        "42P07" -> {1050, "42S01"}
        "3F000" -> {1049, "42000"}
        "42602" -> {1103, "42000"}
        "23502" -> {1048, "23000"}
        "08P01" -> {1210, "HY000"}
        "22P02" -> {1366, "HY000"}
        "57P03" -> {1105, "HY000"}
        _ -> {1105, "HY000"}
      end

    Proto.err(code, state, message)
  end
end
