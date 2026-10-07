defmodule Kurwa.Pg.Server do
  @moduledoc """
  A PostgreSQL server, one process per connection, so `psql` and the usual
  drivers can talk to kurwadb with no client library of its own.

  The wire is protocol 3.0: the simple query protocol that `psql` uses, and the
  extended one (Parse, Bind, Describe, Execute, Sync) that drivers use for
  parameters and prepared statements. What the statements can say is
  `Kurwa.Sql.Parser`'s; this module is the conversation around them.

  * TLS is declined (`N` to SSLRequest). libpq's default `sslmode=prefer` then
    carries on in the clear; a client that requires TLS will refuse, which is
    the honest outcome until there is TLS to offer.
  * With `auth_token` configured, the password is the token, sent cleartext -
    which is only reasonable on a network you trust, as everything else here.
    Without it, any user and password are accepted.
  * There are no transactions. The status a driver sees (`I`, `T`, `E`) moves
    as PostgreSQL's would, because drivers decide whether to send BEGIN and
    COMMIT from it, but every statement has taken effect by the time it
    answers.
  * CancelRequest is ignored: every statement is a bounded quorum call and is
    over before a cancel could matter.

  Like 9P, this is a client frontend only. Nodes replicate to each other over
  Erlang distribution.
  """

  use ThousandIsland.Handler

  alias Kurwa.Pg.{Auth, Catalog, Proto, Types}
  alias Kurwa.Sql.{Exec, Parser}

  require Logger

  # Settings that PostgreSQL reports back with ParameterStatus when they change.
  @reported ~w(application_name client_encoding datestyle timezone integer_datetimes
               standard_conforming_strings intervalstyle is_superuser server_encoding
               server_version session_authorization)

  @impl ThousandIsland.Handler
  def handle_connection(socket, _state) do
    Kurwa.Metrics.connect(:pg, socket)

    {:continue,
     %{
       phase: :startup,
       buffer: <<>>,
       session: nil,
       status: :idle,
       statements: %{},
       portals: %{},
       skipping: false
     }}
  end

  # Replies are collected and written once the input in hand has been handled,
  # rather than a syscall per message: a prepared statement's Bind, Execute and
  # Sync then cost one write instead of three. Under pgbench that took prepared
  # statements from 70 000 to 136 000 transactions a second.
  @impl ThousandIsland.Handler
  def handle_data(data, socket, state) do
    result = loop(state.buffer <> data, socket, state)
    flush(socket)
    result
  end

  # ------------------------------------------------------------------ startup

  defp loop(buffer, socket, %{phase: :startup} = state) do
    case Proto.decode_startup(buffer) do
      {:ssl, rest} ->
        case tls_options() do
          nil ->
            send!(socket, "N")
            loop(rest, socket, state)

          options ->
            upgrade(socket, options, rest, state)
        end

      {:gss, rest} ->
        send!(socket, "N")
        loop(rest, socket, state)

      {:cancel, _pid, _key, _rest} ->
        {:close, state}

      {:startup, {3, minor}, params, rest} ->
        state = %{state | session: session(params)}

        # A newer client offers 3.2 and protocol extensions (_pq_.*); say what
        # we speak and carry on in 3.0.
        unsupported = params |> Map.keys() |> Enum.filter(&String.starts_with?(&1, "_pq_."))

        if minor > 0 or unsupported != [],
          do: send!(socket, Proto.negotiate_protocol(0, unsupported))

        case Kurwa.Config.auth_token() do
          nil ->
            finish_startup(socket, state)
            loop(rest, socket, %{state | phase: :ready})

          _token ->
            {challenge, phase} = challenge()
            send!(socket, challenge)
            loop(rest, socket, %{state | phase: phase})
        end

      {:startup, {major, minor}, _params, _rest} ->
        send!(socket, Proto.fatal("0A000", "unsupported frontend protocol #{major}.#{minor}"))
        {:close, state}

      :more ->
        {:continue, %{state | buffer: buffer}}

      {:error, reason} ->
        Logger.warning("kurwadb pg: dropping connection, bad startup packet: #{inspect(reason)}")
        {:close, state}
    end
  end

  defp loop(buffer, socket, %{phase: phase} = state) when phase != :ready do
    case Proto.decode(buffer) do
      {:ok, {:password_message, body}, rest} ->
        case authenticate(phase, body, state) do
          {:continue, reply, phase} ->
            send!(socket, reply)
            loop(rest, socket, %{state | phase: phase})

          {:ok, final} ->
            send!(socket, final)
            finish_startup(socket, state)
            loop(rest, socket, %{state | phase: :ready})

          :error ->
            message = "password authentication failed for user \"#{state.session.user}\""
            send!(socket, Proto.fatal("28P01", message))
            {:close, state}
        end

      {:ok, _other, _rest} ->
        send!(socket, Proto.fatal("08P01", "expected a password message"))
        {:close, state}

      :more ->
        {:continue, %{state | buffer: buffer}}

      {:error, _} ->
        {:close, state}
    end
  end

  defp loop(buffer, socket, %{phase: :ready} = state) do
    case Proto.decode(buffer) do
      {:ok, :terminate, _rest} ->
        {:close, state}

      {:ok, message, rest} ->
        state = handle(message, socket, state)
        loop(rest, socket, state)

      :more ->
        {:continue, %{state | buffer: buffer}}

      {:error, reason} ->
        Logger.warning("kurwadb pg: dropping connection, undecodable message: #{inspect(reason)}")
        send!(socket, Proto.fatal("08P01", "invalid message"))
        {:close, state}
    end
  end

  # The method is pg_auth's: SCRAM-SHA-256 unless configured otherwise.
  defp challenge do
    case Kurwa.Config.get(:pg_auth) do
      :md5 ->
        salt = Auth.md5_salt()
        {Proto.auth_md5(salt), {:md5, salt}}

      :password ->
        {Proto.auth_cleartext(), :password}

      _scram ->
        {Proto.auth_sasl(Auth.mechanisms(channel_binding())), :sasl_initial}
    end
  end

  defp authenticate(:password, body, _state) do
    if Plug.Crypto.secure_compare(Proto.password(body), token()), do: {:ok, []}, else: :error
  end

  defp authenticate({:md5, salt}, body, state) do
    if Auth.md5_ok?(Proto.password(body), state.session.user, salt, token()),
      do: {:ok, []},
      else: :error
  end

  defp authenticate(:sasl_initial, body, _state) do
    {mechanism, client_first} = Proto.sasl_initial(body)
    binding = channel_binding()

    with true <- mechanism in Auth.mechanisms(binding),
         {:ok, server_first, exchange} <-
           Auth.scram_first(mechanism, client_first, token(), binding) do
      {:continue, Proto.auth_sasl_continue(server_first), {:sasl_final, exchange}}
    else
      _ -> :error
    end
  end

  defp authenticate({:sasl_final, exchange}, body, _state) do
    case Auth.scram_final(body, exchange) do
      {:ok, server_final} -> {:ok, Proto.auth_sasl_final(server_final)}
      {:error, _} -> :error
    end
  end

  defp token, do: to_string(Kurwa.Config.auth_token())

  # Set once the connection is TLS: the tls-server-end-point data of the
  # certificate it was made with.
  defp channel_binding, do: Process.get(:kurwa_pg_binding)

  # --------------------------------------------------------------------- TLS

  # TLS is on when pg_tls is configured: a keyword list of :ssl server options,
  # usually certfile and keyfile (KURWA_PG_TLS_CERT, KURWA_PG_TLS_KEY).
  defp tls_options do
    case Kurwa.Config.get(:pg_tls) do
      nil -> nil
      [] -> nil
      options -> options
    end
  end

  # PostgreSQL negotiates TLS inside the plain connection: SSLRequest, an "S",
  # then the handshake on the same socket. ThousandIsland keeps its socket for
  # the life of the connection, so from here this process serves the
  # connection itself, over :ssl, with the same protocol code.
  defp upgrade(socket, options, _rest, state) do
    raw = socket.socket
    :ok = :gen_tcp.send(raw, "S")

    case :ssl.handshake(raw, options, 10_000) do
      {:ok, tls} ->
        Process.put(:kurwa_pg_tls, tls)

        case certificate(options) do
          nil -> :ok
          der -> Process.put(:kurwa_pg_binding, Auth.end_point(der))
        end

        tls_loop(tls, socket, %{state | buffer: <<>>})

      {:error, reason} ->
        Logger.warning("kurwadb pg: TLS handshake failed: #{inspect(reason)}")
        {:close, state}
    end
  end

  # The certificate this server presents - the first in the chain - from the
  # options it was given, for channel binding.
  defp certificate(options) do
    cond do
      der = Keyword.get(options, :cert) ->
        if is_list(der), do: hd(der), else: der

      path = Keyword.get(options, :certfile) ->
        path |> File.read!() |> :public_key.pem_decode() |> hd() |> elem(1)

      true ->
        nil
    end
  end

  defp tls_loop(tls, socket, state) do
    case :ssl.recv(tls, 0) do
      {:ok, data} ->
        result = loop(state.buffer <> data, socket, state)
        flush(socket)

        case result do
          {:continue, state} ->
            tls_loop(tls, socket, state)

          {:close, state} ->
            :ssl.close(tls)
            {:close, state}
        end

      {:error, _closed} ->
        {:close, state}
    end
  end

  defp session(params) do
    user = Map.get(params, "user", "kurwadb")
    Kurwa.Metrics.identify(user: user, app: Map.get(params, "application_name"))

    %{
      user: user,
      database: Map.get(params, "database", user),
      pid: :rand.uniform(2_000_000_000),
      key: :rand.uniform(2_000_000_000),
      settings:
        params
        |> Map.drop(["user", "database", "options", "replication"])
        |> Map.reject(fn {k, _} -> String.starts_with?(k, "_pq_.") end)
        |> Map.new(fn {k, v} -> {String.downcase(k), v} end)
    }
  end

  defp finish_startup(socket, %{session: session}) do
    statuses = [
      {"server_version", "16.0"},
      {"server_encoding", "UTF8"},
      {"client_encoding", "UTF8"},
      {"DateStyle", "ISO, MDY"},
      {"TimeZone", "UTC"},
      {"integer_datetimes", "on"},
      {"standard_conforming_strings", "on"},
      {"IntervalStyle", "postgres"},
      {"is_superuser", "off"},
      {"session_authorization", session.user},
      {"application_name", Map.get(session.settings, "application_name", "")}
    ]

    send!(socket, [
      Proto.auth_ok(),
      Enum.map(statuses, fn {k, v} -> Proto.parameter_status(k, v) end),
      Proto.backend_key(session.pid, session.key),
      Proto.ready(:idle)
    ])
  end

  # ------------------------------------------------------------- simple query

  defp handle({:query, _} = message, socket, state),
    do: Kurwa.Metrics.measure(:pg, fn -> run_query(message, socket, state) end)

  defp handle({:execute, _, _} = message, socket, %{skipping: false} = state),
    do: Kurwa.Metrics.measure(:pg, fn -> run_execute(message, socket, state) end)

  defp handle(message, socket, state), do: handle_other(message, socket, state)

  defp run_query({:query, sql}, socket, state) do
    Logger.debug("kurwadb pg query: #{sql}")

    state =
      case Parser.parse(sql) do
        {:ok, statements} ->
          Enum.reduce_while(statements, state, fn statement, state ->
            case run(statement, [], socket, state, nil) do
              {:ok, state} -> {:cont, state}
              {:error, state} -> {:halt, state}
            end
          end)

        {:error, code, message} ->
          fail(socket, state, code, message)
      end

    send!(socket, Proto.ready(state.status))
    state
  end

  # ----------------------------------------------------------- extended query

  # After an error, the extended protocol discards everything up to Sync.
  defp handle_other(:sync, socket, state) do
    send!(socket, Proto.ready(state.status))
    %{state | skipping: false}
  end

  defp handle_other(_message, _socket, %{skipping: true} = state), do: state

  defp handle_other(:flush, _socket, state), do: state

  defp handle_other({:parse, name, sql, oids}, socket, state) do
    Logger.debug("kurwadb pg parse: #{sql}")

    with :ok <- free_statement(state, name),
         {:ok, statement} <- one_statement(sql) do
      {inferred, columns} = describe(statement)
      types = merge_types(oids, inferred)

      entry = %{sql: sql, statement: statement, types: types, columns: columns}
      send!(socket, Proto.parse_complete())
      %{state | statements: Map.put(state.statements, name, entry)}
    else
      {:error, code, message} -> skip(fail(socket, state, code, message))
    end
  end

  defp handle_other({:bind, portal, name, formats, values, result_formats}, socket, state) do
    with {:ok, entry} <- fetch_statement(state, name),
         {:ok, params} <- decode_params(entry, formats, values) do
      bound = %{entry: entry, params: params, formats: result_formats, pending: nil}
      send!(socket, Proto.bind_complete())
      %{state | portals: Map.put(state.portals, portal, bound)}
    else
      {:error, code, message} -> skip(fail(socket, state, code, message))
    end
  end

  defp handle_other({:describe, :statement, name}, socket, state) do
    case fetch_statement(state, name) do
      {:ok, entry} ->
        oids = Enum.map(entry.types, &elem(Types.oid(&1), 0))
        send!(socket, [Proto.parameter_description(oids), rows_or_no_data(entry.columns, [])])
        state

      {:error, code, message} ->
        skip(fail(socket, state, code, message))
    end
  end

  defp handle_other({:describe, :portal, name}, socket, state) do
    case Map.fetch(state.portals, name) do
      {:ok, portal} ->
        send!(socket, rows_or_no_data(portal.entry.columns, portal.formats))
        state

      :error ->
        skip(fail(socket, state, "34000", "portal \"#{name}\" does not exist"))
    end
  end

  defp handle_other({:close, :statement, name}, socket, state) do
    send!(socket, Proto.close_complete())
    %{state | statements: Map.delete(state.statements, name)}
  end

  defp handle_other({:close, :portal, name}, socket, state) do
    send!(socket, Proto.close_complete())
    %{state | portals: Map.delete(state.portals, name)}
  end

  defp handle_other({:password_message, _}, socket, state),
    do: fail(socket, state, "08P01", "unexpected password message")

  defp handle_other({:unsupported, type}, socket, state) do
    message =
      case type do
        ?F -> "the function-call protocol is not supported"
        t when t in [?d, ?c, ?f] -> "COPY is not supported"
        t -> "unsupported message type #{inspect(<<t>>)}"
      end

    skip(fail(socket, state, "0A000", message))
  end

  defp run_execute({:execute, name, max_rows}, socket, state) do
    case Map.fetch(state.portals, name) do
      {:ok, %{pending: {rows, tag}} = portal} ->
        continue_portal(socket, state, name, portal, rows, tag, max_rows)

      {:ok, portal} ->
        case run(portal.entry.statement, portal.params, socket, state, {name, portal, max_rows}) do
          {:ok, state} -> state
          {:error, state} -> skip(state)
        end

      :error ->
        skip(fail(socket, state, "34000", "portal \"#{name}\" does not exist"))
    end
  end

  defp skip(state), do: %{state | skipping: true}

  defp one_statement(sql) do
    case Parser.parse(sql) do
      {:ok, [statement]} ->
        {:ok, statement}

      {:ok, _many} ->
        {:error, "42601", "cannot insert multiple commands into a prepared statement"}

      error ->
        error
    end
  end

  # PostgreSQL's rule: the unnamed statement is replaced freely, a named one
  # must be closed first.
  defp free_statement(_state, ""), do: :ok

  defp free_statement(state, name) do
    if Map.has_key?(state.statements, name),
      do: {:error, "42P05", "prepared statement \"#{name}\" already exists"},
      else: :ok
  end

  defp fetch_statement(state, name) do
    case Map.fetch(state.statements, name) do
      {:ok, entry} -> {:ok, entry}
      :error -> {:error, "26000", "prepared statement \"#{name}\" does not exist"}
    end
  end

  defp describe({:catalog, sql}), do: {[], Catalog.columns(sql)}
  defp describe(statement), do: Exec.describe(statement)

  # Types the client declared win; zero means "you decide", and so does a
  # parameter the client did not mention.
  defp merge_types(oids, inferred) do
    count = max(length(oids), length(inferred))

    for i <- 0..(count - 1)//1 do
      case Enum.at(oids, i, 0) do
        0 -> Enum.at(inferred, i, :text)
        oid -> Types.from_oid(oid)
      end
    end
  end

  defp decode_params(entry, formats, values) do
    if length(values) != length(entry.types) and entry.types != [] do
      {:error, "08P01",
       "bind message supplies #{length(values)} parameters, but prepared statement requires #{length(entry.types)}"}
    else
      params =
        values
        |> Enum.with_index()
        |> Enum.map(fn {value, i} ->
          type = Enum.at(entry.types, i, :text)
          {oid, _} = Types.oid(type)
          Types.decode(value, oid, format_at(formats, i))
        end)

      {:ok, params}
    end
  end

  defp format_at([], _i), do: 0
  defp format_at([format], _i), do: format
  defp format_at(formats, i), do: Enum.at(formats, i, 0)

  defp rows_or_no_data(nil, _formats), do: Proto.no_data()

  defp rows_or_no_data(columns, formats) do
    formats = for i <- 0..(length(columns) - 1)//1, do: format_at(formats, i)
    Proto.row_description(columns, formats)
  end

  # ------------------------------------------------------------------ running

  # `portal` is nil for the simple protocol, which sends a RowDescription with
  # the rows; the extended protocol sent it at Describe, and may page.
  defp run(statement, params, socket, state, portal) do
    if state.status == :failed and not ending?(statement) do
      message = "current transaction is aborted, commands ignored until end of transaction block"
      {:error, fail(socket, state, "25P02", message)}
    else
      result =
        case Exec.run(statement, params, state.session) do
          {:catalog, sql} -> Catalog.answer(sql, state.session, params)
          other -> other
        end

      send!(socket, Exec.take_notices() |> Enum.map(&Proto.warning/1))
      respond(result, statement, socket, state, portal)
    end
  end

  defp ending?({:utility, kind, _}), do: kind in [:commit, :rollback]
  defp ending?(_), do: false

  defp respond({:error, code, message}, _statement, socket, state, _portal),
    do: {:error, fail(socket, state, code, message)}

  defp respond(:empty, _statement, socket, state, _portal) do
    send!(socket, Proto.empty_query())
    {:ok, state}
  end

  defp respond({:rows, columns, rows, tag}, _statement, socket, state, nil) do
    formats = List.duplicate(0, length(columns))

    send!(socket, [
      Proto.row_description(columns, formats),
      encode_rows(columns, rows, formats),
      Proto.command_complete(tag)
    ])

    {:ok, state}
  end

  defp respond({:rows, columns, rows, tag}, _statement, socket, state, {name, portal, max_rows}) do
    # Describe told the client the shape from the statement; a catalog query
    # only knows its columns once it has run, so it is the same either way.
    portal = %{portal | entry: %{portal.entry | columns: columns}}
    {:ok, continue_portal(socket, state, name, portal, rows, tag, max_rows)}
  end

  defp respond({:command, tag}, statement, socket, state, _portal) do
    send!(socket, Proto.command_complete(tag))
    {:ok, transition(state, statement)}
  end

  defp respond({:set, name, value, tag}, _statement, socket, state, _portal) do
    session = %{state.session | settings: Map.put(state.session.settings, name, value)}
    if name == "application_name", do: Kurwa.Metrics.identify(app: value)

    if name in @reported, do: send!(socket, Proto.parameter_status(reported_name(name), value))
    send!(socket, Proto.command_complete(tag))
    {:ok, %{state | session: session}}
  end

  defp respond({:deallocate, name, tag}, _statement, socket, state, _portal) do
    statements = if name == :all, do: %{}, else: Map.delete(state.statements, name)
    send!(socket, Proto.command_complete(tag))
    {:ok, %{state | statements: statements}}
  end

  defp continue_portal(socket, state, name, portal, rows, tag, max_rows) do
    formats = portal_formats(portal.entry.columns, portal.formats)
    columns = portal.entry.columns

    if max_rows > 0 and length(rows) > max_rows do
      {now, later} = Enum.split(rows, max_rows)
      send!(socket, [encode_rows(columns, now, formats), Proto.portal_suspended()])
      %{state | portals: Map.put(state.portals, name, %{portal | pending: {later, tag}})}
    else
      send!(socket, [encode_rows(columns, rows, formats), Proto.command_complete(tag)])
      %{state | portals: Map.put(state.portals, name, %{portal | pending: nil})}
    end
  end

  defp portal_formats(columns, formats) do
    for i <- 0..(length(columns || []) - 1)//1, do: format_at(formats, i)
  end

  defp encode_rows(columns, rows, formats) do
    types = Enum.map(columns, &elem(&1, 1))

    Enum.map(rows, fn row ->
      row
      |> Enum.zip(Enum.zip(types, formats))
      |> Enum.map(fn {value, {type, format}} -> Types.encode(value, type, format) end)
      |> Proto.data_row()
    end)
  end

  defp transition(state, {:utility, :begin, _}), do: %{state | status: :transaction}

  defp transition(state, {:utility, kind, _}) when kind in [:commit, :rollback],
    do: %{state | status: :idle}

  defp transition(state, _statement), do: state

  defp fail(socket, state, code, message) do
    Kurwa.Metrics.error(:pg)
    send!(socket, Proto.error(code, message))
    if state.status == :transaction, do: %{state | status: :failed}, else: state
  end

  defp reported_name("datestyle"), do: "DateStyle"
  defp reported_name("timezone"), do: "TimeZone"
  defp reported_name("intervalstyle"), do: "IntervalStyle"
  defp reported_name(name), do: name

  defp send!(_socket, iodata),
    do: Process.put(:kurwa_pg_out, [Process.get(:kurwa_pg_out, []), iodata])

  defp flush(socket) do
    case Process.delete(:kurwa_pg_out) do
      nil ->
        :ok

      out ->
        case Process.get(:kurwa_pg_tls) do
          nil -> ThousandIsland.Socket.send(socket, out)
          tls -> :ssl.send(tls, out)
        end
    end
  end
end
