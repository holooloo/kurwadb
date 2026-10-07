defmodule Kurwa.Mssql.Server do
  @moduledoc """
  A Microsoft SQL Server endpoint, one process per connection, so `sqlcmd`,
  ODBC Driver 18, the .NET and JDBC drivers, tedious and FreeTDS-based
  clients talk to kurwadb as they are.

  The statements are `Kurwa.Sql`'s in its `:tsql` dialect - `[brackets]`,
  `@named` parameters, `N'strings'`, `TOP`, `OUTPUT inserted.[key]`, and
  `IF NOT EXISTS (...) INSERT`, which goes through `add_new` - over the same
  executor as PostgreSQL and MySQL. A set is the same table: one column, `key`.

  What is SQL Server's own is the conversation, and most of it is TLS:

  * **Encryption.** Current drivers encrypt by default, and TDS 7.x does it
    inside its own framing - the TLS handshake travels in PRELOGIN packets
    (`Kurwa.Mssql.Transport`). A client that asks for encryption gets it for
    the whole connection; one that turns it off still sends LOGIN7 encrypted,
    and the connection carries on in the clear after it, as with SQL Server
    itself. TDS 8 ("strict"), TLS before anything else, is served too. With no
    certificate configured, a self-signed one is made at start, which is
    SQL Server's behaviour as well; clients then need `TrustServerCertificate`.
  * **Login.** SQL Server authentication, with `auth_token` as the password
    for any login. Integrated (Windows) authentication is refused by name.
  * **Parameters.** Drivers do not send parameterised SQL as text: they call
    `sp_executesql`, or `sp_prepare` / `sp_execute` / `sp_prepexec`, as RPCs
    with typed parameters, and those are answered as SQL Server would.
  * **Transactions.** There are none. `BEGIN TRAN`, `COMMIT` and ODBC's
    transaction-manager requests are acknowledged with the descriptors
    drivers track; `ROLLBACK` adds an informational message that nothing was
    rolled back.
  """

  use ThousandIsland.Handler

  alias Kurwa.Mssql.{Tds, Transport}
  alias Kurwa.Sql.Procedural

  require Logger

  @version Mix.Project.config()[:version]
  @database "kurwadb"
  @product "16.0.1000.6"

  @impl ThousandIsland.Handler
  def handle_connection(socket, state) do
    raw = socket.socket
    :inet.setopts(raw, active: false)

    try do
      serve(%{mode: :tcp, sock: raw, buf: <<>>, size: 4096, proxy: nil, raw: nil})
    catch
      :exit, reason -> Logger.debug("kurwadb mssql: connection ended: #{inspect(reason)}")
    end

    {:close, state}
  end

  # ------------------------------------------------------------ connecting

  defp serve(conn) do
    with {:ok, conn} <- fill(conn) do
      case conn.buf do
        # TDS 8: a TLS ClientHello before any TDS at all.
        <<0x16, _::binary>> -> strict(conn)
        _ -> prelogin(conn)
      end
    end
  end

  defp strict(conn) do
    {:ok, proxy} = Transport.start(conn.sock, conn.buf, false)

    options =
      tls_options() ++ [versions: [:"tlsv1.3", :"tlsv1.2"], alpn_preferred_protocols: ["tds/8.0"]]

    case :ssl.handshake(proxy, [{:cb_info, Transport.cb_info()} | options], 15_000) do
      {:ok, tls} ->
        conn = %{conn | mode: :ssl, sock: tls, buf: <<>>}

        with {:ok, :prelogin, _payload, conn} <- recv(conn) do
          conn = send_message(conn, :reply, Tds.prelogin_response(:on))
          login(conn, :full)
        end

      {:error, reason} ->
        Logger.warning("kurwadb mssql: strict TLS handshake failed: #{inspect(reason)}")
    end
  end

  defp prelogin(conn) do
    with {:ok, :prelogin, payload, conn} <- recv(conn) do
      encryption =
        case {Tds.encryption(Tds.prelogin(payload)), Kurwa.Config.get(:mssql_encryption)} do
          {_, :off} -> :not_supported
          {client, _} when client in [:on, :required] -> :on
          {:off, _} -> :off
          {:not_supported, _} -> :not_supported
        end

      conn = send_message(conn, :reply, Tds.prelogin_response(encryption))

      case encryption do
        :not_supported -> login(conn, :none)
        :on -> conn |> wrapped_tls() |> login(:full)
        # Off, but both sides can: LOGIN7 alone goes encrypted.
        :off -> conn |> wrapped_tls() |> login(:login_only)
      end
    end
  end

  defp wrapped_tls(conn) do
    {:ok, proxy} = Transport.start(conn.sock, conn.buf, true)
    options = [{:cb_info, Transport.cb_info()}, {:versions, [:"tlsv1.2"]} | tls_options()]

    case :ssl.handshake(proxy, options, 15_000) do
      {:ok, tls} ->
        Transport.raw(proxy)
        %{conn | mode: :ssl, sock: tls, buf: <<>>, proxy: proxy, raw: conn.sock}

      {:error, reason} ->
        exit({:tls_handshake, reason})
    end
  end

  defp login(conn, encryption) do
    with {:ok, :login7, payload, conn} <- recv(conn) do
      # Login-only encryption: the rest of the connection is plain TCP again,
      # and TLS must stop without a close alert reaching the client.
      conn =
        if encryption == :login_only do
          socket = Transport.release(conn.proxy, conn.sock)
          %{conn | mode: :tcp, sock: socket, buf: <<>>, proxy: nil}
        else
          conn
        end

      case Tds.login7(payload) do
        {:ok, login} ->
          accept(conn, login)

        {:error, _} ->
          send_message(conn, :reply, [
            Tds.notice(:error, 18456, 14, "Login failed: malformed LOGIN7."),
            Tds.done(:done, error: true)
          ])
      end
    end
  end

  defp accept(conn, login) do
    token = Kurwa.Config.auth_token()

    cond do
      login.integrated ->
        refuse(
          conn,
          18452,
          "Login failed. Integrated (Windows) authentication is not supported; use SQL Server authentication."
        )

      token != nil and not Plug.Crypto.secure_compare(login.password, to_string(token)) ->
        refuse(conn, 18456, "Login failed for user '#{login.user}'.")

      login.database not in ["", @database, "master"] ->
        refuse(
          conn,
          4060,
          "Cannot open database \"#{login.database}\" requested by the login. The login failed."
        )

      true ->
        size = if login.packet_size in 512..32_767, do: login.packet_size, else: 4096
        conn = %{conn | size: size}

        tokens = [
          Tds.envchange(1, @database, "master"),
          Tds.notice(:info, 5701, 0, "Changed database context to '#{@database}'."),
          Tds.envchange(:collation),
          Tds.envchange(2, "us_english", ""),
          Tds.notice(:info, 5703, 0, "Changed language setting to us_english."),
          Tds.loginack(),
          Tds.envchange(4, Integer.to_string(size), "4096"),
          if(login.features != [], do: Tds.featureextack(), else: []),
          Tds.done(:done)
        ]

        conn = send_message(conn, :reply, tokens)
        commands(conn, session(login))
    end
  end

  defp refuse(conn, number, message) do
    send_message(conn, :reply, [
      Tds.notice(:error, number, 14, message),
      Tds.done(:done, error: true)
    ])
  end

  defp session(login) do
    spid = 51 + rem(System.unique_integer([:positive]), 30_000)

    %{
      user: login.user,
      app: login.app,
      database: @database,
      pid: spid,
      settings: %{},
      sysvars: %{},
      server_properties: %{
        "productversion" => @product,
        "productmajorversion" => "16",
        "productlevel" => "RTM",
        "edition" => "kurwadb",
        "engineedition" => 3,
        "servername" => to_string(node()),
        "machinename" => to_string(node()),
        "collation" => "SQL_Latin1_General_CP1_CI_AS",
        "isclustered" => 0,
        "ishadrenabled" => 0,
        "isintegratedsecurityonly" => 0
      },
      transaction: nil,
      tx_next: 1,
      nocount: false,
      prepared: %{},
      next_handle: 1,
      rowcount: 0
    }
  end

  # ----------------------------------------------------------- the commands

  defp commands(conn, session) do
    case recv(conn) do
      {:ok, :sql_batch, payload, conn} ->
        {_descriptor, text} = Tds.all_headers(payload)
        sql = Tds.utf8(text)
        Logger.debug("kurwadb mssql batch: #{sql}")

        {tokens, session} =
          Kurwa.Metrics.measure(:mssql, fn ->
            guarded(sql, session, fn -> batch(sql, %{}, session, :done) end)
          end)

        conn |> send_message(:reply, tokens) |> commands(session)

      {:ok, :rpc, payload, conn} ->
        Logger.debug("kurwadb mssql rpc: #{Base.encode16(payload)}")

        {tokens, session} =
          Kurwa.Metrics.measure(:mssql, fn ->
            guarded("RPC", session, fn -> rpcs(payload, session) end)
          end)

        conn |> send_message(:reply, tokens) |> commands(session)

      {:ok, :transaction_manager, payload, conn} ->
        {tokens, session} = transaction_manager(payload, session)
        conn |> send_message(:reply, tokens) |> commands(session)

      {:ok, :attention, _payload, conn} ->
        conn |> send_message(:reply, Tds.done(:done, attention: true)) |> commands(session)

      {:ok, other, _payload, conn} ->
        message = "TDS message type #{inspect(other)} is not supported"

        conn
        |> send_message(:reply, [
          Tds.notice(:error, 50_000, 16, message),
          Tds.done(:done, error: true)
        ])
        |> commands(session)

      :closed ->
        :ok
    end
  end

  # A request kurwadb fails on is answered with an error, never by dropping the
  # connection: SSMS and friends treat a dropped connection as "cannot connect".
  defp guarded(what, session, fun) do
    fun.()
  rescue
    e ->
      Logger.error(
        "kurwadb mssql: #{Exception.format(:error, e, __STACKTRACE__)}\nwhile running: #{what}"
      )

      {[
         error("XX000", "kurwadb failed on this request: #{Exception.message(e)}"),
         Tds.done(:done, error: true)
       ], session}
  end

  # A batch: the procedural interpreter runs it - a batch may DECLARE, IF and
  # EXEC like a procedure body - and its events become tokens. `kind` is :done
  # for a SQL batch, :in_proc inside sp_executesql.
  defp batch(sql, params, session, kind) do
    session = refresh(session)

    case Procedural.parse_body(sql) do
      {:ok, statements} ->
        ctx = %{
          session: session,
          vars: params,
          procedure: &Kurwa.Procedures.lookup/1,
          observe: &observe/3
        }

        {events, after_batch} = Procedural.run_batch(statements, ctx)
        {tokens(events, session, kind), after_batch}

      {:error, code, message} ->
        {[error(code, message), Tds.done(kind, error: true)], session}
    end
  end

  # Called by the interpreter as each statement completes, so @@ROWCOUNT,
  # @@TRANCOUNT and NOCOUNT are current for the next one.
  defp observe(statement, result, session) do
    session =
      case {statement, result} do
        {{:set, _}, _} ->
          nocount(statement, session)

        {_, {:command, tag}} ->
          {_envchange, session} = transaction(statement, session)
          %{session | rowcount: affected(tag) || 0}

        {_, {:rows, _, rows, _}} ->
          %{session | rowcount: length(rows)}

        _ ->
          session
      end

    refresh(session)
  end

  # Events to tokens. Every DONE but the last says more follows. Transactions
  # are replayed from the session the batch started with, which gives the same
  # descriptors observe/3 gave, for the ENVCHANGEs drivers track.
  defp tokens(events, session, kind) do
    {groups, _session} = Enum.flat_map_reduce(events, session, &event_tokens(&1, &2, kind))

    last = groups |> Enum.with_index() |> Enum.filter(&match?({{:done, _}, _}, &1)) |> List.last()
    last = if last, do: elem(last, 1), else: nil

    tokens =
      groups
      |> Enum.with_index()
      |> Enum.map(fn
        {{:done, build}, i} -> build.(i != last)
        {{:plain, iodata}, _} -> iodata
      end)

    if last == nil, do: [tokens, Tds.done(kind)], else: tokens
  end

  defp event_tokens({:result, statement, result, depth}, session, kind) do
    kind = if depth == 0, do: kind, else: :in_proc
    session = nocount(statement, session)

    {envchange, session} =
      case result do
        {:command, _} -> transaction(statement, session)
        _ -> {[], session}
      end

    # Inside a procedure, SQL Server sends no DONEINPROC for a SET, nor - with
    # NOCOUNT ON, which is what NOCOUNT is for - for any statement without a
    # result set. Clients read OUTPUT values only once the results are done,
    # so the extra tokens are not harmless: pymssql then reads stale outputs.
    silent? =
      kind == :in_proc and
        (match?({:set, _}, statement) or
           (session.nocount and not match?({:rows, _, _, _}, result)))

    if silent? and envchange == [] do
      {[], session}
    else
      result_group(statement, result, session, kind, envchange)
    end
  end

  defp event_tokens({:notice, text}, session, _kind),
    do: {[{:plain, Tds.notice(:info, 0, 0, text)}], session}

  defp event_tokens({:raised, number, text}, session, _kind),
    do: {[{:plain, Tds.notice(:error, number, 16, text)}], session}

  defp event_tokens({:error, number, text}, session, kind) do
    token =
      case number do
        {:sqlstate, code} -> error(code, text)
        n -> Tds.notice(:error, n, 16, text)
      end

    {[{:done, fn more -> [token, Tds.done(kind, more: more, error: true)] end}], session}
  end

  defp event_tokens({:proc, _name, code, depth}, session, _kind) do
    _ = depth
    {[{:done, fn more -> [Tds.return_status(code), Tds.done(:proc, more: more)] end}], session}
  end

  defp result_group(_statement, result, session, kind, envchange) do
    result =
      case result do
        {:catalog, sql} -> Kurwa.Pg.Catalog.answer(sql, session)
        other -> other
      end

    group =
      case result do
        {:rows, columns, rows, _tag} ->
          columns =
            Enum.map(columns, fn {name, type} ->
              {if(name == "?column?", do: "", else: name), type}
            end)

          count = if session.nocount, do: nil, else: length(rows)

          {:done,
           fn more ->
             [
               Tds.colmetadata(columns),
               Enum.map(rows, &Tds.row(&1, columns)),
               Tds.done(kind, more: more, count: count, command: 0xC1)
             ]
           end}

        {:command, tag} ->
          n = affected(tag)
          count = if session.nocount or n == nil, do: nil, else: n

          {:done,
           fn more ->
             [envchange, Tds.done(kind, more: more, count: count, command: command(tag))]
           end}

        {:use, db} when db in [@database, "master"] ->
          {:done,
           fn more -> [Tds.envchange(1, @database, @database), Tds.done(kind, more: more)] end}

        {:use, db} ->
          message =
            "Database '#{db}' does not exist. Make sure that the name is entered correctly."

          {:done,
           fn more ->
             [Tds.notice(:error, 911, 16, message), Tds.done(kind, more: more, error: true)]
           end}

        _ ->
          {:done, fn more -> Tds.done(kind, more: more) end}
      end

    {[group], session}
  end

  defp nocount({:set, {name, _}}, session) do
    case String.split(name) do
      ["nocount", "on"] -> %{session | nocount: true}
      ["nocount", "off"] -> %{session | nocount: false}
      _ -> session
    end
  end

  defp nocount(_statement, session), do: session

  defp transaction({:utility, :begin, _}, %{transaction: nil} = session) do
    descriptor = session.tx_next

    {Tds.envchange(:begin, descriptor),
     %{session | transaction: descriptor, tx_next: descriptor + 1}}
  end

  defp transaction({:utility, kind, _}, %{transaction: d} = session)
       when kind in [:commit, :rollback] and d != nil,
       do: {Tds.envchange(kind, d), %{session | transaction: nil}}

  defp transaction(_statement, session), do: {[], session}

  # The values @@ reads change as the session does.
  defp refresh(session) do
    sysvars = %{
      "version" =>
        "Microsoft SQL Server 2022 (RTM) - #{@product} (kurwadb #{@version}, a distributed set that stores keys)",
      "spid" => session.pid,
      "servername" => to_string(node()),
      "servicename" => "MSSQLSERVER",
      "language" => "us_english",
      "trancount" => if(session.transaction, do: 1, else: 0),
      "rowcount" => session.rowcount,
      "identity" => nil,
      "error" => 0,
      "datefirst" => 7,
      "textsize" => 2_147_483_647,
      "lock_timeout" => -1,
      "max_connections" => 32_767,
      "microsoftversion" => 0x10000000 + 1000,
      "options" => if(session.nocount, do: 512, else: 0)
    }

    %{session | sysvars: sysvars}
  end

  defp affected("INSERT 0 " <> n), do: String.to_integer(n)
  defp affected("DELETE " <> n), do: String.to_integer(n)
  defp affected(_), do: nil

  defp command("INSERT" <> _), do: 0xC3
  defp command("DELETE" <> _), do: 0xC4
  defp command(_), do: 0

  # --------------------------------------------------------------------- RPC

  defp rpcs(payload, session) do
    {_descriptor, rest} = Tds.all_headers(payload)

    requests =
      try do
        {:ok, Tds.rpcs(rest)}
      catch
        {:tds_unsupported_type, type} ->
          {:error, "parameter type 0x#{Integer.to_string(type, 16)} is not supported"}
      end

    case requests do
      {:ok, requests} ->
        last = length(requests) - 1

        requests
        |> Enum.with_index()
        |> Enum.reduce({[], session}, fn {request, i}, {acc, session} ->
          {tokens, session} = rpc(request, session, i < last)
          {[acc, tokens], session}
        end)

      {:error, message} ->
        {[Tds.notice(:error, 8016, 16, message), Tds.done(:proc, error: true)], session}
    end
  end

  defp rpc({"sp_executesql", [{_, sql, _} | rest]}, session, more?) do
    {tokens, session} = batch(sql, named(Enum.drop(rest, 1)), session, :in_proc)
    {[tokens, Tds.return_status(0), Tds.done(:proc, more: more?)], session}
  end

  defp rpc({"sp_prepare", [{handle_name, _, true}, _decl, {_, sql, _} | _]}, session, more?) do
    {handle, session} = prepare(sql, session)

    {[
       Tds.return_status(0),
       Tds.return_value(0, handle_name, handle),
       Tds.done(:proc, more: more?)
     ], session}
  end

  defp rpc({"sp_prepexec", [{handle_name, _, true}, _decl, {_, sql, _} | values]}, session, more?) do
    {handle, session} = prepare(sql, session)
    {tokens, session} = batch(sql, named(values), session, :in_proc)

    {[
       tokens,
       Tds.return_status(0),
       Tds.return_value(0, handle_name, handle),
       Tds.done(:proc, more: more?)
     ], session}
  end

  defp rpc({"sp_execute", [{_, handle, _} | values]}, session, more?) do
    case Map.fetch(session.prepared, handle) do
      {:ok, sql} ->
        {tokens, session} = batch(sql, named(values), session, :in_proc)
        {[tokens, Tds.return_status(0), Tds.done(:proc, more: more?)], session}

      :error ->
        {[
           Tds.notice(
             :error,
             8179,
             16,
             "Could not find prepared statement with handle #{handle}."
           ),
           Tds.done(:proc, error: true, more: more?)
         ], session}
    end
  end

  defp rpc({"sp_unprepare", [{_, handle, _} | _]}, session, more?) do
    {[Tds.return_status(0), Tds.done(:proc, more: more?)],
     %{session | prepared: Map.delete(session.prepared, handle)}}
  end

  # A procedure by name: how ODBC's {call ...}, tedious's callProcedure and
  # pymssql's callproc reach one.
  defp rpc({name, params}, session, more?) when is_binary(name) and name != "" do
    case Kurwa.Procedures.lookup(name) do
      nil ->
        not_found(name, session, more?)

      proc ->
        args =
          Enum.map(params, fn {param, value, output} ->
            %{
              param: param |> String.trim_leading("@") |> String.downcase() |> blank_to_nil(),
              value: value,
              output: output
            }
          end)

        ctx = %{
          session: refresh(session),
          procedure: &Kurwa.Procedures.lookup/1,
          observe: &observe/3
        }

        {events, code, outputs, session_after} = Procedural.call(proc, args, ctx)
        inner = tokens(events, session, :in_proc)

        if code == nil do
          {[inner, Tds.done(:proc, error: true, more: more?)], session_after}
        else
          {[
             inner,
             Tds.return_status(code),
             return_values(params, proc, outputs),
             Tds.done(:proc, more: more?)
           ], session_after}
        end
    end
  end

  defp rpc({name, _params}, session, more?), do: not_found(name, session, more?)

  # OUTPUT values go back for the parameters the client marked as output, at
  # the place it sent them.
  defp return_values(params, proc, outputs) do
    by_name = Map.new(outputs, fn {name, type, value} -> {name, {type, value}} end)

    params
    |> Enum.with_index()
    |> Enum.flat_map(fn {{name, _value, output}, i} ->
      param = name |> String.trim_leading("@") |> String.downcase()
      param = if param == "", do: (Enum.at(proc.params, i) || %{name: ""}).name, else: param

      case {output, Map.fetch(by_name, param)} do
        {true, {:ok, {type, value}}} -> [Tds.return_value(i, "@" <> param, value, type)]
        _ -> []
      end
    end)
  end

  defp blank_to_nil(""), do: nil
  defp blank_to_nil(name), do: name

  defp not_found(name, session, more?) do
    shown = if is_binary(name), do: name, else: inspect(name)

    message =
      "Could not find stored procedure '#{shown}'. Procedures are .sql files in the node's procedures_dir."

    {[Tds.notice(:error, 2812, 16, message), Tds.done(:proc, error: true, more: more?)], session}
  end

  defp prepare(sql, session) do
    handle = session.next_handle

    {handle,
     %{session | prepared: Map.put(session.prepared, handle, sql), next_handle: handle + 1}}
  end

  # RPC parameters arrive named - @P1, @key - and statements refer to them so.
  defp named(params),
    do:
      Map.new(params, fn {name, value, _out} ->
        {name |> String.trim_leading("@") |> String.downcase(), value}
      end)

  # ----------------------------------------------------- transaction manager

  defp transaction_manager(payload, session) do
    {_descriptor, rest} = Tds.all_headers(payload)

    case rest do
      # TM_BEGIN_XACT
      <<5::16-little, _::binary>> ->
        transaction_reply(transaction({:utility, :begin, nil}, %{session | transaction: nil}))

      # TM_COMMIT_XACT, possibly beginning the next one at once.
      <<7::16-little, name_len, _name::binary-size(name_len * 2), begin_next, _::binary>> ->
        {commit, session} = transaction({:utility, :commit, nil}, session)

        if begin_next == 1 do
          {begin, session} = transaction({:utility, :begin, nil}, session)
          {[commit, begin, Tds.done(:done)], session}
        else
          {[commit, Tds.done(:done)], session}
        end

      # TM_ROLLBACK_XACT
      <<8::16-little, name_len, _name::binary-size(name_len * 2), begin_next, _::binary>> ->
        {rollback, session} = transaction({:utility, :rollback, nil}, session)

        info =
          Tds.notice(
            :info,
            0,
            0,
            "kurwadb has no transactions: every statement took effect when it ran"
          )

        if begin_next == 1 do
          {begin, session} = transaction({:utility, :begin, nil}, session)
          {[rollback, info, begin, Tds.done(:done)], session}
        else
          {[rollback, info, Tds.done(:done)], session}
        end

      _ ->
        {[Tds.done(:done)], session}
    end
  end

  defp transaction_reply({envchange, session}), do: {[envchange, Tds.done(:done)], session}

  # ------------------------------------------------------------------ errors

  # SQLSTATE from the executor, as the SQL Server message number and class a
  # client expects for the same failure.
  defp error(sqlstate, message) do
    Kurwa.Metrics.error(:mssql)

    {number, class} =
      case sqlstate do
        "42601" -> {102, 15}
        "42883" -> {195, 15}
        "42703" -> {207, 16}
        "42P07" -> {2714, 16}
        "3F000" -> {911, 16}
        "23502" -> {515, 16}
        "08P01" -> {137, 15}
        "22P02" -> {245, 16}
        "22023" -> {245, 16}
        _ -> {50_000, 16}
      end

    Tds.notice(:error, number, class, message)
  end

  # ------------------------------------------------------------------- I/O

  # Reads one whole TDS message.
  defp recv(conn) do
    case Tds.message(conn.buf) do
      {:ok, type, payload, rest} ->
        {:ok, Tds.type_name(type), payload, %{conn | buf: rest}}

      :more ->
        case fill(conn) do
          {:ok, conn} -> recv(conn)
          :closed -> :closed
        end

      {:error, reason} ->
        Logger.warning("kurwadb mssql: bad packet: #{inspect(reason)}")
        :closed
    end
  end

  defp fill(%{mode: :tcp, sock: sock} = conn) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, data} -> {:ok, %{conn | buf: conn.buf <> data}}
      {:error, _} -> :closed
    end
  end

  defp fill(%{mode: :ssl, sock: sock} = conn) do
    case :ssl.recv(sock, 0) do
      {:ok, data} -> {:ok, %{conn | buf: conn.buf <> IO.iodata_to_binary(data)}}
      {:error, _} -> :closed
    end
  end

  defp send_message(conn, type, payload) do
    packets = Tds.packets(Tds.packet_type(type), payload, conn.size)

    case conn.mode do
      :tcp -> :gen_tcp.send(conn.sock, packets)
      :ssl -> :ssl.send(conn.sock, packets)
    end

    conn
  end

  # ------------------------------------------------------------------- TLS

  # The configured certificate, or - as SQL Server does when none is set - a
  # self-signed one made once per node.
  defp tls_options do
    case Kurwa.Config.get(:mssql_tls) do
      options when is_list(options) and options != [] -> options
      _ -> self_signed()
    end
  end

  defp self_signed do
    case :persistent_term.get({__MODULE__, :certificate}, nil) do
      nil ->
        options = make_certificate()
        :persistent_term.put({__MODULE__, :certificate}, options)
        options

      options ->
        options
    end
  end

  # Built by hand rather than with :public_key.pkix_test_data, whose RSA key
  # identifier leaves out the NULL parameters - which Go's x509, and so
  # go-sqlcmd and go-mssqldb, refuse to parse. The public key's algorithm
  # carries the NULL; the signature's may omit it, and OTP's encoder wants it so.
  @rsa_encryption {1, 2, 840, 113_549, 1, 1, 1}
  @sha256_with_rsa {1, 2, 840, 113_549, 1, 1, 11}

  defp make_certificate do
    key = :public_key.generate_key({:rsa, 2048, 65_537})
    {:RSAPrivateKey, _, modulus, exponent, _, _, _, _, _, _, _} = key
    name = {:rdnSequence, [[{:AttributeTypeAndValue, {2, 5, 4, 3}, {:utf8String, "kurwadb"}}]]}
    now = DateTime.utc_now()

    time = fn dt ->
      {:utcTime, dt |> Calendar.strftime("%y%m%d%H%M%SZ") |> String.to_charlist()}
    end

    public_key = {:RSAPublicKey, modulus, exponent}

    # 825 days is the longest a TLS certificate may live for macOS and browsers.
    tbs =
      {:OTPTBSCertificate, :v3, :crypto.strong_rand_bytes(8) |> :binary.decode_unsigned(),
       {:SignatureAlgorithm, @sha256_with_rsa, :asn1_NOVALUE}, name,
       {:Validity, time.(DateTime.add(now, -86_400)), time.(DateTime.add(now, 824 * 86_400))},
       name,
       {:OTPSubjectPublicKeyInfo, {:PublicKeyAlgorithm, @rsa_encryption, :NULL}, public_key},
       :asn1_NOVALUE, :asn1_NOVALUE, extensions()}

    [
      cert: :public_key.pkix_sign(tbs, key),
      key: {:RSAPrivateKey, :public_key.der_encode(:RSAPrivateKey, key)}
    ]
  end

  # What a TLS client checks a server certificate for: not a CA, for server
  # authentication, and the names it answers to.
  defp extensions do
    host = node() |> Atom.to_string() |> String.split("@") |> List.last() |> String.to_charlist()

    [
      {:Extension, {2, 5, 29, 19}, true, {:BasicConstraints, false, :asn1_NOVALUE}},
      {:Extension, {2, 5, 29, 15}, true, [:digitalSignature, :keyEncipherment]},
      {:Extension, {2, 5, 29, 37}, false, [{1, 3, 6, 1, 5, 5, 7, 3, 1}]},
      {:Extension, {2, 5, 29, 17}, false,
       Enum.uniq([{:dNSName, ~c"localhost"}, {:dNSName, host}, {:iPAddress, <<127, 0, 0, 1>>}])}
    ]
  end
end
