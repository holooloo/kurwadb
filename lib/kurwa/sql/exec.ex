defmodule Kurwa.Sql.Exec do
  @moduledoc """
  Runs a statement from `Kurwa.Sql.Parser` against the store.

  The result is protocol-neutral - columns as `{name, type}`, rows as Elixir
  values, a command tag - and the frontend encodes it for its wire:

      {:rows, columns, rows, tag}
      {:command, tag}
      {:set, name, value, tag}        a session setting changed
      {:deallocate, name | :all, tag}
      {:catalog, sql}                 for the frontend's catalog to answer
      :empty
      {:error, sqlstate, message}

  `notices` in the session collect warnings to send before the result.

  There are no transactions. Every statement takes effect when it runs;
  `BEGIN` and `COMMIT` are accepted because drivers send them unasked, and
  `ROLLBACK` says, as a warning, that there was nothing to roll back. A
  `DELETE` reports how many of the keys were members when it looked, which
  is what a client checking "did I consume it" needs - but the look and the
  delete are two operations, not one.
  """

  alias Kurwa.Coordinator
  alias Kurwa.Key
  alias Kurwa.Namespace
  alias Kurwa.Record

  require Logger

  @version Mix.Project.config()[:version]

  @doc "The value `SELECT version()` answers with."
  def version, do: "PostgreSQL 16.0 (kurwadb #{@version}), a distributed set that stores keys"

  @doc "Runs `statement` with bound `params`."
  def run(statement, params, session) do
    execute(statement, params, session)
  catch
    {:sql_error, code, message} ->
      {:error, code, message}

    # A bug here must cost the statement, not the connection.
    kind, reason ->
      Logger.error(
        "kurwadb sql: #{inspect(statement)} failed: " <>
          Exception.format(kind, reason, __STACKTRACE__)
      )

      {:error, "XX000", "internal error: #{Exception.format_banner(kind, reason)}"}
  end

  defp execute(:empty, _params, _session), do: :empty
  defp execute({:catalog, sql}, _params, _session), do: {:catalog, sql}

  defp execute({:select, %{from: nil} = select}, params, session) do
    if select.where, do: fail("42601", "WHERE without FROM")

    row = Enum.map(select.items, fn {expr, _} -> eval(expr, params, session) end)
    columns = columns(select, params)
    {:rows, columns, [row], "SELECT 1"}
  end

  defp execute({:select, select}, params, session) do
    rows = select_rows(select, params, session)
    {:rows, columns(select, params), rows, "SELECT #{length(rows)}"}
  end

  defp execute({:insert, set, columns, rows, returning}, params, session) do
    set = store_set(set)

    entries =
      Enum.map(rows, fn row ->
        values = Enum.map(row, &eval(&1, params, session))
        fields = Enum.zip(columns || Enum.take(["key", "ttl"], length(values)), values)
        key = fields |> List.keyfind("key", 0) |> elem(1) |> key!()
        ttl = fields |> List.keyfind("ttl", 0, {"ttl", nil}) |> elem(1) |> ttl!()
        {key, ttl}
      end)

    entries
    |> concurrently(fn {key, ttl} -> add(set, key, if(ttl, do: [ttl: ttl * 1000], else: [])) end)
    |> Enum.each(&ok!/1)

    tag = "INSERT 0 #{length(entries)}"
    returning(returning, Enum.map(entries, &elem(&1, 0)), set, params, session, tag)
  end

  defp execute({:delete, set, {:keys, exprs}, returning}, params, session) do
    set = store_set(set)
    keys = exprs |> Enum.map(&(&1 |> eval(params, session) |> key!())) |> Enum.uniq()

    # Read first, so the tag counts what was there: "DELETE 0" from a key that
    # was never added is how a client knows it did not consume anything.
    present =
      keys
      |> concurrently(fn key -> {key, member?(set, key)} end)
      |> Enum.flat_map(fn {key, answer} -> if ok!(answer), do: [key], else: [] end)

    present |> concurrently(&delete(set, &1)) |> Enum.each(&ok!/1)

    returning(returning, present, set, params, session, "DELETE #{length(present)}")
  end

  defp execute({:create_table, set, if_not_exists}, _params, _session) do
    set = store_set(set)

    cond do
      set == nil ->
        unless if_not_exists, do: fail("42P07", "relation \"kurwa\" already exists")

      not if_not_exists and set in known_sets() ->
        fail("42P07", "relation \"#{set}\" already exists")

      true ->
        Kurwa.Registry.register(set)
    end

    {:command, "CREATE TABLE"}
  end

  defp execute({:utility, :rollback, tag}, _params, _session) do
    notice("kurwadb has no transactions: every statement took effect when it ran")
    {:command, tag}
  end

  defp execute({:utility, _kind, tag}, _params, _session), do: {:command, tag}

  defp execute({:set, {name, value}}, _params, _session),
    do: {:set, String.downcase(name), unquote_value(value), "SET"}

  defp execute({:show, name}, _params, session) do
    value = setting(session, String.downcase(name))
    {:rows, [{name, :text}], [[value]], "SHOW"}
  end

  defp execute({:deallocate, name}, _params, _session) do
    {:deallocate, name, if(name == :all, do: "DEALLOCATE ALL", else: "DEALLOCATE")}
  end

  # ------------------------------------------------------------------ select

  defp select_rows(%{where: nil, from: set}, _params, _session) do
    fail(
      "0A000",
      "SELECT from #{table_name(set)} without WHERE key = ... would be a scan, and there are " <>
        "none: ask for the keys you want"
    )
  end

  defp select_rows(%{where: {:keys, exprs}, from: set} = select, params, session) do
    set = store_set(set)
    keys = exprs |> Enum.map(&(&1 |> eval(params, session) |> key!())) |> Enum.uniq()
    keys = limit(keys, select.limit, params, session)
    wants_ttl? = Enum.any?(select.items, &match?({{:col, "ttl"}, _}, &1))

    members =
      if wants_ttl? do
        keys
        |> concurrently(fn key -> {key, lookup(set, key)} end)
        |> Enum.flat_map(fn {key, answer} ->
          record = ok!(answer)
          if Record.member?(record), do: [{key, record}], else: []
        end)
      else
        keys
        |> concurrently(fn key -> {key, member?(set, key)} end)
        |> Enum.flat_map(fn {key, answer} -> if ok!(answer), do: [{key, nil}], else: [] end)
      end

    case aggregate(select.items) do
      :count ->
        [Enum.map(select.items, fn _ -> length(members) end)]

      :none ->
        Enum.map(members, fn {key, record} ->
          Enum.map(select.items, fn {expr, _} -> row_value(expr, key, record, params, session) end)
        end)
    end
  end

  defp aggregate(items) do
    counts = Enum.count(items, &match?({:count_star, _}, &1))

    cond do
      counts == 0 -> :none
      counts == length(items) -> :count
      true -> fail("42803", "count(*) cannot be mixed with columns: there is no GROUP BY")
    end
  end

  defp row_value(:star, key, _record, _params, _session), do: key
  defp row_value({:col, "key"}, key, _record, _params, _session), do: key
  defp row_value({:col, "ttl"}, _key, record, _params, _session), do: ttl_seconds(record)

  defp row_value({:col, other}, _key, _record, _params, _session),
    do: fail("42703", "column \"#{other}\" does not exist: a set has key and ttl")

  defp row_value(expr, _key, _record, params, session), do: eval(expr, params, session)

  defp limit(keys, nil, _params, _session), do: keys

  defp limit(keys, expr, params, session) do
    case expr |> eval(params, session) |> integer() do
      n when n >= 0 -> Enum.take(keys, n)
      _ -> fail("2201W", "LIMIT must not be negative")
    end
  end

  defp returning(nil, _keys, _set, _params, _session, tag), do: {:command, tag}

  defp returning(items, keys, _set, params, session, tag) do
    rows =
      Enum.map(keys, fn key ->
        Enum.map(items, fn {expr, _} -> row_value(expr, key, nil, params, session) end)
      end)

    columns = Enum.map(items, fn {expr, name} -> {name, type(expr, params)} end)
    {:rows, columns, rows, tag}
  end

  # -------------------------------------------------------------- expressions

  defp eval({:lit, value}, _params, _session), do: value

  defp eval({:param, n}, params, _session) do
    case Enum.fetch(params, n - 1) do
      {:ok, value} -> value
      :error -> fail("08P01", "there is no parameter $#{n}")
    end
  end

  defp eval({:cast, expr, type}, params, session), do: cast(eval(expr, params, session), type)

  defp eval({:exists, select}, params, session),
    do: select_rows(select, params, session) != []

  defp eval({:col, name}, _params, _session),
    do: fail("42703", "column \"#{name}\" does not exist")

  defp eval(:count_star, _params, _session), do: fail("42803", "count(*) needs a FROM")
  defp eval(:star, _params, _session), do: fail("42601", "SELECT * needs a FROM")

  defp eval({:call, name, args}, params, session),
    do: call(name, Enum.map(args, &eval(&1, params, session)), session)

  defp call("kurwa_add", [set, key], _s), do: ok!(add(set(set), key!(key), [])) && true

  defp call("kurwa_add", [set, key, ttl], _s) do
    opts = if ttl!(ttl), do: [ttl: ttl!(ttl) * 1000], else: []
    ok!(add(set(set), key!(key), opts)) && true
  end

  defp call("kurwa_member", [set, key], _s), do: ok!(member?(set(set), key!(key)))
  defp call("kurwa_delete", [set, key], _s), do: ok!(delete(set(set), key!(key))) && true

  defp call("kurwa_ttl", [set, key], _s) do
    record = ok!(lookup(set(set), key!(key)))
    if Record.member?(record), do: ttl_seconds(record) || -1, else: nil
  end

  defp call("kurwa_count", [], _s), do: ok!(Kurwa.count()).approximate

  defp call("kurwa_forget", [set], _s) do
    case set(set) do
      nil -> fail("22023", "the default set cannot be forgotten")
      name -> ok!(Namespace.forget(name)) && true
    end
  end

  defp call("version", [], _s), do: version()
  defp call("current_database", [], s), do: Map.get(s, :database, "kurwadb")
  defp call("current_catalog", [], s), do: Map.get(s, :database, "kurwadb")
  defp call("current_schema", [], _s), do: "public"
  defp call(user, [], s) when user in ~w(current_user session_user user), do: s.user
  defp call("pg_backend_pid", [], s), do: s.pid
  defp call("current_setting", [name | _], s), do: setting(s, String.downcase(name))
  defp call("now", [], _s), do: DateTime.utc_now() |> DateTime.to_iso8601()

  defp call(name, args, _s) when name in ~w(kurwa_add kurwa_member kurwa_delete kurwa_ttl),
    do:
      fail(
        "42883",
        "#{name} takes (set, key#{if name == "kurwa_add", do: "[, ttl]"}), got #{length(args)} arguments"
      )

  defp call(name, _args, _s), do: fail("42883", "function #{name}() does not exist")

  defp cast(nil, _type), do: nil

  defp cast(value, type) when type in ~w(int int2 int4 int8 integer bigint smallint),
    do: integer(value)

  defp cast(value, type) when type in ~w(bool boolean), do: value in [true, "t", "true", "1", 1]
  defp cast(value, _type) when is_binary(value), do: value
  defp cast(value, _type) when is_integer(value) or is_float(value), do: to_string(value)
  defp cast(value, _type), do: value

  defp integer(n) when is_integer(n), do: n

  defp integer(s) when is_binary(s) do
    case Integer.parse(s) do
      {n, ""} -> n
      _ -> fail("22P02", "invalid input syntax for type bigint: \"#{s}\"")
    end
  end

  defp integer(other), do: fail("22P02", "expected an integer, got #{inspect(other)}")

  defp key!(nil), do: fail("23502", "a key cannot be NULL")
  defp key!(key) when is_binary(key) and key != "", do: key
  defp key!(""), do: fail("22023", "a key cannot be empty")
  defp key!(n) when is_integer(n) or is_float(n), do: to_string(n)
  defp key!(other), do: fail("22023", "a key is text, got #{inspect(other)}")

  defp ttl!(nil), do: nil

  defp ttl!(value) do
    case integer(value) do
      n when n > 0 -> n
      _ -> fail("22023", "ttl is a positive number of seconds")
    end
  end

  defp ttl_seconds(nil), do: nil

  defp ttl_seconds(record) do
    case Record.ttl(record) do
      :never -> nil
      ms -> div(ms + 999, 1000)
    end
  end

  # The set argument of the kurwa_* functions: NULL, '' and 'kurwa' all name
  # the default set, as the table name does.
  defp set(nil), do: nil
  defp set(""), do: nil
  defp set("kurwa"), do: nil

  defp set(name) when is_binary(name) do
    if Kurwa.Key.valid_name?(name),
      do: name,
      else: fail("42602", "\"#{name}\" is not a valid set name")
  end

  defp set(other), do: fail("22023", "a set name is text, got #{inspect(other)}")

  # ----------------------------------------------------------------- describe

  @doc """
  What a statement takes and returns, before it runs: `{param_types, columns}`
  where `columns` is nil for a statement that returns no rows. This is what the
  extended protocol's Describe needs.
  """
  def describe(statement) do
    params = param_types(statement)

    columns =
      case statement do
        {:select, select} -> columns(select, params)
        {:insert, _, _, _, items} when items != nil -> item_columns(items, params)
        {:delete, _, _, items} when items != nil -> item_columns(items, params)
        {:show, name} -> [{name, :text}]
        _ -> nil
      end

    {params, columns}
  catch
    {:sql_error, _code, _message} ->
      {param_types(statement), nil}

    kind, reason ->
      Logger.error(
        "kurwadb sql: describing #{inspect(statement)}: " <>
          Exception.format(kind, reason, __STACKTRACE__)
      )

      {[], nil}
  end

  defp columns(%{items: items}, params), do: item_columns(items, params)

  defp item_columns(items, params),
    do: Enum.map(items, fn {expr, name} -> {name, type(expr, params)} end)

  defp type(:star, _), do: :text
  defp type({:col, "ttl"}, _), do: :int8
  defp type({:col, _}, _), do: :text
  defp type(:count_star, _), do: :int8
  defp type({:exists, _}, _), do: :bool
  defp type({:lit, n}, _) when is_integer(n), do: :int4
  defp type({:lit, b}, _) when is_boolean(b), do: :bool
  defp type({:lit, _}, _), do: :text
  defp type({:param, _}, _), do: :text

  defp type({:cast, _, t}, _) when t in ~w(int8 bigint), do: :int8
  defp type({:cast, _, t}, _) when t in ~w(int int4 integer), do: :int4
  defp type({:cast, _, t}, _) when t in ~w(int2 smallint), do: :int2
  defp type({:cast, _, t}, _) when t in ~w(bool boolean), do: :bool
  defp type({:cast, _, _}, _), do: :text

  defp type({:call, name, _}, _)
       when name in ~w(kurwa_add kurwa_member kurwa_delete kurwa_forget),
       do: :bool

  defp type({:call, name, _}, _) when name in ~w(kurwa_ttl kurwa_count), do: :int8
  defp type({:call, "pg_backend_pid", _}, _), do: :int4
  defp type({:call, _, _}, _), do: :text

  # A parameter's type comes from where it sits: in a key position it is text,
  # as a ttl or a LIMIT it is an integer. Anything unplaced is text.
  defp param_types(statement) do
    placed = statement |> placements(%{}) |> Map.new()

    case Map.keys(placed) do
      [] -> []
      indexes -> Enum.map(1..Enum.max(indexes), &Map.get(placed, &1, :text))
    end
  end

  defp placements({:select, select}, acc) do
    acc = Enum.reduce(select.items, acc, fn {expr, _}, acc -> place(expr, :text, acc) end)
    acc = keys(select.where, acc)
    if select.limit, do: place(select.limit, :int8, acc), else: acc
  end

  defp placements({:insert, _set, columns, rows, _ret}, acc) do
    Enum.reduce(rows, acc, fn row, acc ->
      row
      |> Enum.zip(columns || ["key", "ttl"])
      |> Enum.reduce(acc, fn {expr, col}, acc ->
        place(expr, if(col == "ttl", do: :int8, else: :text), acc)
      end)
    end)
  end

  defp placements({:delete, _set, where, _ret}, acc), do: keys(where, acc)
  defp placements(_statement, acc), do: acc

  defp keys({:keys, exprs}, acc), do: Enum.reduce(exprs, acc, &place(&1, :text, &2))
  defp keys(nil, acc), do: acc

  defp place({:param, n}, type, acc), do: Map.put_new(acc, n, type)
  defp place({:cast, expr, t}, _type, acc), do: place(expr, type({:cast, nil, t}, nil), acc)
  defp place({:exists, select}, _type, acc), do: placements({:select, select}, acc)

  defp place({:call, "kurwa_add", [set, key, ttl]}, _type, acc),
    do:
      acc
      |> then(&place(set, :text, &1))
      |> then(&place(key, :text, &1))
      |> then(&place(ttl, :int8, &1))

  defp place({:call, _name, args}, _type, acc), do: Enum.reduce(args, acc, &place(&1, :text, &2))
  defp place(_expr, _type, acc), do: acc

  # ------------------------------------------------------------------- store

  # The table kurwa is the default set, which the store calls nil.
  defp store_set(:default), do: nil
  defp store_set(set), do: set

  defp add(nil, key, opts), do: Kurwa.add(key, opts)
  defp add(set, key, opts), do: Namespace.add(set, key, opts)

  defp member?(nil, key), do: Kurwa.fetch(key)
  defp member?(set, key), do: Namespace.member?(set, key)

  defp delete(nil, key), do: Kurwa.delete(key)
  defp delete(set, key), do: Namespace.delete(set, key)

  defp lookup(set, key), do: Coordinator.lookup(Key.encode(set, key))

  defp known_sets do
    case Namespace.list() do
      {:ok, %{sets: sets}} -> sets
      _ -> []
    end
  end

  # Several keys in one statement are independent quorum calls, so they run
  # side by side; one key runs in the caller.
  defp concurrently([one], fun), do: [fun.(one)]

  defp concurrently(items, fun) do
    items
    |> Task.async_stream(fun, max_concurrency: 64, ordered: true, timeout: :infinity)
    |> Enum.map(fn {:ok, result} -> result end)
  end

  defp ok!(:ok), do: true
  defp ok!({:ok, value}), do: value
  defp ok!({:error, reason}), do: unavailable(reason)

  defp unavailable(reason),
    do: fail("57P03", "the cluster could not answer: #{inspect(reason)}")

  defp setting(session, name) do
    Map.get(session.settings, name) ||
      Map.get(defaults(session), name) ||
      fail("42704", "unrecognized configuration parameter \"#{name}\"")
  end

  defp defaults(session) do
    %{
      "server_version" => "16.0",
      "server_version_num" => "160000",
      "server_encoding" => "UTF8",
      "client_encoding" => "UTF8",
      "datestyle" => "ISO, MDY",
      "timezone" => "UTC",
      "transaction isolation level" => "read committed",
      "transaction_isolation" => "read committed",
      "standard_conforming_strings" => "on",
      "integer_datetimes" => "on",
      "search_path" => "public",
      "application_name" => Map.get(session.settings, "application_name", ""),
      "max_identifier_length" => "63",
      "is_superuser" => "off"
    }
  end

  defp unquote_value(value), do: String.trim(value, "'")

  defp table_name(:default), do: "kurwa"
  defp table_name(set), do: set

  defp notice(message), do: Process.put(:kurwa_sql_notices, [message | notices()])

  @doc "Takes the warnings the last statement raised."
  def take_notices do
    notices = notices()
    Process.delete(:kurwa_sql_notices)
    Enum.reverse(notices)
  end

  defp notices, do: Process.get(:kurwa_sql_notices, [])

  defp fail(code, message), do: throw({:sql_error, code, message})
end
