defmodule Kurwa.Mongo.Commands do
  @moduledoc """
  The MongoDB commands kurwadb answers, on its own data model.

  A collection is a set and a document is `{_id: key}`. In the database
  `kurwadb` a collection is the set of the same name - the one SQL calls a
  table and Redis a set - and `kurwa` is the default set; in any other
  database it is the set `db.collection`. A string `_id` is the key itself, so
  the same members are visible from every protocol. Any other `_id` - an
  ObjectId, a number, a document - is stored under a key made from its BSON
  bytes. It never has to be turned back into a value, because nothing here
  lists keys: every answer is about an `_id` the client named.

  What a collection can be asked follows from that:

      find({_id: x}), find({_id: {$in: [...]}})   membership, one cursor batch
      insertOne / insertMany                       Kurwa.add_new: a second insert of
                                                   an _id is E11000, as in MongoDB
      deleteOne / deleteMany by _id                n counts what was there
      updateOne({_id: x}, {$setOnInsert: {}},     the dedup idiom: upserted or matched
                {upsert: true})
      countDocuments({_id: ...})                   the members among those
      listCollections, listIndexes, create         from the set registry

  A filter on anything but `_id` - an empty one included - would be a scan and
  is refused, as is a document with fields: there is nowhere to keep them. The
  one field accepted is `expireAt`, a date, which becomes the key's TTL.

  A node answers `hello` as a `mongos` does (`msg: "isdbgrid"`), so drivers
  given several kurwadb nodes treat each as a router: any of them takes any
  request, and a driver fails over between them. That is the literal truth of
  a leaderless store, and it needs no pretend primary.

  There are no transactions. `commitTransaction` succeeds, because the writes
  already did; `abortTransaction` fails and says why, since MongoDB has no
  warnings to carry that and silence would claim a rollback that did not
  happen.
  """

  alias Kurwa.Key
  alias Kurwa.Mongo.Bson
  alias Kurwa.Namespace
  alias Kurwa.Pg.Auth

  @version Mix.Project.config()[:version]
  @database "kurwadb"
  @default "kurwa"
  @wire_version 21

  @open ~w(hello isMaster ismaster saslStart saslContinue ping buildInfo buildinfo endSessions logout)

  def session(connection_id), do: %{id: connection_id, authed: false, scram: nil}

  @doc "Runs one command document. Returns `{reply_doc, session}`."
  def run(command, session) do
    name = Bson.name(command)
    db = Bson.get(command, "$db", "admin")

    cond do
      name == nil ->
        {error(2, "BadValue", "an empty command"), session}

      Kurwa.Config.auth_token() != nil and not session.authed and name not in @open ->
        {error(13, "Unauthorized", "Command #{name} requires authentication"), session}

      true ->
        dispatch(name, command, db, session)
    end
  catch
    {:mongo_error, code, code_name, message} -> {error(code, code_name, message), session}
  end

  # -------------------------------------------------------------- handshake

  defp dispatch(hello, command, _db, s) when hello in ~w(hello isMaster ismaster) do
    primary = if hello == "hello", do: "isWritablePrimary", else: "ismaster"

    mechs =
      case Bson.get(command, "saslSupportedMechs") do
        nil -> []
        _ -> [{"saslSupportedMechs", ["SCRAM-SHA-256"]}]
      end

    {ok(
       [
         {"helloOk", true},
         {primary, true},
         {"msg", "isdbgrid"},
         {"maxBsonObjectSize", 16_777_216},
         {"maxMessageSizeBytes", Kurwa.Mongo.Wire.max_message()},
         {"maxWriteBatchSize", 100_000},
         {"localTime", {:datetime, System.system_time(:millisecond)}},
         {"logicalSessionTimeoutMinutes", 30},
         {"connectionId", s.id},
         {"minWireVersion", 0},
         {"maxWireVersion", @wire_version},
         {"readOnly", false}
       ] ++ mechs
     ), s}
  end

  defp dispatch("ping", _c, _db, s), do: {ok([]), s}

  defp dispatch(build, _c, _db, s) when build in ~w(buildInfo buildinfo) do
    {ok([
       {"version", "7.0.0"},
       {"gitVersion", "kurwadb-#{@version}"},
       {"versionArray", [7, 0, 0, 0]},
       {"bits", 64},
       {"maxBsonObjectSize", 16_777_216},
       {"kurwadb", @version},
       {"modules", []}
     ]), s}
  end

  defp dispatch("getParameter", command, _db, s) do
    if Bson.get(command, "featureCompatibilityVersion") != nil,
      do: {ok([{"featureCompatibilityVersion", {:doc, [{"version", "7.0"}]}}]), s},
      else: {ok([]), s}
  end

  defp dispatch("getCmdLineOpts", _c, _db, s), do: {ok([{"argv", []}, {"parsed", {:doc, []}}]), s}
  defp dispatch("getLog", _c, _db, s), do: {ok([{"totalLinesWritten", 0}, {"log", []}]), s}
  defp dispatch("whatsmyuri", _c, _db, s), do: {ok([{"you", "client"}]), s}
  defp dispatch("logout", _c, _db, s), do: {ok([]), %{s | authed: false}}

  defp dispatch(sessions, _c, _db, s)
       when sessions in ~w(endSessions refreshSessions startSession), do: {ok([]), s}

  defp dispatch("connectionStatus", _c, _db, s) do
    users = if s.authed, do: [{:doc, [{"user", "kurwadb"}, {"db", "admin"}]}], else: []

    {ok([{"authInfo", {:doc, [{"authenticatedUsers", users}, {"authenticatedUserRoles", []}]}}]),
     s}
  end

  defp dispatch("hostInfo", _c, _db, s) do
    {ok([
       {"system",
        {:doc,
         [
           {"hostname", to_string(node())},
           {"cpuArch", to_string(:erlang.system_info(:system_architecture))}
         ]}},
       {"os", {:doc, []}},
       {"extra", {:doc, []}}
     ]), s}
  end

  defp dispatch("serverStatus", _c, _db, s) do
    {ok([
       {"host", to_string(node())},
       {"version", "7.0.0"},
       {"process", "mongos"},
       {"uptime", div(:erlang.statistics(:wall_clock) |> elem(0), 1000) * 1.0},
       {"localTime", {:datetime, System.system_time(:millisecond)}}
     ]), s}
  end

  # ------------------------------------------------------------------- auth

  defp dispatch("saslStart", command, _db, s) do
    with "SCRAM-SHA-256" <- Bson.get(command, "mechanism"),
         {:binary, _, client_first} <- Bson.get(command, "payload"),
         {:ok, server_first, exchange} <-
           Auth.scram_first("SCRAM-SHA-256", client_first, token(), nil) do
      skip? = command |> Bson.get("options", {:doc, []}) |> Bson.get("skipEmptyExchange", false)
      reply = [{"conversationId", 1}, {"done", false}, {"payload", {:binary, 0, server_first}}]
      {ok(reply), %{s | scram: %{exchange: exchange, skip_empty: skip?, final: nil}}}
    else
      mechanism when is_binary(mechanism) ->
        fail(
          2,
          "BadValue",
          "authentication mechanism #{mechanism} is not supported: use SCRAM-SHA-256"
        )

      _ ->
        fail(18, "AuthenticationFailed", "Authentication failed.")
    end
  end

  defp dispatch("saslContinue", command, _db, %{scram: %{final: nil} = scram} = s) do
    {:binary, _, client_final} = Bson.get(command, "payload", {:binary, 0, ""})

    case Auth.scram_final(client_final, scram.exchange) do
      {:ok, server_final} ->
        done? = scram.skip_empty
        reply = [{"conversationId", 1}, {"done", done?}, {"payload", {:binary, 0, server_final}}]

        s =
          if done?,
            do: %{s | authed: true, scram: nil},
            else: %{s | scram: %{scram | final: server_final}}

        {ok(reply), s}

      {:error, _} ->
        {error(18, "AuthenticationFailed", "Authentication failed."), %{s | scram: nil}}
    end
  end

  # The empty last step, for drivers that do not skip it.
  defp dispatch("saslContinue", _command, _db, %{scram: %{final: _}} = s) do
    {ok([{"conversationId", 1}, {"done", true}, {"payload", {:binary, 0, ""}}]),
     %{s | authed: true, scram: nil}}
  end

  defp dispatch("saslContinue", _command, _db, s),
    do: {error(18, "AuthenticationFailed", "No SASL session state found"), s}

  # -------------------------------------------------------------- catalogue

  defp dispatch("listDatabases", _c, _db, s) do
    names = [@database | sets() |> Enum.flat_map(&database_of/1)] |> Enum.uniq() |> Enum.sort()
    dbs = for name <- names, do: {:doc, [{"name", name}, {"sizeOnDisk", 0}, {"empty", false}]}
    {ok([{"databases", dbs}, {"totalSize", 0}]), s}
  end

  defp dispatch("listCollections", command, db, s) do
    wanted = command |> Bson.get("filter", {:doc, []}) |> Bson.get("name")

    collections =
      db
      |> collections()
      |> Enum.filter(&(wanted == nil or &1 == wanted))
      |> Enum.map(fn name ->
        {:doc,
         [
           {"name", name},
           {"type", "collection"},
           {"options", {:doc, []}},
           {"info", {:doc, [{"readOnly", false}]}},
           {"idIndex", id_index()}
         ]}
      end)

    {cursor(db, "$cmd.listCollections", collections), s}
  end

  defp dispatch("listIndexes", command, db, s) do
    {cursor(db, Bson.get(command, "listIndexes"), [id_index()]), s}
  end

  defp dispatch("create", command, db, s) do
    case set!(db, Bson.get(command, "create")) do
      nil -> :ok
      set -> Kurwa.Registry.register(set)
    end

    {ok([]), s}
  end

  defp dispatch("createIndexes", command, _db, s) do
    only_id? =
      command
      |> Bson.get("indexes", [])
      |> Enum.all?(fn index -> Bson.get(index, "key") == {:doc, [{"_id", 1}]} end)

    if only_id?,
      do:
        {ok([
           {"note", "all indexes already exist"},
           {"numIndexesBefore", 1},
           {"numIndexesAfter", 1}
         ]), s},
      else: fail(2, "BadValue", "the only index is _id: a key has no other fields to index")
  end

  defp dispatch(drop, _c, _db, _s) when drop in ~w(drop dropDatabase dropIndexes),
    do:
      fail(
        20,
        "IllegalOperation",
        "#{drop} would have to find every key to delete it, and kurwadb has no scans"
      )

  # ------------------------------------------------------------------ reads

  defp dispatch("find", command, db, s) do
    coll = Bson.get(command, "find")
    set = set!(db, coll)
    ids = ids!(Bson.get(command, "filter", {:doc, []}))

    present = Enum.filter(ids, &member?(set, &1))
    present = present |> Enum.drop(skip(command)) |> limit(command)

    projection = Bson.get(command, "projection", {:doc, []})
    hide_id? = Bson.get(projection, "_id") in [0, false, 0.0]
    docs = for id <- present, do: {:doc, if(hide_id?, do: [], else: [{"_id", id}])}

    {cursor(db, coll, docs), s}
  end

  defp dispatch("count", command, db, s) do
    set = set!(db, Bson.get(command, "count"))

    case Bson.get(command, "query") do
      q when q in [nil, {:doc, []}] ->
        fail(2, "BadValue", "counting a whole collection is a scan; count the _ids you want")

      query ->
        n = query |> ids!() |> Enum.count(&member?(set, &1))
        {ok([{"n", n}]), s}
    end
  end

  # countDocuments is an aggregation: $match, then $group counting into n.
  defp dispatch("aggregate", command, db, s) do
    coll = Bson.get(command, "aggregate")
    set = set!(db, coll)

    case Bson.get(command, "pipeline", []) do
      [{:doc, [{"$match", filter}]} | stages] ->
        present = filter |> ids!() |> Enum.filter(&member?(set, &1))
        {cursor(db, coll, pipeline(stages, present)), s}

      _ ->
        fail(
          2,
          "BadValue",
          "the only pipeline kurwadb runs is $match on _id, then $skip, $limit or a $group count"
        )
    end
  end

  defp dispatch("distinct", _c, _db, _s),
    do: fail(2, "BadValue", "distinct would walk the collection, and kurwadb has no scans")

  defp dispatch("getMore", _c, _db, _s),
    do:
      fail(43, "CursorNotFound", "cursor not found: every kurwadb answer fits in its first batch")

  defp dispatch("killCursors", command, _db, s) do
    ids = Bson.get(command, "cursors", [])

    {ok([
       {"cursorsKilled", []},
       {"cursorsNotFound", ids},
       {"cursorsAlive", []},
       {"cursorsUnknown", []}
     ]), s}
  end

  # ----------------------------------------------------------------- writes

  defp dispatch("insert", command, db, s) do
    coll = Bson.get(command, "insert")
    set = set!(db, coll)
    ordered? = Bson.truthy?(Bson.get(command, "ordered"), true)

    {n, errors} =
      command
      |> Bson.get("documents", [])
      |> Enum.with_index()
      |> write_each(ordered?, fn {doc, index} ->
        with {:ok, id, ttl} <- document(doc) do
          case add_new(set, id, ttl) do
            :ok -> :ok
            :exists -> {:error, duplicate(index, db, coll, id)}
          end
        else
          {:error, message} -> {:error, write_error(index, 2, message)}
        end
      end)

    {write_reply(n, errors, []), s}
  end

  defp dispatch("delete", command, db, s) do
    set = set!(db, Bson.get(command, "delete"))
    ordered? = Bson.truthy?(Bson.get(command, "ordered"), true)

    {n, errors} =
      command
      |> Bson.get("deletes", [])
      |> Enum.with_index()
      |> write_each(ordered?, fn {delete, index} ->
        case ids(Bson.get(delete, "q", {:doc, []})) do
          {:ok, ids} ->
            present = Enum.filter(ids, &member?(set, &1))

            present =
              if Bson.int(Bson.get(delete, "limit", 0)) == 1,
                do: Enum.take(present, 1),
                else: present

            Enum.each(present, &ok!(remove(set, &1)))
            {:ok, length(present)}

          {:error, message} ->
            {:error, write_error(index, 2, message)}
        end
      end)

    {write_reply(n, errors, []), s}
  end

  # Only the shape that makes sense for a key: match an _id, set nothing, and
  # optionally insert it if absent - MongoDB's usual way to say "add if new".
  defp dispatch("update", command, db, s) do
    set = set!(db, Bson.get(command, "update"))
    ordered? = Bson.truthy?(Bson.get(command, "ordered"), true)

    results =
      command
      |> Bson.get("updates", [])
      |> Enum.with_index()
      |> Enum.reduce_while({0, [], []}, fn {update, index}, {n, errors, upserted} ->
        outcome =
          with {:ok, [id]} <- ids(Bson.get(update, "q", {:doc, []})) |> one_id(),
               :ok <- no_fields(Bson.get(update, "u", {:doc, []})) do
            cond do
              Bson.truthy?(Bson.get(update, "upsert"), false) ->
                case add_new(set, id, nil) do
                  :ok -> {:upserted, id}
                  :exists -> :matched
                end

              member?(set, id) ->
                :matched

              true ->
                :none
            end
          end

        case outcome do
          {:upserted, id} ->
            {:cont, {n + 1, errors, [{:doc, [{"index", index}, {"_id", id}]} | upserted]}}

          :matched ->
            {:cont, {n + 1, errors, upserted}}

          :none ->
            {:cont, {n, errors, upserted}}

          {:error, message} ->
            acc = {n, [write_error(index, 2, message) | errors], upserted}
            if ordered?, do: {:halt, acc}, else: {:cont, acc}
        end
      end)

    {n, errors, upserted} = results
    {write_reply(n, Enum.reverse(errors), Enum.reverse(upserted), [{"nModified", 0}]), s}
  end

  defp dispatch("findAndModify", _c, _db, _s),
    do:
      fail(
        2,
        "BadValue",
        "findAndModify returns a document's fields, and a key has none; use updateOne with upsert"
      )

  # --------------------------------------------------------- transactions

  defp dispatch("commitTransaction", _c, _db, s), do: {ok([]), s}

  defp dispatch("abortTransaction", _c, _db, _s),
    do:
      fail(
        20,
        "IllegalOperation",
        "kurwadb has no transactions: every write in this one already took effect, and none was rolled back"
      )

  defp dispatch(name, _c, _db, _s), do: fail(59, "CommandNotFound", "no such command: '#{name}'")

  # ---------------------------------------------------------------- helpers

  # The _ids a filter names: {_id: v}, {_id: {$eq: v}}, {_id: {$in: [...]}}.
  defp ids({:doc, [{"_id", {:doc, [{"$in", list}]}}]}) when is_list(list),
    do: {:ok, Enum.uniq(list)}

  defp ids({:doc, [{"_id", {:doc, [{"$eq", v}]}}]}), do: {:ok, [v]}

  defp ids({:doc, [{"_id", {:doc, [{<<?$, _::binary>> = op, _} | _]}}]}),
    do: {:error, "#{op} on _id is not supported: use a value, $eq or $in"}

  defp ids({:doc, [{"_id", v}]}), do: {:ok, [v]}

  defp ids({:doc, []}),
    do:
      {:error,
       "an empty filter would walk the collection, and kurwadb has no scans; filter on _id"}

  defp ids(_), do: {:error, "the only filter is on _id: {_id: value} or {_id: {$in: [...]}}"}

  defp ids!(filter) do
    case ids(filter) do
      {:ok, ids} -> ids
      {:error, message} -> fail(2, "BadValue", message)
    end
  end

  defp one_id({:ok, [id]}), do: {:ok, [id]}
  defp one_id({:ok, _many}), do: {:error, "an update names one _id"}
  defp one_id(error), do: error

  # An update that sets no field: {}, {$setOnInsert: {}}, {$set: {}}, or one
  # that only restates the _id.
  defp no_fields({:doc, pairs}) do
    empty? =
      Enum.all?(pairs, fn
        {op, {:doc, inner}} when op in ["$setOnInsert", "$set"] ->
          Enum.all?(inner, &match?({"_id", _}, &1))

        {"_id", _} ->
          true

        _ ->
          false
      end)

    if empty?,
      do: :ok,
      else:
        {:error,
         "a key has no fields to update: match on _id and set nothing, with upsert to add it"}
  end

  defp no_fields(_), do: {:error, "the update document is malformed"}

  defp document({:doc, pairs}) do
    case List.keyfind(pairs, "_id", 0) do
      nil ->
        {:error, "a document needs an _id"}

      {"_id", id} ->
        case Enum.reject(pairs, fn {k, _} -> k in ["_id", "expireAt"] end) do
          [] ->
            {:ok, id, ttl(Bson.get({:doc, pairs}, "expireAt"))}

          [{field, _} | _] ->
            {:error,
             "kurwadb stores keys only: a document is {_id: ...}, and \"#{field}\" has nowhere to go"}
        end
    end
  end

  defp ttl(nil), do: nil

  defp ttl({:datetime, at}) do
    case at - System.system_time(:millisecond) do
      ms when ms > 0 -> ms
      _ -> fail(2, "BadValue", "expireAt is in the past")
    end
  end

  defp ttl(_), do: fail(2, "BadValue", "expireAt must be a date")

  defp pipeline([], present), do: Enum.map(present, &{:doc, [{"_id", &1}]})

  defp pipeline([{:doc, [{"$skip", n}]} | rest], present),
    do: pipeline(rest, Enum.drop(present, Bson.int(n)))

  defp pipeline([{:doc, [{"$limit", n}]} | rest], present),
    do: pipeline(rest, Enum.take(present, Bson.int(n)))

  defp pipeline(
         [{:doc, [{"$group", {:doc, [{"_id", group}, {field, {:doc, [{"$sum", 1}]}}]}}]}],
         present
       ) do
    if present == [], do: [], else: [{:doc, [{"_id", group}, {field, length(present)}]}]
  end

  defp pipeline(_stages, _present),
    do:
      fail(
        2,
        "BadValue",
        "that pipeline stage is not supported: $match on _id, then $skip, $limit or a $group count"
      )

  defp skip(command), do: Bson.int(Bson.get(command, "skip", 0)) || 0

  defp limit(ids, command) do
    case Bson.int(Bson.get(command, "limit", 0)) do
      n when is_integer(n) and n > 0 -> Enum.take(ids, n)
      n when is_integer(n) and n < 0 -> Enum.take(ids, -n)
      _ -> ids
    end
  end

  # Writes one entry at a time; an ordered batch stops at the first error.
  defp write_each(entries, ordered?, fun) do
    Enum.reduce_while(entries, {0, []}, fn entry, {n, errors} ->
      case fun.(entry) do
        :ok ->
          {:cont, {n + 1, errors}}

        {:ok, count} ->
          {:cont, {n + count, errors}}

        {:error, error} ->
          if ordered?, do: {:halt, {n, [error | errors]}}, else: {:cont, {n, [error | errors]}}
      end
    end)
    |> then(fn {n, errors} -> {n, Enum.reverse(errors)} end)
  end

  defp write_reply(n, errors, upserted, extra \\ []) do
    ok(
      [{"n", n}] ++
        extra ++
        if(upserted == [], do: [], else: [{"upserted", upserted}]) ++
        if(errors == [], do: [], else: [{"writeErrors", errors}])
    )
  end

  defp duplicate(index, db, coll, id) do
    {:doc,
     [
       {"index", index},
       {"code", 11_000},
       {"keyPattern", {:doc, [{"_id", 1}]}},
       {"keyValue", {:doc, [{"_id", id}]}},
       {"errmsg",
        "E11000 duplicate key error collection: #{db}.#{coll} index: _id_ dup key: { _id: #{show(id)} }"}
     ]}
  end

  defp write_error(index, code, message),
    do: {:doc, [{"index", index}, {"code", code}, {"errmsg", message}]}

  defp show(id) when is_binary(id), do: inspect(id)
  defp show({:oid, oid}), do: "ObjectId('#{Base.encode16(oid, case: :lower)}')"
  defp show(id), do: inspect(id)

  defp cursor(db, coll, docs) do
    ok([{"cursor", {:doc, [{"firstBatch", docs}, {"id", {:int64, 0}}, {"ns", "#{db}.#{coll}"}]}}])
  end

  defp id_index, do: {:doc, [{"v", 2}, {"key", {:doc, [{"_id", 1}]}}, {"name", "_id_"}]}

  defp ok(pairs), do: {:doc, pairs ++ [{"ok", 1.0}]}

  defp error(code, name, message),
    do: {:doc, [{"ok", 0.0}, {"errmsg", message}, {"code", code}, {"codeName", name}]}

  defp fail(code, name, message), do: throw({:mongo_error, code, name, message})

  defp token, do: to_string(Kurwa.Config.auth_token())

  # ------------------------------------------------------------- the store

  # The database kurwadb is the sets themselves; any other is a prefix.
  defp set!(db, coll) when is_binary(coll) do
    name =
      cond do
        db in ~w(admin config local) ->
          fail(20, "IllegalOperation", "#{db} is a system database and holds no sets")

        db == @database and coll == @default ->
          nil

        db == @database ->
          coll

        true ->
          "#{db}.#{coll}"
      end

    if name == nil or Key.valid_name?(name),
      do: name,
      else:
        fail(
          73,
          "InvalidNamespace",
          "\"#{coll}\" is not a usable collection name: letters, digits and _ . : -"
        )
  end

  defp set!(_db, _coll), do: fail(2, "BadValue", "the collection name must be a string")

  defp sets do
    case Namespace.list() do
      {:ok, %{sets: sets}} -> sets
      _ -> []
    end
  end

  defp collections(@database),
    do: Enum.sort([@default | Enum.reject(sets(), &String.contains?(&1, "."))])

  defp collections(db) do
    prefix = db <> "."

    for set <- sets(),
        String.starts_with?(set, prefix),
        do: String.replace_prefix(set, prefix, "")
  end

  defp database_of(set) do
    case String.split(set, ".", parts: 2) do
      [db, _coll] -> [db]
      _ -> []
    end
  end

  # A string _id is the key, so the same member is visible from SQL and Redis.
  # Any other type is keyed by its BSON bytes, behind a NUL no string key starts with.
  defp key(id) when is_binary(id), do: id
  defp key(id), do: <<0, Bson.value_bytes(id)::binary>>

  defp member?(nil, id), do: ok!(Kurwa.fetch(key(id)))
  defp member?(set, id), do: ok!(Namespace.member?(set, key(id)))

  defp add_new(nil, id, ttl), do: answer!(Kurwa.add_new(key(id), ttl_opts(ttl)))
  defp add_new(set, id, ttl), do: answer!(Namespace.add_new(set, key(id), ttl_opts(ttl)))

  defp remove(nil, id), do: Kurwa.delete(key(id))
  defp remove(set, id), do: Namespace.delete(set, key(id))

  defp ttl_opts(nil), do: []
  defp ttl_opts(ms), do: [ttl: ms]

  defp answer!(:ok), do: :ok
  defp answer!(:exists), do: :exists

  defp answer!({:error, {:no_majority, _}}),
    do:
      fail(
        133,
        "FailedToSatisfyReadPreference",
        "not enough replicas reachable for a write that must have one winner"
      )

  defp answer!(other), do: ok!(other)

  defp ok!(:ok), do: true
  defp ok!({:ok, value}), do: value

  defp ok!({:error, reason}),
    do: fail(6, "HostUnreachable", "the cluster could not answer: #{inspect(reason)}")
end
