defmodule Kurwa.Resp.Commands do
  @moduledoc """
  The Redis commands kurwadb answers, on its own data model.

  Two families map cleanly. Redis sets are kurwadb's named sets - `SADD`,
  `SREM`, `SISMEMBER`, `SMISMEMBER` - and Redis string keys are the default set,
  where a key either exists or does not:

      SET order:1 x EX 3600 NX     add with a ttl, only if absent: OK or nil
      EXISTS order:1                1
      TTL order:1                   3600
      DEL order:1                   1, and 0 the second time

  There are no values. `SET` accepts one, because the command needs it, and
  throws it away; `GET` is an error rather than an invented value, so code that
  reads values back fails loudly instead of quietly. Anything that has to walk
  the keyspace - `KEYS`, `SCAN`, `SMEMBERS`, `SCARD`, `FLUSHDB` - is refused,
  as everywhere else in kurwadb.

  Counts are true: `SADD`, `SREM` and `DEL` look before they write, so they
  report what changed, the way Redis does. The look and the write are two
  operations, as are `SET NX`'s - two clients racing on the same absent key can
  both be told they set it. Redis gives that atomically; kurwadb, with no
  leader to serialise on, does not.

  `SET NX` and `SETNX` are the exception to "look, then write": they go through
  `Kurwa.add_new/2`, so of concurrent callers for one absent key at most one is
  told OK - what an idempotency check on Redis relies on.

  `MULTI` and `EXEC` queue commands and run them in order, which is what
  pipelining clients wrap their batches in; there is no isolation from other
  clients. `WATCH` works as clients use it: EXEC answers a null array, and
  runs nothing, if a watched key's version changed since the WATCH. The check
  and the commands are two steps, so a change landing between them is not
  seen; the retry loop that Redis clients build on WATCH works.
  """

  alias Kurwa.Coordinator
  alias Kurwa.Key
  alias Kurwa.Namespace
  alias Kurwa.Record

  @version Mix.Project.config()[:version]

  @doc "A fresh connection's state."
  def session,
    do: %{
      proto: 2,
      authed: false,
      name: nil,
      multi: nil,
      watched: %{},
      id: System.unique_integer([:positive])
    }

  @doc """
  Runs one request. Returns `{reply, session}`, or `{:close, reply, session}`
  when the connection should end after the reply.
  """
  def run([], session), do: {{:error, "ERR empty command"}, session}

  def run([name | args], session) do
    command = String.upcase(name)

    cond do
      needs_auth?(session) and command not in ~w(AUTH HELLO QUIT) ->
        {{:error, "NOAUTH Authentication required."}, session}

      session.multi != nil and command not in ~w(EXEC DISCARD MULTI WATCH UNWATCH QUIT RESET) ->
        queue(command, args, session)

      true ->
        dispatch(command, args, session)
    end
  catch
    {:resp_error, message} -> {{:error, message}, session}
  end

  defp needs_auth?(session), do: Kurwa.Config.auth_token() != nil and not session.authed

  # ---------------------------------------------------------------- connection

  defp dispatch("PING", [], s), do: {{:simple, "PONG"}, s}
  defp dispatch("PING", [message], s), do: {message, s}
  defp dispatch("ECHO", [message], s), do: {message, s}
  defp dispatch("QUIT", _args, s), do: {:close, {:simple, "OK"}, s}

  defp dispatch("HELLO", [], s), do: {hello(s), s}

  defp dispatch("HELLO", [version | rest], s) do
    case Integer.parse(version) do
      {v, ""} when v in [2, 3] ->
        s = hello_options(rest, s)
        s = %{s | proto: v}
        {hello(s), s}

      _ ->
        {{:error, "NOPROTO unsupported protocol version"}, s}
    end
  end

  defp dispatch("AUTH", [password], s), do: auth(password, s)
  defp dispatch("AUTH", [_user, password], s), do: auth(password, s)

  defp dispatch("SELECT", ["0"], s), do: {{:simple, "OK"}, s}
  defp dispatch("SELECT", [_db], s), do: {{:error, "ERR DB index is out of range"}, s}

  defp dispatch("CLIENT", [sub | args], s), do: client(String.upcase(sub), args, s)
  defp dispatch("COMMAND", [], s), do: {[], s}
  defp dispatch("COMMAND", [sub | _], s) when sub in ~w(COUNT count), do: {0, s}
  defp dispatch("COMMAND", _args, s), do: {[], s}

  defp dispatch("INFO", _args, s) do
    info = """
    # Server\r
    redis_version:7.2.0\r
    kurwadb_version:#{@version}\r
    redis_mode:standalone\r
    os:#{:erlang.system_info(:system_architecture)}\r
    # Keyspace\r
    """

    {info, s}
  end

  defp dispatch("DBSIZE", [], s), do: {ok!(Kurwa.count()).approximate, s}

  defp dispatch("TIME", [], s) do
    us = System.os_time(:microsecond)
    {[Integer.to_string(div(us, 1_000_000)), Integer.to_string(rem(us, 1_000_000))], s}
  end

  defp dispatch("RESET", [], s),
    do: {{:simple, "RESET"}, %{s | multi: nil, proto: 2, name: nil, watched: %{}}}

  # --------------------------------------------------------------- string keys

  defp dispatch("SET", [key, _value | options], s) do
    case set_options(options, nil, nil) do
      {:nx, ttl} ->
        if add_new(key, ttl), do: {{:simple, "OK"}, s}, else: {nil, s}

      {:xx, ttl} ->
        if member?(nil, key), do: ok!(add(nil, key, ttl)) && {{:simple, "OK"}, s}, else: {nil, s}

      {nil, ttl} ->
        ok!(add(nil, key, ttl)) && {{:simple, "OK"}, s}
    end
  end

  defp dispatch("SETNX", [key, _value], s), do: {if(add_new(key, nil), do: 1, else: 0), s}

  defp dispatch("SETEX", [key, seconds, _value], s),
    do: ok!(add(nil, key, positive!(seconds) * 1000)) && {{:simple, "OK"}, s}

  defp dispatch("PSETEX", [key, ms, _value], s),
    do: ok!(add(nil, key, positive!(ms))) && {{:simple, "OK"}, s}

  defp dispatch(get, [_key | _], _s) when get in ~w(GET GETEX GETDEL GETSET MGET),
    do: fail("ERR kurwadb stores keys without values; EXISTS says whether a key is there")

  defp dispatch("EXISTS", [_ | _] = keys, s),
    do: {keys |> Enum.map(&member?(nil, &1)) |> Enum.count(& &1), s}

  defp dispatch(del, [_ | _] = keys, s) when del in ~w(DEL UNLINK) do
    present = keys |> Enum.uniq() |> Enum.filter(&member?(nil, &1))
    Enum.each(present, &ok!(Kurwa.delete(&1)))
    {length(present), s}
  end

  defp dispatch("EXPIRE", [key, seconds], s), do: expire(key, positive!(seconds) * 1000, s)
  defp dispatch("PEXPIRE", [key, ms], s), do: expire(key, positive!(ms), s)

  defp dispatch("PERSIST", [key], s) do
    case lookup(nil, key) do
      nil ->
        {0, s}

      record ->
        if Record.ttl(record) == :never, do: {0, s}, else: ok!(add(nil, key, nil)) && {1, s}
    end
  end

  defp dispatch("TTL", [key], s), do: {ttl(key, 1000), s}
  defp dispatch("PTTL", [key], s), do: {ttl(key, 1), s}

  defp dispatch("TYPE", [key], s),
    do: {{:simple, if(member?(nil, key), do: "string", else: "none")}, s}

  # ---------------------------------------------------------------------- sets

  defp dispatch("SADD", [set | [_ | _] = members], s) do
    set = set!(set)
    new = members |> Enum.uniq() |> Enum.reject(&member?(set, &1))
    Enum.each(new, &ok!(add(set, &1, nil)))
    {length(new), s}
  end

  defp dispatch("SREM", [set | [_ | _] = members], s) do
    set = set!(set)
    present = members |> Enum.uniq() |> Enum.filter(&member?(set, &1))
    Enum.each(present, &ok!(Namespace.delete(set, &1)))
    {length(present), s}
  end

  defp dispatch("SISMEMBER", [set, member], s),
    do: {if(member?(set!(set), member), do: 1, else: 0), s}

  defp dispatch("SMISMEMBER", [set | [_ | _] = members], s) do
    set = set!(set)
    {Enum.map(members, &if(member?(set, &1), do: 1, else: 0)), s}
  end

  # ------------------------------------------------------------- transactions

  defp dispatch("MULTI", [], %{multi: nil} = s), do: {{:simple, "OK"}, %{s | multi: []}}
  defp dispatch("MULTI", [], s), do: {{:error, "ERR MULTI calls can not be nested"}, s}
  defp dispatch("EXEC", [], %{multi: nil} = s), do: {{:error, "ERR EXEC without MULTI"}, s}

  defp dispatch("EXEC", [], %{multi: :aborted} = s),
    do:
      {{:error, "EXECABORT Transaction discarded because of previous errors."}, %{s | multi: nil}}

  defp dispatch("EXEC", [], %{multi: queued} = s) do
    s = %{s | multi: nil}

    # WATCH: if any watched key moved since it was watched, EXEC does nothing
    # and answers with a null array, which is how clients know to retry.
    if watch_broken?(s.watched) do
      {:null_array, %{s | watched: %{}}}
    else
      queued
      |> Enum.reverse()
      |> Enum.map_reduce(%{s | watched: %{}}, fn request, s ->
        case run(request, s) do
          {:close, reply, s} -> {reply, s}
          {reply, s} -> {reply, s}
        end
      end)
    end
  end

  defp dispatch("DISCARD", [], %{multi: nil} = s), do: {{:error, "ERR DISCARD without MULTI"}, s}

  defp dispatch("DISCARD", [], s), do: {{:simple, "OK"}, %{s | multi: nil, watched: %{}}}

  defp dispatch("WATCH", _keys, %{multi: multi}) when multi != nil,
    do: fail("ERR WATCH inside MULTI is not allowed")

  # Remembers the version of each key - its Lamport stamp and origin - as the
  # cluster answers it now. A set name cannot be watched this way: its members
  # are separate keys with versions of their own, so a change to them would go
  # unseen, and a WATCH that cannot see is worse than none.
  defp dispatch("WATCH", [_ | _] = keys, s) do
    if Enum.any?(keys, &(Key.valid_name?(&1) and &1 in known_sets())),
      do:
        fail("ERR WATCH on a set is not supported: kurwadb can watch a key, not a set's members")

    watched = Enum.reduce(keys, s.watched, fn key, acc -> Map.put_new(acc, key, version(key)) end)
    {{:simple, "OK"}, %{s | watched: watched}}
  end

  defp dispatch("UNWATCH", [], s), do: {{:simple, "OK"}, %{s | watched: %{}}}

  # ---------------------------------------------------------------- refusals

  defp dispatch(command, _args, _s)
       when command in ~w(KEYS SCAN RANDOMKEY FLUSHDB FLUSHALL SMEMBERS SSCAN SCARD SRANDMEMBER SPOP
                          SINTER SUNION SDIFF SINTERSTORE SUNIONSTORE SDIFFSTORE SINTERCARD SMOVE),
       do:
         fail(
           "ERR #{command} would walk a set or the keyspace, and kurwadb has no scans; ask for the keys you want"
         )

  defp dispatch(command, args, _s) do
    if known?(command) do
      fail("ERR wrong number of arguments for '#{String.downcase(command)}' command")
    else
      preview = args |> Enum.take(3) |> Enum.map_join(" ", &"'#{&1}'")
      fail("ERR unknown command '#{command}', with args beginning with: #{preview}")
    end
  end

  @known ~w(PING ECHO QUIT HELLO AUTH SELECT CLIENT COMMAND INFO DBSIZE TIME RESET SET SETNX SETEX
            PSETEX EXISTS DEL UNLINK EXPIRE PEXPIRE PERSIST TTL PTTL TYPE SADD SREM SISMEMBER
            SMISMEMBER MULTI EXEC DISCARD WATCH UNWATCH)

  defp known?(command), do: command in @known

  # Commands inside MULTI are checked now and run at EXEC. One that cannot run
  # aborts the transaction, as in Redis.
  defp queue(_command, _args, %{multi: :aborted} = s), do: {{:simple, "QUEUED"}, s}

  defp queue(command, args, s) do
    if known?(command),
      do: {{:simple, "QUEUED"}, %{s | multi: [[command | args] | s.multi]}},
      else: {{:error, "ERR unknown command '#{command}'"}, %{s | multi: :aborted}}
  end

  # -------------------------------------------------------------------- helpers

  defp hello(s) do
    {:map,
     [
       {"server", "redis"},
       {"version", "7.2.0"},
       {"kurwadb", @version},
       {"proto", s.proto},
       {"id", s.id},
       {"mode", "standalone"},
       {"role", "master"},
       {"modules", []}
     ]}
  end

  defp hello_options([], s), do: s

  defp hello_options([opt, _user, password | rest], s) when opt in ~w(AUTH auth) do
    case auth(password, s) do
      {{:simple, "OK"}, s} -> hello_options(rest, s)
      {{:error, message}, _s} -> fail(message)
    end
  end

  defp hello_options([opt, name | rest], s) when opt in ~w(SETNAME setname),
    do: hello_options(rest, %{s | name: name})

  defp hello_options(_other, _s), do: fail("ERR Syntax error in HELLO option")

  defp auth(password, s) do
    case Kurwa.Config.auth_token() do
      nil ->
        {{:error,
          "ERR AUTH <password> called without any password configured for the default user. " <>
            "Are you sure your configuration is correct?"}, s}

      token ->
        if Plug.Crypto.secure_compare(password, to_string(token)),
          do: {{:simple, "OK"}, %{s | authed: true}},
          else: {{:error, "WRONGPASS invalid username-password pair or user is disabled."}, s}
    end
  end

  defp client("SETNAME", [name], s), do: {{:simple, "OK"}, %{s | name: name}}
  defp client("GETNAME", [], s), do: {s.name, s}
  defp client("ID", [], s), do: {s.id, s}
  defp client("SETINFO", [_attr, _value], s), do: {{:simple, "OK"}, s}
  defp client("INFO", [], s), do: {"id=#{s.id} name=#{s.name || ""} db=0 resp=#{s.proto}\n", s}
  defp client(sub, _args, s) when sub in ~w(NO-EVICT NO-TOUCH REPLY), do: {{:simple, "OK"}, s}
  defp client(sub, _args, _s), do: fail("ERR unknown subcommand '#{sub}'")

  defp set_options([], condition, ttl), do: {condition, ttl}

  defp set_options([opt | rest], condition, ttl) do
    case {String.upcase(opt), rest} do
      {"NX", rest} when condition == nil ->
        set_options(rest, :nx, ttl)

      {"XX", rest} when condition == nil ->
        set_options(rest, :xx, ttl)

      {"EX", [n | rest]} when ttl == nil ->
        set_options(rest, condition, positive!(n) * 1000)

      {"PX", [n | rest]} when ttl == nil ->
        set_options(rest, condition, positive!(n))

      {"EXAT", [t | rest]} when ttl == nil ->
        set_options(rest, condition, until(positive!(t) * 1000))

      {"PXAT", [t | rest]} when ttl == nil ->
        set_options(rest, condition, until(positive!(t)))

      {"KEEPTTL", _} ->
        fail("ERR KEEPTTL is not supported: a SET replaces the key's expiry")

      {"GET", _} ->
        fail("ERR SET ... GET needs a value to return, and kurwadb stores none")

      _ ->
        fail("ERR syntax error")
    end
  end

  defp until(epoch_ms) do
    case epoch_ms - System.system_time(:millisecond) do
      ms when ms > 0 -> ms
      _ -> fail("ERR invalid expire time in 'set' command")
    end
  end

  defp expire(key, ms, s) do
    if member?(nil, key), do: ok!(add(nil, key, ms)) && {1, s}, else: {0, s}
  end

  # Redis: -2 for a key that is not there, -1 for one with no expiry.
  defp ttl(key, unit) do
    case lookup(nil, key) do
      nil ->
        -2

      record ->
        case Record.ttl(record) do
          :never -> -1
          ms -> div(ms + unit - 1, unit)
        end
    end
  end

  defp positive!(text) do
    case Integer.parse(text) do
      {n, ""} when n > 0 -> n
      _ -> fail("ERR value is not an integer or out of range")
    end
  end

  defp set!(name) do
    if Key.valid_name?(name),
      do: name,
      else:
        fail(
          "ERR \"#{name}\" is not a usable set name: letters, digits and _ . : -, not starting with _"
        )
  end

  # SET NX: one winner among concurrent callers. See Kurwa.Coordinator.add_new/2.
  defp add_new(key, ttl) do
    case Kurwa.add_new(key, if(ttl, do: [ttl: ttl], else: [])) do
      :ok -> true
      :exists -> false
      {:error, {:no_majority, _}} -> fail("NOREPLICAS Not enough good replicas to write.")
      {:error, reason} -> fail("ERR the cluster could not answer: #{inspect(reason)}")
    end
  end

  defp version(key) do
    case ok!(Coordinator.lookup(Key.encode(nil, key))) do
      nil -> nil
      record -> {Record.lamport(record), elem(record, 2), Record.member?(record)}
    end
  end

  defp watch_broken?(watched), do: Enum.any?(watched, fn {key, seen} -> version(key) != seen end)

  defp known_sets do
    case Namespace.list() do
      {:ok, %{sets: sets}} -> sets
      _ -> []
    end
  end

  defp add(nil, key, nil), do: Kurwa.add(key)
  defp add(nil, key, ms), do: Kurwa.add(key, ttl: ms)
  defp add(set, key, nil), do: Namespace.add(set, key)

  defp member?(nil, key), do: ok!(Kurwa.fetch(key))
  defp member?(set, key), do: ok!(Namespace.member?(set, key))

  defp lookup(set, key) do
    record = ok!(Coordinator.lookup(Key.encode(set, key)))
    if Record.member?(record), do: record, else: nil
  end

  defp ok!(:ok), do: true
  defp ok!({:ok, value}), do: value
  defp ok!({:error, reason}), do: fail("ERR the cluster could not answer: #{inspect(reason)}")

  defp fail(message), do: throw({:resp_error, message})
end
