defmodule Kurwa.Resp.ServerTest do
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [eventually: 1]

  setup_all do
    {:ok, server} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Resp.Server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, port: port}
  end

  setup %{port: port} do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)
    on_exit(fn -> :gen_tcp.close(socket) end)

    {:ok,
     socket: socket,
     set: "resp:#{System.unique_integer([:positive])}",
     key: "k:#{System.unique_integer([:positive])}"}
  end

  defp command(socket, args), do: hd(pipeline(socket, [args]))

  # Sends every command at once and reads one reply per command, so this is
  # also a pipelining test.
  defp pipeline(socket, commands) do
    data =
      Enum.map(commands, fn args ->
        ["*#{length(args)}\r\n", Enum.map(args, &"$#{byte_size(&1)}\r\n#{&1}\r\n")]
      end)

    :ok = :gen_tcp.send(socket, data)
    for _ <- commands, do: reply(socket)
  end

  defp reply(socket) do
    {:ok, line} = recv_line(socket)

    case line do
      "+" <> s ->
        {:simple, s}

      "-" <> s ->
        {:error, s}

      ":" <> n ->
        String.to_integer(n)

      "$-1" ->
        nil

      "*-1" ->
        nil

      "_" ->
        nil

      "#t" ->
        true

      "#f" ->
        false

      "$" <> n ->
        {:ok, data} = :gen_tcp.recv(socket, String.to_integer(n) + 2, 2_000)
        binary_part(data, 0, String.to_integer(n))

      "*" <> n ->
        for _ <- 1..String.to_integer(n)//1, do: reply(socket)

      "%" <> n ->
        for _ <- 1..String.to_integer(n)//1, do: {reply(socket), reply(socket)}
    end
  end

  defp recv_line(socket, acc \\ "") do
    {:ok, c} = :gen_tcp.recv(socket, 1, 2_000)
    line = acc <> c

    if String.ends_with?(line, "\r\n"),
      do: {:ok, String.trim_trailing(line, "\r\n")},
      else: recv_line(socket, line)
  end

  test "ping, echo, select 0, and an inline command", %{socket: s} do
    assert command(s, ["PING"]) == {:simple, "PONG"}
    assert command(s, ["ECHO", "hi"]) == "hi"
    assert command(s, ["SELECT", "0"]) == {:simple, "OK"}
    assert {:error, _} = command(s, ["SELECT", "1"])

    :ok = :gen_tcp.send(s, "PING\r\n")
    assert reply(s) == {:simple, "PONG"}
  end

  test "SET NX EX, EXISTS, TTL, DEL, as a dedup check", %{socket: s, key: k} do
    assert pipeline(s, [
             ["SET", k, "1", "EX", "3600", "NX"],
             ["SET", k, "1", "NX"],
             ["EXISTS", k, "nope:#{k}"],
             ["TTL", k],
             ["TTL", "nope:#{k}"],
             ["DEL", k, "nope:#{k}"],
             ["DEL", k]
           ]) == [{:simple, "OK"}, nil, 1, 3600, -2, 1, 0]
  end

  test "a key without an expiry, EXPIRE and PERSIST", %{socket: s, key: k} do
    assert pipeline(s, [
             ["SET", k, "x"],
             ["TTL", k],
             ["EXPIRE", k, "100"],
             ["TTL", k],
             ["PERSIST", k],
             ["TTL", k],
             ["EXPIRE", "nope:#{k}", "5"]
           ]) == [{:simple, "OK"}, -1, 1, 100, 1, -1, 0]
  end

  test "GET is an error: there are no values to return", %{socket: s, key: k} do
    command(s, ["SET", k, "value"])
    assert {:error, message} = command(s, ["GET", k])
    assert message =~ "without values"
  end

  test "sets: SADD and SREM count what changed", %{socket: s, set: set} do
    assert pipeline(s, [
             ["SADD", set, "a", "b", "a"],
             ["SADD", set, "b", "c"],
             ["SISMEMBER", set, "a"],
             ["SMISMEMBER", set, "a", "z", "c"],
             ["SREM", set, "a", "z"],
             ["SISMEMBER", set, "a"]
           ]) == [2, 1, 1, [1, 0, 1], 1, 0]
  end

  test "scans are refused, with the reason", %{socket: s, set: set} do
    for args <- [["KEYS", "*"], ["SCAN", "0"], ["SMEMBERS", set], ["SCARD", set], ["FLUSHDB"]] do
      assert {:error, message} = command(s, args)
      assert message =~ "no scans"
    end
  end

  test "MULTI queues and EXEC runs in order; an unknown command aborts", %{socket: s, set: set} do
    assert pipeline(s, [["MULTI"], ["SADD", set, "x"], ["SISMEMBER", set, "x"], ["EXEC"]]) ==
             [{:simple, "OK"}, {:simple, "QUEUED"}, {:simple, "QUEUED"}, [1, 1]]

    assert [{:simple, "OK"}, {:error, _}, {:error, "EXECABORT" <> _}] =
             pipeline(s, [["MULTI"], ["NOSUCH"], ["EXEC"]])
  end

  test "WATCH: EXEC runs nothing if another client changed the key", %{
    port: port,
    socket: s,
    key: k
  } do
    {:ok, other} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)
    on_exit(fn -> :gen_tcp.close(other) end)

    assert command(s, ["WATCH", k]) == {:simple, "OK"}
    assert command(other, ["SET", k, "1"]) == {:simple, "OK"}

    assert pipeline(s, [["MULTI"], ["SET", "#{k}:after", "1"], ["EXEC"]]) ==
             [{:simple, "OK"}, {:simple, "QUEUED"}, nil]

    assert command(s, ["EXISTS", "#{k}:after"]) == 0

    # Unchanged, it runs; and EXEC ends the watch either way.
    assert command(s, ["WATCH", k]) == {:simple, "OK"}

    assert pipeline(s, [["MULTI"], ["SET", "#{k}:after", "1"], ["EXEC"]]) ==
             [{:simple, "OK"}, {:simple, "QUEUED"}, [{:simple, "OK"}]]
  end

  test "WATCH on a set name is refused, not silently blind", %{socket: s, set: set} do
    command(s, ["SADD", set, "a"])

    eventually(fn ->
      {:ok, %{sets: sets}} = Kurwa.Namespace.list()
      set in sets
    end)

    assert {:error, message} = command(s, ["WATCH", set])
    assert message =~ "set"
  end

  test "SET NX and SETNX answer for one winner", %{socket: s, key: k} do
    assert pipeline(s, [["SETNX", k, "1"], ["SETNX", k, "1"], ["SET", k, "1", "NX"]]) == [
             1,
             0,
             nil
           ]
  end

  test "HELLO 3 switches the protocol, and nulls change shape", %{socket: s, key: k} do
    assert [{"server", "redis"} | _] = command(s, ["HELLO", "3"])
    assert command(s, ["SET", k, "1", "NX"]) == {:simple, "OK"}
    assert command(s, ["SET", k, "1", "NX"]) == nil
    assert {:error, "NOPROTO" <> _} = command(s, ["HELLO", "4"])
  end

  test "with an auth token, nothing runs before AUTH", %{socket: s} do
    original = Application.get_env(:kurwadb, :auth_token)
    Application.put_env(:kurwadb, :auth_token, "s3cret")
    on_exit(fn -> Application.put_env(:kurwadb, :auth_token, original) end)

    assert {:error, "NOAUTH" <> _} = command(s, ["PING"])
    assert {:error, "WRONGPASS" <> _} = command(s, ["AUTH", "wrong"])
    assert command(s, ["AUTH", "default", "s3cret"]) == {:simple, "OK"}
    assert command(s, ["PING"]) == {:simple, "PONG"}
  end

  test "set names with a colon, as Redis users write them", %{socket: s} do
    assert command(s, ["SADD", "seen:orders:#{System.unique_integer([:positive])}", "o1"]) == 1
    assert {:error, message} = command(s, ["SADD", "_reserved", "x"])
    assert message =~ "not a usable set name"
  end
end
