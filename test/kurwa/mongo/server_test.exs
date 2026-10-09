defmodule Kurwa.Mongo.ServerTest do
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [eventually: 1]

  alias Kurwa.Mongo.Bson

  setup_all do
    {:ok, server} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Mongo.Server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, port: port}
  end

  setup %{port: port} do
    {:ok, s} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)
    on_exit(fn -> :gen_tcp.close(s) end)
    {:ok, s: s, coll: "mtest#{System.unique_integer([:positive])}"}
  end

  # OP_MSG with a body; `sequence` adds a kind-1 section, as drivers send inserts.
  defp command(s, pairs, sequence \\ nil) do
    body = Bson.encode({:doc, pairs ++ [{"$db", "kurwadb"}]})

    seq =
      case sequence do
        nil ->
          <<>>

        {name, docs} ->
          payload = IO.iodata_to_binary([name, 0, Enum.map(docs, &Bson.encode/1)])
          <<1, byte_size(payload) + 4::32-little, payload::binary>>
      end

    msg = <<0::32-little, 0, body::binary, seq::binary>>

    :ok =
      :gen_tcp.send(
        s,
        <<byte_size(msg) + 16::32-little, 7::32-little, 0::32-little, 2013::32-little,
          msg::binary>>
      )

    {:ok, <<len::32-little, _id::32, 7::32-little, 2013::32-little>>} =
      :gen_tcp.recv(s, 16, 5_000)

    {:ok, <<_flags::32, 0, doc::binary>>} = :gen_tcp.recv(s, len - 16, 5_000)
    Bson.decode!(doc)
  end

  defp get(doc, path), do: Enum.reduce(path, doc, fn k, d -> Bson.get(d, k) end)

  test "hello answers as a mongos", %{s: s} do
    reply = command(s, [{"hello", 1}])
    assert Bson.get(reply, "ok") == 1.0
    assert Bson.get(reply, "msg") == "isdbgrid"
    assert Bson.get(reply, "isWritablePrimary") == true
    assert Bson.get(reply, "maxWireVersion") >= 17
  end

  test "the legacy OP_QUERY hello still works", %{s: s} do
    query = Bson.encode({:doc, [{"isMaster", 1}]})
    body = <<0::32, "admin.$cmd", 0, 0::32, -1::32-little-signed, query::binary>>

    :ok =
      :gen_tcp.send(
        s,
        <<byte_size(body) + 16::32-little, 9::32-little, 0::32, 2004::32-little, body::binary>>
      )

    {:ok, <<len::32-little, _id::32, 9::32-little, 1::32-little>>} = :gen_tcp.recv(s, 16, 5_000)

    {:ok, <<_flags::32, 0::64, 0::32, 1::32-little, doc::binary>>} =
      :gen_tcp.recv(s, len - 16, 5_000)

    assert Bson.get(Bson.decode!(doc), "ismaster") == true
  end

  test "insert, find by _id and $in, duplicate _id, delete", %{s: s, coll: c} do
    assert get(
             command(
               s,
               [{"insert", c}],
               {"documents", [{:doc, [{"_id", "a"}]}, {:doc, [{"_id", "b"}]}]}
             ),
             ["n"]
           ) == 2

    found =
      command(s, [{"find", c}, {"filter", {:doc, [{"_id", {:doc, [{"$in", ["a", "zz"]}]}}]}}])

    assert get(found, ["cursor", "firstBatch"]) == [{:doc, [{"_id", "a"}]}]
    assert get(found, ["cursor", "id"]) == {:int64, 0}

    dup = command(s, [{"insert", c}, {"documents", [{:doc, [{"_id", "a"}]}]}])
    assert get(dup, ["n"]) == 0
    assert [error] = Bson.get(dup, "writeErrors")
    assert Bson.get(error, "code") == 11_000
    assert Bson.get(error, "errmsg") =~ "E11000 duplicate key"

    deletes = [{:doc, [{"q", {:doc, [{"_id", {:doc, [{"$in", ["a", "zz"]}]}}]}}, {"limit", 0}]}]
    assert get(command(s, [{"delete", c}, {"deletes", deletes}]), ["n"]) == 1
  end

  test "an ordered insert stops at the first duplicate; unordered goes on", %{s: s, coll: c} do
    command(s, [{"insert", c}, {"documents", [{:doc, [{"_id", "x"}]}]}])
    docs = for id <- ["y", "x", "z"], do: {:doc, [{"_id", id}]}
    assert get(command(s, [{"insert", c}, {"documents", docs}]), ["n"]) == 1

    assert get(
             command(s, [
               {"insert", c},
               {"documents", [{:doc, [{"_id", "x"}]}, {:doc, [{"_id", "w"}]}]},
               {"ordered", false}
             ]),
             ["n"]
           ) == 1
  end

  test "updateOne with upsert and $setOnInsert is add-if-new", %{s: s, coll: c} do
    update = [
      {:doc,
       [
         {"q", {:doc, [{"_id", "job"}]}},
         {"u", {:doc, [{"$setOnInsert", {:doc, []}}]}},
         {"upsert", true}
       ]}
    ]

    first = command(s, [{"update", c}, {"updates", update}])
    assert get(first, ["n"]) == 1
    assert [{:doc, [{"index", 0}, {"_id", "job"}]}] = Bson.get(first, "upserted")

    second = command(s, [{"update", c}, {"updates", update}])
    assert get(second, ["n"]) == 1
    assert Bson.get(second, "upserted") == nil

    set_field = [
      {:doc,
       [{"q", {:doc, [{"_id", "job"}]}}, {"u", {:doc, [{"$set", {:doc, [{"name", "x"}]}}]}}]}
    ]

    assert [_] = Bson.get(command(s, [{"update", c}, {"updates", set_field}]), "writeErrors")
  end

  test "countDocuments' aggregation", %{s: s, coll: c} do
    command(s, [{"insert", c}, {"documents", [{:doc, [{"_id", "a"}]}, {:doc, [{"_id", "b"}]}]}])

    pipeline = [
      {:doc, [{"$match", {:doc, [{"_id", {:doc, [{"$in", ["a", "b", "c"]}]}}]}}]},
      {:doc, [{"$group", {:doc, [{"_id", 1}, {"n", {:doc, [{"$sum", 1}]}}]}}]}
    ]

    reply = command(s, [{"aggregate", c}, {"pipeline", pipeline}, {"cursor", {:doc, []}}])
    assert get(reply, ["cursor", "firstBatch"]) == [{:doc, [{"_id", 1}, {"n", 2}]}]
  end

  test "scans and fields are refused, with the reason", %{s: s, coll: c} do
    reply = command(s, [{"find", c}, {"filter", {:doc, []}}])
    assert Bson.get(reply, "ok") == 0.0
    assert Bson.get(reply, "errmsg") =~ "no scans"

    reply = command(s, [{"insert", c}, {"documents", [{:doc, [{"_id", "f"}, {"name", "x"}]}]}])
    assert [error] = Bson.get(reply, "writeErrors")
    assert Bson.get(error, "errmsg") =~ "keys only"
  end

  test "a non-string _id is found by its value", %{s: s, coll: c} do
    oid = {:oid, :crypto.strong_rand_bytes(12)}
    command(s, [{"insert", c}, {"documents", [{:doc, [{"_id", oid}]}, {:doc, [{"_id", 42}]}]}])

    assert get(command(s, [{"find", c}, {"filter", {:doc, [{"_id", oid}]}}]), [
             "cursor",
             "firstBatch"
           ]) == [{:doc, [{"_id", oid}]}]

    assert get(command(s, [{"find", c}, {"filter", {:doc, [{"_id", 42}]}}]), [
             "cursor",
             "firstBatch"
           ]) == [{:doc, [{"_id", 42}]}]

    assert get(command(s, [{"find", c}, {"filter", {:doc, [{"_id", "42"}]}}]), [
             "cursor",
             "firstBatch"
           ]) == []
  end

  test "a collection in kurwadb is the set of that name, as SQL and Redis see it", %{
    s: s,
    coll: c
  } do
    command(s, [{"insert", c}, {"documents", [{:doc, [{"_id", "shared"}]}]}])
    assert Kurwa.Namespace.member?(c, "shared") == {:ok, true}

    eventually(fn ->
      {:ok, %{sets: sets}} = Kurwa.Namespace.list()
      c in sets
    end)

    names =
      for d <- get(command(s, [{"listCollections", 1}]), ["cursor", "firstBatch"]),
          do: Bson.get(d, "name")

    assert c in names
  end

  test "abortTransaction says nothing was rolled back", %{s: s} do
    reply = command(s, [{"abortTransaction", 1}])
    assert Bson.get(reply, "ok") == 0.0
    assert Bson.get(reply, "errmsg") =~ "no transactions"
    assert Bson.get(command(s, [{"commitTransaction", 1}]), "ok") == 1.0
  end

  test "with an auth token, commands wait for SCRAM", %{s: s} do
    original = Application.get_env(:kurwadb, :auth_token)
    Kurwa.Config.put(:auth_token, "s3cret")
    on_exit(fn -> Kurwa.Config.put(:auth_token, original) end)

    assert Bson.get(command(s, [{"find", "x"}, {"filter", {:doc, [{"_id", "a"}]}}]), "code") == 13
    assert Bson.get(command(s, [{"hello", 1}]), "ok") == 1.0

    nonce = Base.encode64(:crypto.strong_rand_bytes(18))
    first_bare = "n=u,r=" <> nonce

    start =
      command(s, [
        {"saslStart", 1},
        {"mechanism", "SCRAM-SHA-256"},
        {"payload", {:binary, 0, "n,," <> first_bare}},
        {"options", {:doc, [{"skipEmptyExchange", true}]}}
      ])

    {:binary, _, server_first} = Bson.get(start, "payload")

    %{"r" => r, "s" => salt, "i" => i} =
      server_first
      |> String.split(",")
      |> Map.new(fn <<k::binary-size(1), "=", v::binary>> -> {k, v} end)

    salted =
      :crypto.pbkdf2_hmac(:sha256, "s3cret", Base.decode64!(salt), String.to_integer(i), 32)

    client_key = :crypto.mac(:hmac, :sha256, salted, "Client Key")
    without_proof = "c=biws,r=" <> r
    auth_message = Enum.join([first_bare, server_first, without_proof], ",")

    proof =
      :crypto.exor(
        client_key,
        :crypto.mac(:hmac, :sha256, :crypto.hash(:sha256, client_key), auth_message)
      )

    final =
      command(s, [
        {"saslContinue", 1},
        {"conversationId", 1},
        {"payload", {:binary, 0, without_proof <> ",p=" <> Base.encode64(proof)}}
      ])

    assert Bson.get(final, "done") == true
    assert Bson.get(command(s, [{"find", "x"}, {"filter", {:doc, [{"_id", "a"}]}}]), "ok") == 1.0
  end
end
