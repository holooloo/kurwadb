defmodule Kurwa.Gateway.RouterTest do
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn
  import Kurwa.TestHelpers, only: [unique_key: 1]

  alias Kurwa.Gateway.Router

  @opts Router.init([])

  test "put, get, delete, get" do
    key = unique_key("http")

    assert %{status: 200, body: %{"ok" => true, "key" => ^key}} = request(:put, "/k/#{key}")
    assert %{status: 200, body: %{"member" => true}} = request(:get, "/k/#{key}")
    assert %{status: 200} = request(:delete, "/k/#{key}")
    assert %{status: 404, body: %{"member" => false}} = request(:get, "/k/#{key}")
  end

  test "a key that was never added is a 404, not an error" do
    assert %{status: 404, body: %{"member" => false}} = request(:get, "/k/#{unique_key("nope")}")
  end

  test "keys with reserved characters survive percent-encoding" do
    key = "a/b c?d#e"
    encoded = URI.encode(key, &URI.char_unreserved?/1)

    assert %{status: 200} = request(:put, "/k/#{encoded}")

    assert %{status: 200, body: %{"member" => true, "key" => ^key}} =
             request(:get, "/k/#{encoded}")

    assert %{status: 200} = request(:delete, "/k/#{encoded}")
  end

  test "non-text keys go through base64url and are echoed back encoded" do
    key = <<0, 255, 200, 1>>
    encoded = Base.url_encode64(key, padding: false)

    assert %{status: 200, body: %{"key" => ^encoded}} = request(:put, "/k/#{encoded}?b64=1")
    assert %{status: 200, body: %{"member" => true}} = request(:get, "/k/#{encoded}?b64=1")

    # same key, reached without the encoding hop
    assert Kurwa.member?(key)
  end

  test "a malformed base64 key is a client error" do
    assert %{status: 400, body: %{"error" => error}} = request(:get, "/k/not!base64?b64=1")
    assert error =~ "base64url"
  end

  test "batch add, then batch member, then batch delete" do
    keys = for i <- 1..5, do: unique_key("batch-#{i}")

    assert %{status: 200, body: %{"applied" => 5, "errors" => %{}}} =
             request(:post, "/batch", %{op: "add", keys: keys})

    assert %{status: 200, body: %{"members" => members}} =
             request(:post, "/batch", %{
               op: "member",
               keys: keys ++ ["missing-#{unique_key("x")}"]
             })

    assert Enum.all?(keys, &(members[&1] == true))
    assert Enum.count(members, fn {_key, member?} -> member? == false end) == 1

    assert %{status: 200, body: %{"applied" => 5}} =
             request(:post, "/batch", %{op: "delete", keys: keys})

    assert %{status: 200, body: %{"members" => gone}} =
             request(:post, "/batch", %{op: "member", keys: keys})

    assert Enum.all?(keys, &(gone[&1] == false))
  end

  test "batch rejects nonsense" do
    assert %{status: 400} = request(:post, "/batch", %{op: "sort", keys: ["a"]})
    assert %{status: 400} = request(:post, "/batch", %{op: "add", keys: []})
    assert %{status: 400} = request(:post, "/batch", %{op: "add", keys: [1, 2]})
    assert %{status: 400} = request(:post, "/batch", %{keys: ["a"]})

    too_many = for i <- 1..1_001, do: "k#{i}"
    assert %{status: 413} = request(:post, "/batch", %{op: "add", keys: too_many})
  end

  test "named sets are separate namespaces over the same API" do
    key = unique_key("set")

    assert %{status: 200, body: %{"set" => "alpha"}} = request(:put, "/sets/alpha/k/#{key}")
    assert %{status: 200, body: %{"member" => true}} = request(:get, "/sets/alpha/k/#{key}")

    # same key, different set, and the default set - all independent
    assert %{status: 404} = request(:get, "/sets/beta/k/#{key}")
    assert %{status: 404} = request(:get, "/k/#{key}")

    assert %{status: 200} = request(:delete, "/sets/alpha/k/#{key}")
    assert %{status: 404} = request(:get, "/sets/alpha/k/#{key}")
  end

  test "a set name the store cannot represent is a client error" do
    assert %{status: 400, body: %{"error" => error}} = request(:get, "/sets/has%20space/k/x")
    assert error =~ "invalid set name"
  end

  test "union answers yes when any listed set has the key" do
    key = unique_key("union-http")

    assert %{status: 200} = request(:put, "/sets/greylist/k/#{key}")

    assert %{status: 200, body: %{"member" => true, "sets" => ["blacklist", "greylist"]}} =
             request(:get, "/union/k/#{key}?sets=blacklist,greylist")

    assert %{status: 404, body: %{"member" => false}} =
             request(:get, "/union/k/#{unique_key("nobody")}?sets=blacklist,greylist")
  end

  test "intersection answers yes only when every listed set has the key" do
    key = unique_key("inter-http")

    assert %{status: 200} = request(:put, "/sets/alpha/k/#{key}")

    assert %{status: 404, body: %{"member" => false}} =
             request(:get, "/intersection/k/#{key}?sets=alpha,beta")

    assert %{status: 200} = request(:put, "/sets/beta/k/#{key}")

    assert %{status: 200, body: %{"member" => true}} =
             request(:get, "/intersection/k/#{key}?sets=alpha,beta")
  end

  test "a union needs at least one set to read from" do
    assert %{status: 400, body: %{"error" => error}} = request(:get, "/union/k/x")
    assert error =~ "sets="

    assert %{status: 400} = request(:get, "/union/k/x?sets=has%20space")
  end

  test "a key can be given an expiry in the URL" do
    key = unique_key("http-ttl")

    assert %{status: 200} = request(:put, "/k/#{key}?ttl_ms=150")
    assert %{status: 200, body: %{"member" => true}} = request(:get, "/k/#{key}")

    Process.sleep(250)
    assert %{status: 404, body: %{"member" => false}} = request(:get, "/k/#{key}")
  end

  test "named sets take an expiry too" do
    key = unique_key("http-set-ttl")

    assert %{status: 200} = request(:put, "/sets/alpha/k/#{key}?ttl_ms=150")
    assert %{status: 200} = request(:get, "/sets/alpha/k/#{key}")

    Process.sleep(250)
    assert %{status: 404} = request(:get, "/sets/alpha/k/#{key}")
  end

  test "a batch can expire as a whole" do
    keys = for i <- 1..3, do: unique_key("http-batch-ttl-#{i}")

    assert %{status: 200, body: %{"applied" => 3}} =
             request(:post, "/batch", %{op: "add", keys: keys, ttl_ms: 150})

    assert %{status: 200, body: %{"members" => present}} =
             request(:post, "/batch", %{op: "member", keys: keys})

    assert Enum.all?(keys, &(present[&1] == true))

    Process.sleep(250)

    assert %{status: 200, body: %{"members" => gone}} =
             request(:post, "/batch", %{op: "member", keys: keys})

    assert Enum.all?(keys, &(gone[&1] == false))
  end

  test "a nonsense expiry is a client error, not a key that never dies" do
    key = unique_key("bad-ttl")

    assert %{status: 400, body: %{"error" => error}} = request(:put, "/k/#{key}?ttl=0")
    assert error =~ "positive"

    assert %{status: 400} = request(:put, "/k/#{key}?ttl=-1")
    assert %{status: 400} = request(:put, "/k/#{key}?ttl=soon")
    assert %{status: 400} = request(:put, "/k/#{key}?ttl=60&ttl_ms=1000")
    assert %{status: 400} = request(:post, "/batch", %{op: "add", keys: [key], ttl: "soon"})

    # and none of those created the key
    assert %{status: 404} = request(:get, "/k/#{key}")
  end

  test "count reports the cluster estimate and who answered" do
    assert %{status: 200, body: body} = request(:get, "/count")

    assert is_integer(body["approximate"])
    assert body["replicas"] == 1
    assert body["unreachable"] == %{}
    assert map_size(body["per_node"]) == 1
  end

  test "info describes the node and its quorum settings" do
    assert %{status: 200, body: body} = request(:get, "/info")

    assert body["node"] == to_string(node())
    assert body["members"] == [to_string(node())]
    assert body["n"] == 1
    assert body["shards"] == 2
    assert body["engine"] == inspect(Kurwa.Config.engine())
  end

  test "health is up while the node is in its own ring" do
    assert %{status: 200, body: %{"status" => "ok", "members" => 1}} = request(:get, "/health")
  end

  test "an unknown route is a 404" do
    assert %{status: 404, body: %{"error" => "no such route"}} = request(:get, "/nothing/here")
  end

  describe "with an auth token configured" do
    setup do
      original = Application.get_env(:kurwadb, :auth_token)
      Application.put_env(:kurwadb, :auth_token, "s3cret")
      on_exit(fn -> Application.put_env(:kurwadb, :auth_token, original) end)
      :ok
    end

    test "requests without the token are rejected" do
      assert %{status: 401} = request(:get, "/k/whatever")
      assert %{status: 401} = request(:put, "/k/whatever")
    end

    test "the wrong token is rejected" do
      assert %{status: 401} =
               request(:get, "/k/whatever", nil, [{"authorization", "Bearer nope"}])
    end

    test "the right token is accepted" do
      key = unique_key("authed")
      headers = [{"authorization", "Bearer s3cret"}]

      assert %{status: 200} = request(:put, "/k/#{key}", nil, headers)
      assert %{status: 200, body: %{"member" => true}} = request(:get, "/k/#{key}", nil, headers)
    end

    test "health stays open so load balancers keep working" do
      assert %{status: 200} = request(:get, "/health")
    end
  end

  defp request(method, path, body \\ nil, headers \\ []) do
    conn =
      case body do
        nil ->
          conn(method, path)

        body ->
          conn(method, path, Jason.encode!(body))
          |> put_req_header("content-type", "application/json")
      end

    conn =
      Enum.reduce(headers, conn, fn {name, value}, conn -> put_req_header(conn, name, value) end)

    conn = Router.call(conn, @opts)

    %{status: conn.status, body: decode(conn.resp_body)}
  end

  defp decode(body) do
    case Jason.decode(body) do
      {:ok, decoded} -> decoded
      {:error, _} -> body
    end
  end
end
