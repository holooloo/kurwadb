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
    assert body["engine"] == "Kurwa.Store.Ets"
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
