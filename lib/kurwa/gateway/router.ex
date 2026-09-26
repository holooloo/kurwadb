defmodule Kurwa.Gateway.Router do
  @moduledoc """
  HTTP surface of kurwadb.

      PUT    /k/:key                add the key            -> 200
      GET    /k/:key                membership check       -> 200 member / 404 absent
      DELETE /k/:key                remove the key         -> 200
      POST   /batch                 {"op":"add"|"member"|"delete","keys":[...]}

      PUT    /sets/:set/k/:key      same three, in a named set
      GET    /sets/:set/k/:key
      DELETE /sets/:set/k/:key
      GET    /union/k/:key?sets=a,b         member of ANY of these sets
      GET    /intersection/k/:key?sets=a,b  member of ALL of these sets

      GET    /count                 approximate live keys
      GET    /info                  ring, quorum settings, local and cache stats
      GET    /health                liveness (never authenticated)

  Keys are URL path segments, so a key containing `/` must be percent-encoded.
  For keys that are not valid UTF-8, send them base64url-encoded and add
  `?b64=1`; responses echo the key exactly as it arrived in the path, because a
  decoded binary key has no JSON representation.

  A failed quorum answers 503, never 200 - an unreachable replica must not read
  as "key is not in the set".
  """

  use Plug.Router

  alias Kurwa.Config
  alias Kurwa.Namespace

  @max_batch 1_000

  plug(:match)
  plug(Kurwa.Gateway.Auth)

  plug(Plug.Parsers,
    parsers: [:json],
    pass: ["application/json"],
    json_decoder: Jason,
    length: 4_000_000
  )

  plug(:dispatch)

  put "/k/:key" do
    with {:ok, decoded} <- decode_key(conn, key) do
      respond(conn, Kurwa.add(decoded), %{ok: true, key: key})
    else
      error -> key_error(conn, error)
    end
  end

  get "/k/:key" do
    with {:ok, decoded} <- decode_key(conn, key) do
      membership(conn, Kurwa.fetch(decoded), %{key: key})
    else
      error -> key_error(conn, error)
    end
  end

  delete "/k/:key" do
    with {:ok, decoded} <- decode_key(conn, key) do
      respond(conn, Kurwa.delete(decoded), %{ok: true, key: key})
    else
      error -> key_error(conn, error)
    end
  end

  put "/sets/:set/k/:key" do
    with {:ok, set} <- namespace(set),
         {:ok, decoded} <- decode_key(conn, key) do
      respond(conn, Namespace.add(set, decoded), %{ok: true, set: set, key: key})
    else
      error -> key_error(conn, error)
    end
  end

  get "/sets/:set/k/:key" do
    with {:ok, set} <- namespace(set),
         {:ok, decoded} <- decode_key(conn, key) do
      membership(conn, Namespace.member?(set, decoded), %{set: set, key: key})
    else
      error -> key_error(conn, error)
    end
  end

  delete "/sets/:set/k/:key" do
    with {:ok, set} <- namespace(set),
         {:ok, decoded} <- decode_key(conn, key) do
      respond(conn, Namespace.delete(set, decoded), %{ok: true, set: set, key: key})
    else
      error -> key_error(conn, error)
    end
  end

  get "/union/k/:key" do
    with {:ok, sets} <- sets_param(conn),
         {:ok, decoded} <- decode_key(conn, key) do
      membership(conn, Namespace.member_any?(sets, decoded), %{sets: sets, key: key})
    else
      error -> key_error(conn, error)
    end
  end

  get "/intersection/k/:key" do
    with {:ok, sets} <- sets_param(conn),
         {:ok, decoded} <- decode_key(conn, key) do
      membership(conn, Namespace.member_all?(sets, decoded), %{sets: sets, key: key})
    else
      error -> key_error(conn, error)
    end
  end

  post "/batch" do
    case conn.body_params do
      %{"op" => op, "keys" => keys} when is_list(keys) and op in ~w(add member delete) ->
        cond do
          keys == [] ->
            json(conn, 400, %{error: "keys is empty"})

          length(keys) > @max_batch ->
            json(conn, 413, %{error: "at most #{@max_batch} keys per batch"})

          not Enum.all?(keys, &is_binary/1) ->
            json(conn, 400, %{error: "keys must be strings"})

          true ->
            json(conn, 200, run_batch(op, keys))
        end

      _ ->
        json(conn, 400, %{error: ~s(expected {"op":"add"|"member"|"delete","keys":["..."]})})
    end
  end

  get "/count" do
    case Kurwa.count() do
      {:ok, stats} ->
        json(conn, 200, %{
          approximate: stats.approximate,
          replicas: stats.replicas,
          per_node: stringify(stats.per_node),
          unreachable: stringify_reasons(stats.unreachable)
        })

      {:error, reason} ->
        json(conn, 503, %{error: reason_json(reason)})
    end
  end

  get "/info" do
    info = Kurwa.info()

    json(conn, 200, %{
      node: to_string(info.node),
      members: Enum.map(info.members, &to_string/1),
      up: Enum.map(info.up, &to_string/1),
      down: Enum.map(info.down, &to_string/1),
      handoff: stringify(info.handoff),
      vnodes: info.vnodes,
      n: info.n,
      r: info.r,
      w: info.w,
      strict_quorum: info.strict_quorum,
      shards: info.shards,
      engine: inspect(info.engine),
      local_keys: info.local_keys,
      lamport: info.lamport,
      cache: info.cache
    })
  end

  get "/health" do
    members = Kurwa.Cluster.members()
    healthy? = length(members) >= 1
    json(conn, if(healthy?, do: 200, else: 503), %{status: "ok", members: length(members)})
  end

  match _ do
    json(conn, 404, %{error: "no such route"})
  end

  defp run_batch("member", keys) do
    results =
      keys
      |> parallel(fn key -> Kurwa.fetch(key) end)
      |> Enum.zip(keys)
      |> Enum.reduce(%{members: %{}, errors: %{}}, fn
        {{:ok, {:ok, member?}}, key}, acc ->
          %{acc | members: Map.put(acc.members, key, member?)}

        {{:ok, {:error, reason}}, key}, acc ->
          %{acc | errors: Map.put(acc.errors, key, reason_json(reason))}

        {{:exit, reason}, key}, acc ->
          %{acc | errors: Map.put(acc.errors, key, inspect(reason))}
      end)

    %{op: "member", members: results.members, errors: results.errors}
  end

  defp run_batch(op, keys) do
    fun = if op == "add", do: &Kurwa.add/1, else: &Kurwa.delete/1

    errors =
      keys
      |> parallel(fun)
      |> Enum.zip(keys)
      |> Enum.reduce(%{}, fn
        {{:ok, :ok}, _key}, acc -> acc
        {{:ok, {:error, reason}}, key}, acc -> Map.put(acc, key, reason_json(reason))
        {{:exit, reason}, key}, acc -> Map.put(acc, key, inspect(reason))
      end)

    %{op: op, applied: length(keys) - map_size(errors), errors: errors}
  end

  defp parallel(keys, fun) do
    Task.async_stream(keys, fun,
      max_concurrency: 32,
      ordered: true,
      on_timeout: :kill_task,
      timeout: Config.request_timeout() * 4
    )
  end

  defp decode_key(conn, raw) do
    conn = fetch_query_params(conn)

    if conn.query_params["b64"] in ["1", "true"] do
      case Base.url_decode64(raw, padding: false) do
        {:ok, key} -> {:ok, key}
        :error -> {:error, :bad_key}
      end
    else
      {:ok, raw}
    end
  end

  defp namespace(name) do
    if Namespace.valid_name?(name), do: {:ok, name}, else: {:error, {:bad_set, name}}
  end

  defp sets_param(conn) do
    conn = fetch_query_params(conn)

    case String.split(conn.query_params["sets"] || "", ",", trim: true) do
      [] ->
        {:error, :no_sets}

      names ->
        case Enum.reject(names, &Namespace.valid_name?/1) do
          [] -> {:ok, names}
          [bad | _] -> {:error, {:bad_set, bad}}
        end
    end
  end

  # 200 when the key is a member, 404 when it is not, 503 when we could not find
  # out - never 200-with-false, which a client would read as a definite answer.
  defp membership(conn, {:ok, true}, body), do: json(conn, 200, Map.put(body, :member, true))
  defp membership(conn, {:ok, false}, body), do: json(conn, 404, Map.put(body, :member, false))

  defp membership(conn, {:error, reason}, _body),
    do: json(conn, 503, %{error: reason_json(reason)})

  defp key_error(conn, {:error, :bad_key}),
    do: json(conn, 400, %{error: "key is not valid base64url"})

  defp key_error(conn, {:error, {:bad_set, name}}),
    do: json(conn, 400, %{error: "invalid set name: #{inspect(name)}"})

  defp key_error(conn, {:error, :no_sets}),
    do: json(conn, 400, %{error: "pass ?sets=a,b with at least one set name"})

  defp respond(conn, :ok, body), do: json(conn, 200, body)
  defp respond(conn, {:error, reason}, _body), do: json(conn, 503, %{error: reason_json(reason)})

  defp json(conn, status, body) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode_to_iodata!(body))
  end

  defp reason_json(:ring_empty), do: %{kind: "ring_empty"}

  defp reason_json({:quorum_not_met, details}) do
    %{
      kind: "quorum_not_met",
      op: to_string(details.op),
      needed: details.needed,
      got: details.got,
      replicas: Enum.map(details.replicas, &to_string/1),
      failed: stringify_reasons(details.failed)
    }
  end

  defp reason_json(:unavailable), do: %{kind: "unavailable"}

  defp reason_json({:incomplete_union, errors}) do
    %{kind: "incomplete_union", sets: stringify_reasons(errors)}
  end

  defp reason_json(other), do: %{kind: "error", detail: inspect(other)}

  defp stringify(map), do: Map.new(map, fn {node, value} -> {to_string(node), value} end)

  defp stringify_reasons(map) do
    Map.new(map, fn {node, reason} -> {to_string(node), inspect(reason)} end)
  end
end
