defmodule Kurwa.Gateway.Auth do
  @moduledoc """
  Optional shared-secret gate in front of the HTTP API.

  Off unless `:auth_token` is configured (`KURWA_AUTH_TOKEN`). `/health` stays
  open so load balancers do not need the secret, and so is the dashboard: it
  shows rates, sizes and who is connected, never a key or a set name.
  """

  @behaviour Plug

  import Plug.Conn

  @public ["/health", "/dashboard", "/dashboard/state"]

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    case Kurwa.Config.auth_token() do
      nil -> conn
      _token when conn.request_path in @public -> conn
      token -> verify(conn, to_string(token))
    end
  end

  defp verify(conn, token) do
    with ["Bearer " <> presented] <- get_req_header(conn, "authorization"),
         true <- Plug.Crypto.secure_compare(presented, token) do
      conn
    else
      _ ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(401, ~s({"error":"unauthorized"}))
        |> halt()
    end
  end
end
