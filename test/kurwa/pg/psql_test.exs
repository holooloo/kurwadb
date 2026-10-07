defmodule Kurwa.Pg.PsqlTest do
  @moduledoc """
  The real psql against the real server. Skipped where psql is not installed:

      mix test --include psql
  """
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [eventually: 1]

  @moduletag :psql

  setup_all do
    psql = System.find_executable("psql")
    if psql == nil, do: raise("psql is not installed; run without --include psql")

    {:ok, server} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Pg.Server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, psql_bin: psql, dsn: "host=127.0.0.1 port=#{port} user=tester dbname=kurwadb"}
  end

  defp psql(%{psql_bin: psql, dsn: dsn}, args, env \\ []) do
    System.cmd(psql, [dsn, "-X", "-v", "ON_ERROR_STOP=0" | args],
      stderr_to_stdout: true,
      env: env
    )
  end

  # SCRAM-SHA-256-PLUS over TLS, the way libpq does it by default when both
  # sides can: the proof covers a hash of the certificate, so a wrong
  # end-point hash would fail here.
  test "SCRAM over TLS, with and without channel binding", %{psql_bin: psql} do
    %{server_config: server} =
      :public_key.pkix_test_data(%{
        server_chain: %{
          root: [key: {:rsa, 2048, 65537}, digest: :sha256],
          peer: [key: {:rsa, 2048, 65537}, digest: :sha256]
        },
        client_chain: %{
          root: [key: {:rsa, 2048, 65537}, digest: :sha256],
          peer: [key: {:rsa, 2048, 65537}, digest: :sha256]
        }
      })
      |> Map.new()

    original =
      {Application.get_env(:kurwadb, :auth_token), Application.get_env(:kurwadb, :pg_tls)}

    Application.put_env(:kurwadb, :auth_token, "s3cret")
    Application.put_env(:kurwadb, :pg_tls, Keyword.take(server, [:cert, :key, :cacerts]))

    on_exit(fn ->
      Application.put_env(:kurwadb, :auth_token, elem(original, 0))
      Application.put_env(:kurwadb, :pg_tls, elem(original, 1))
    end)

    {:ok, tls_server} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Pg.Server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(tls_server)
    base = "host=127.0.0.1 port=#{port} user=tester dbname=kurwadb"

    for {extra, label} <- [
          {"sslmode=require channel_binding=require", "PLUS"},
          {"sslmode=require channel_binding=disable", "SCRAM over TLS"},
          {"sslmode=disable", "SCRAM in the clear"}
        ] do
      {out, code} =
        psql(%{psql_bin: psql, dsn: "#{base} #{extra}"}, ["-tc", "SELECT 1"], [
          {"PGPASSWORD", "s3cret"}
        ])

      assert {code, String.trim(out)} == {0, "1"}, "#{label}: #{out}"
    end

    {out, code} =
      psql(%{psql_bin: psql, dsn: "#{base} sslmode=require"}, ["-tc", "SELECT 1"], [
        {"PGPASSWORD", "nope"}
      ])

    assert code != 0
    assert out =~ "password authentication failed"
  end

  test "queries, \\dt and \\d", context do
    set = "psqltest#{System.unique_integer([:positive])}"

    {out, 0} =
      psql(context, [
        "-c",
        "INSERT INTO #{set} VALUES ('a'), ('b')",
        "-c",
        "SELECT key FROM #{set} WHERE key IN ('a', 'c')",
        "-c",
        "DELETE FROM #{set} WHERE key = 'b'"
      ])

    assert out =~ "INSERT 0 2"
    assert out =~ ~r/key\s*\n-+\n a\n\(1 row\)/
    assert out =~ "DELETE 1"

    eventually(fn ->
      {:ok, %{sets: sets}} = Kurwa.Namespace.list()
      set in sets
    end)

    {out, 0} = psql(context, ["-c", "\\dt"])
    assert out =~ ~r/public\s*\| #{set}\s*\| table \| tester/

    {out, 0} = psql(context, ["-c", "\\d #{set}"])
    assert out =~ ~s(Table "public.#{set}")
    assert out =~ ~r/key\s+\| text\s+\|\s+\| not null/
  end
end
