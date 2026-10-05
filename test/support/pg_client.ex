defmodule Kurwa.PgClient do
  @moduledoc """
  The smallest PostgreSQL client that can test `Kurwa.Pg.Server`: startup,
  simple queries, and the extended protocol one message at a time, with the
  backend's replies decoded into tuples.
  """

  def connect(port, opts \\ []) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)

    params =
      [{"user", Keyword.get(opts, :user, "test")}, {"database", "kurwadb"}] ++
        Keyword.get(opts, :params, [])

    body = [
      <<3::16, Keyword.get(opts, :minor, 0)::16>>,
      Enum.map(params, fn {k, v} -> [k, 0, v, 0] end),
      0
    ]

    send_raw(socket, [<<IO.iodata_length(body) + 4::32>>, body])

    case Keyword.get(opts, :password) do
      nil ->
        {socket, until_ready(socket)}

      password ->
        [{:auth, 3}] = recv_messages(socket, 1)
        send_message(socket, ?p, [password, 0])
        {socket, until_ready(socket)}
    end
  end

  def ssl_request(port) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)
    send_raw(socket, <<8::32, 80_877_103::32>>)
    {:ok, answer} = :gen_tcp.recv(socket, 1, 2_000)
    {socket, answer}
  end

  def query(socket, sql) do
    send_message(socket, ?Q, [sql, 0])
    until_ready(socket)
  end

  def parse(socket, name, sql, oids \\ []),
    do:
      send_message(socket, ?P, [
        name,
        0,
        sql,
        0,
        <<length(oids)::16>>,
        Enum.map(oids, &<<&1::32>>)
      ])

  def bind(socket, portal, statement, params, param_formats \\ [], result_formats \\ []) do
    values =
      Enum.map(params, fn
        nil -> <<-1::32-signed>>
        value -> [<<byte_size(value)::32>>, value]
      end)

    send_message(socket, ?B, [
      portal,
      0,
      statement,
      0,
      <<length(param_formats)::16>>,
      Enum.map(param_formats, &<<&1::16>>),
      <<length(params)::16>>,
      values,
      <<length(result_formats)::16>>,
      Enum.map(result_formats, &<<&1::16>>)
    ])
  end

  def describe(socket, kind, name), do: send_message(socket, ?D, [kind, name, 0])

  def execute(socket, portal, max_rows \\ 0),
    do: send_message(socket, ?E, [portal, 0, <<max_rows::32>>])

  def close(socket, kind, name), do: send_message(socket, ?C, [kind, name, 0])
  def sync(socket), do: send_message(socket, ?S, [])

  def sync_and_wait(socket) do
    sync(socket)
    until_ready(socket)
  end

  def terminate(socket) do
    send_message(socket, ?X, [])
    :gen_tcp.close(socket)
  end

  @doc "Rows of the first result in `messages`, as lists of text values."
  def rows(messages), do: for({:data_row, row} <- messages, do: row)
  def tags(messages), do: for({:command_complete, tag} <- messages, do: tag)
  def errors(messages), do: for({:error, fields} <- messages, do: {fields[?C], fields[?M]})
  def columns(messages), do: hd(for({:row_description, cols} <- messages, do: cols))

  defp send_message(socket, type, body) do
    body = IO.iodata_to_binary(body)
    send_raw(socket, <<type, byte_size(body) + 4::32, body::binary>>)
  end

  defp send_raw(socket, data), do: :ok = :gen_tcp.send(socket, data)

  # Stops at ReadyForQuery, or when the server closes - which it does after a FATAL.
  def until_ready(socket, acc \\ []) do
    [message] = recv_messages(socket, 1)
    acc = [message | acc]

    if match?({:ready, _}, message) or message == :closed,
      do: Enum.reverse(acc),
      else: until_ready(socket, acc)
  end

  def recv_messages(_socket, 0), do: []

  def recv_messages(socket, n) do
    case :gen_tcp.recv(socket, 5, 5_000) do
      {:ok, <<type, len::32>>} ->
        {:ok, body} = if len > 4, do: :gen_tcp.recv(socket, len - 4, 5_000), else: {:ok, <<>>}
        [decode(type, body) | recv_messages(socket, n - 1)]

      {:error, :closed} ->
        [:closed]
    end
  end

  defp decode(?R, <<code::32, _::binary>>), do: {:auth, code}

  defp decode(?S, body),
    do:
      {:parameter_status,
       body |> :binary.split(<<0>>, [:global]) |> Enum.take(2) |> List.to_tuple()}

  defp decode(?K, <<pid::32, key::32>>), do: {:backend_key, pid, key}
  defp decode(?Z, <<status>>), do: {:ready, status}
  defp decode(?C, body), do: {:command_complete, cstring(body)}
  defp decode(?I, _), do: :empty_query
  defp decode(?1, _), do: :parse_complete
  defp decode(?2, _), do: :bind_complete
  defp decode(?3, _), do: :close_complete
  defp decode(?n, _), do: :no_data
  defp decode(?s, _), do: :portal_suspended
  defp decode(?v, <<minor::32, _::binary>>), do: {:negotiate, minor}

  defp decode(?t, <<count::16, oids::binary-size(count * 4)>>),
    do: {:parameter_description, for(<<o::32 <- oids>>, do: o)}

  defp decode(?T, <<_count::16, fields::binary>>), do: {:row_description, fields(fields, [])}

  defp decode(?D, <<_count::16, cells::binary>>), do: {:data_row, cells(cells, [])}
  defp decode(?E, body), do: {:error, error_fields(body)}
  defp decode(?N, body), do: {:notice, error_fields(body)}
  defp decode(type, body), do: {:unknown, type, body}

  defp fields(<<>>, acc), do: Enum.reverse(acc)

  defp fields(binary, acc) do
    [name, rest] = :binary.split(binary, <<0>>)
    <<_table::32, _attr::16, oid::32, _size::16, _mod::32, format::16, rest::binary>> = rest
    fields(rest, [{name, oid, format} | acc])
  end

  defp cells(<<>>, acc), do: Enum.reverse(acc)
  defp cells(<<-1::32-signed, rest::binary>>, acc), do: cells(rest, [nil | acc])

  defp cells(<<len::32, value::binary-size(len), rest::binary>>, acc),
    do: cells(rest, [value | acc])

  defp error_fields(body) do
    body
    |> :binary.split(<<0>>, [:global])
    |> Enum.reject(&(&1 == ""))
    |> Map.new(fn <<code, value::binary>> -> {code, value} end)
  end

  defp cstring(body), do: body |> :binary.split(<<0>>) |> hd()
end
