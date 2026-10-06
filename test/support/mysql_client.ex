defmodule Kurwa.MysqlClient do
  @moduledoc """
  The smallest MySQL client that can test `Kurwa.Mysql.Server`: the
  handshake with caching_sha2_password or mysql_native_password, COM_QUERY,
  and prepare/execute, with result sets decoded into maps. Written from the
  protocol documentation, not from the server, so the two check each other.
  """

  import Bitwise

  @caps 0x1 ||| 0x8 ||| 0x200 ||| 0x2000 ||| 0x8000 ||| 0x10000 ||| 0x20000 ||| 0x80000 |||
          0x200000 ||| 0x1000000

  def connect(port, opts \\ []) do
    {:ok, s} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)
    {0, hello} = recv(s)
    <<10, rest::binary>> = hello
    [_version, rest] = :binary.split(rest, <<0>>)

    <<_id::32-little, p1::binary-size(8), 0, _caps_lo::16, _cs, _st::16, _caps_hi::16, _len,
      _r::binary-size(10), p2::binary-size(12), 0, rest::binary>> = rest

    [plugin | _] = :binary.split(rest, <<0>>)
    nonce = p1 <> p2

    plugin = Keyword.get(opts, :plugin, plugin)
    password = Keyword.get(opts, :password, "")
    caps = if Keyword.get(opts, :deprecate_eof, true), do: @caps, else: @caps &&& bnot(0x1000000)
    auth = scramble(plugin, password, nonce)

    response = [
      <<caps::32-little, 16_777_216::32-little, 255>>,
      :binary.copy(<<0>>, 23),
      Keyword.get(opts, :user, "test"),
      0,
      byte_size(auth),
      auth,
      Keyword.get(opts, :database, "kurwadb"),
      0,
      plugin,
      0
    ]

    send_packet(s, 1, response)
    Process.put({__MODULE__, :eof}, (caps &&& 0x1000000) == 0)
    {s, %{caps: caps, auth: auth_result(s, password)}}
  end

  # Reads the server's packets until it settles: OK, ERR, or a switch to answer.
  defp auth_result(s, password) do
    case recv(s) do
      {_, <<0x00, _::binary>>} ->
        :ok

      {_, <<0xFF, code::16-little, _::binary>>} ->
        {:error, code}

      {_, <<0x01, 0x03>>} ->
        auth_result(s, password)

      {seq, <<0xFE, rest::binary>>} ->
        [plugin, nonce] = :binary.split(rest, <<0>>)
        nonce = String.trim_trailing(nonce, <<0>>)
        send_packet(s, seq + 1, scramble(plugin, password, nonce))
        auth_result(s, password)
    end
  end

  defp scramble(_plugin, "", _nonce), do: ""

  defp scramble("caching_sha2_password", password, nonce) do
    d1 = :crypto.hash(:sha256, password)
    :crypto.exor(d1, :crypto.hash(:sha256, :crypto.hash(:sha256, d1) <> nonce))
  end

  defp scramble("mysql_native_password", password, nonce) do
    s1 = :crypto.hash(:sha, password)
    :crypto.exor(s1, :crypto.hash(:sha, nonce <> :crypto.hash(:sha, s1)))
  end

  @doc "COM_QUERY. Returns a list of results: {:ok, affected, warnings} | {:rows, names, rows} | {:error, code, message}."
  def query(s, sql) do
    send_packet(s, 0, [0x03, sql])
    results(s, :text, [])
  end

  def prepare(s, sql) do
    send_packet(s, 0, [0x16, sql])

    case recv(s) do
      {_, <<0x00, id::32-little, columns::16-little, params::16-little, _::binary>>} ->
        if params > 0, do: for(_ <- 1..params, do: recv(s))
        if columns > 0, do: for(_ <- 1..columns, do: recv(s))
        {:ok, id, params, columns}

      {_, <<0xFF, code::16-little, _::binary>>} ->
        {:error, code}
    end
  end

  @doc "COM_STMT_EXECUTE with string parameters."
  def execute(s, id, params) do
    count = length(params)

    body =
      if count == 0 do
        []
      else
        bitmap =
          Enum.with_index(params)
          |> Enum.reduce(0, fn {v, i}, acc -> if v == nil, do: acc ||| 1 <<< i, else: acc end)

        types = for _ <- params, do: <<0xFD, 0>>
        values = for v <- params, v != nil, do: [lenenc(byte_size(v)), v]
        [<<bitmap::size(div(count + 7, 8) * 8)-little>>, 1, types, values]
      end

    send_packet(s, 0, [0x17, <<id::32-little, 0, 1::32-little>>, body])
    results(s, :binary, [])
  end

  defp results(s, protocol, acc) do
    case recv(s) do
      {_, <<0x00, rest::binary>>} ->
        {affected, rest} = read_lenenc(rest)
        {_id, <<status::16-little, warnings::16-little, _::binary>>} = read_lenenc(rest)
        more(s, protocol, [{:ok, affected, warnings} | acc], status)

      {_, <<0xFF, code::16-little, ?#, _state::binary-size(5), message::binary>>} ->
        Enum.reverse([{:error, code, message} | acc])

      {_, first} ->
        {count, _} = read_lenenc(first)
        columns = for _ <- 1..count, do: column(elem(recv(s), 1))
        # Without CLIENT_DEPRECATE_EOF an EOF separates the columns from the rows.
        if Process.get({__MODULE__, :eof}), do: {_, <<0xFE, _::binary>>} = recv(s)
        {rows, status} = rows(s, protocol, columns, [])
        more(s, protocol, [{:rows, Enum.map(columns, &elem(&1, 0)), rows} | acc], status)
    end
  end

  defp more(s, protocol, acc, status) do
    if (status &&& 0x08) != 0, do: results(s, protocol, acc), else: Enum.reverse(acc)
  end

  defp rows(s, protocol, columns, acc) do
    case recv(s) do
      {_, <<0xFE, rest::binary>>} when byte_size(rest) < 9 ->
        {Enum.reverse(acc), terminator_status(rest)}

      {_, row} ->
        rows(s, protocol, columns, [row(protocol, row, columns) | acc])
    end
  end

  # An OK with 0xFE header (deprecate EOF), or a classic EOF.
  defp terminator_status(<<_warnings::16-little, status::16-little>>), do: status

  defp terminator_status(rest) do
    {_, rest} = read_lenenc(rest)
    {_, <<status::16-little, _::binary>>} = read_lenenc(rest)
    status
  end

  defp row(:text, row, columns), do: text_values(row, length(columns), [])

  defp row(:binary, <<0x00, rest::binary>>, columns) do
    size = div(length(columns) + 7 + 2, 8)
    <<bitmap::size(^size * 8)-little, rest::binary>> = rest

    {values, _} =
      columns
      |> Enum.with_index(2)
      |> Enum.map_reduce(rest, fn {{_name, type}, i}, rest ->
        if (bitmap >>> i &&& 1) == 1, do: {nil, rest}, else: binary_value(type, rest)
      end)

    values
  end

  defp binary_value(0x01, <<v::8-signed, rest::binary>>), do: {v, rest}
  defp binary_value(0x08, <<v::64-little-signed, rest::binary>>), do: {v, rest}
  defp binary_value(0x03, <<v::32-little-signed, rest::binary>>), do: {v, rest}

  defp binary_value(_string, rest) do
    {len, rest} = read_lenenc(rest)
    <<v::binary-size(^len), rest::binary>> = rest
    {v, rest}
  end

  defp text_values(_rest, 0, acc), do: Enum.reverse(acc)
  defp text_values(<<0xFB, rest::binary>>, n, acc), do: text_values(rest, n - 1, [nil | acc])

  defp text_values(rest, n, acc) do
    {len, rest} = read_lenenc(rest)
    <<v::binary-size(^len), rest::binary>> = rest
    text_values(rest, n - 1, [v | acc])
  end

  defp column(packet) do
    {_, rest} = lenstr(packet)
    {_, rest} = lenstr(rest)
    {_, rest} = lenstr(rest)
    {_, rest} = lenstr(rest)
    {name, rest} = lenstr(rest)
    {_, <<0x0C, _cs::16, _len::32, type, _::binary>>} = lenstr(rest)
    {name, type}
  end

  defp lenstr(binary) do
    {len, rest} = read_lenenc(binary)
    <<s::binary-size(^len), rest::binary>> = rest
    {s, rest}
  end

  defp read_lenenc(<<0xFC, n::16-little, rest::binary>>), do: {n, rest}
  defp read_lenenc(<<0xFD, n::24-little, rest::binary>>), do: {n, rest}
  defp read_lenenc(<<n, rest::binary>>) when n < 251, do: {n, rest}

  defp lenenc(n) when n < 251, do: <<n>>
  defp lenenc(n), do: <<0xFC, n::16-little>>

  defp send_packet(s, seq, payload) do
    payload = IO.iodata_to_binary(payload)
    :ok = :gen_tcp.send(s, <<byte_size(payload)::24-little, seq, payload::binary>>)
  end

  defp recv(s) do
    {:ok, <<len::24-little, seq>>} = :gen_tcp.recv(s, 4, 5_000)
    {:ok, payload} = if len > 0, do: :gen_tcp.recv(s, len, 5_000), else: {:ok, <<>>}
    {seq, payload}
  end
end
