defmodule Kurwa.TdsClient do
  @moduledoc """
  The smallest SQL Server client that can test `Kurwa.Mssql.Server`, without
  encryption: PRELOGIN, LOGIN7, SQL batches and RPCs, and the token stream of
  replies decoded into a list. Written from [MS-TDS], not from the server.
  TLS is left to the real clients in test/drivers and the sqlcmd test.
  """

  import Bitwise

  def connect(port, opts \\ []) do
    {:ok, s} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)

    # PRELOGIN: version, encryption NOT_SUP, terminator.
    prelogin = <<0x00, 11::16, 6::16, 0x01, 17::16, 1::16, 0xFF, 16, 0, 0, 0, 0::16, 0x02>>
    send_message(s, 0x12, prelogin)
    {0x04, response} = recv_message(s)
    encryption = prelogin_encryption(response)

    send_message(s, 0x10, login7(opts))
    {0x04, tokens} = recv_message(s)
    {s, %{encryption: encryption, login: decode(tokens)}}
  end

  def batch(s, sql) do
    send_message(s, 0x01, [all_headers(), ucs2(sql)])
    {0x04, tokens} = recv_message(s)
    decode(tokens)
  end

  @doc "sp_executesql with NVARCHAR parameters: [{name, value}]."
  def executesql(s, sql, params) do
    decl = params |> Enum.map(fn {name, _} -> "@#{name} nvarchar(4000)" end) |> Enum.join(",")

    body = [
      all_headers(),
      <<0xFFFF::16, 10::16-little, 0::16>>,
      nvarchar_param("", sql),
      nvarchar_param("", decl),
      Enum.map(params, fn {name, value} -> nvarchar_param("@" <> name, value) end)
    ]

    send_message(s, 0x03, body)
    {0x04, tokens} = recv_message(s)
    decode(tokens)
  end

  @doc "sp_prepexec, then sp_execute with the handle it returned."
  def prepexec(s, sql, name, value) do
    body = [
      all_headers(),
      <<0xFFFF::16, 13::16-little, 0::16>>,
      [0, 0x01, 0x26, 4, 0],
      nvarchar_param("", "@#{name} nvarchar(4000)"),
      nvarchar_param("", sql),
      nvarchar_param("@" <> name, value)
    ]

    send_message(s, 0x03, body)
    {0x04, tokens} = recv_message(s)
    decode(tokens)
  end

  def execute(s, handle, name, value) do
    body = [
      all_headers(),
      <<0xFFFF::16, 12::16-little, 0::16>>,
      [0, 0x00, 0x26, 4, 4, <<handle::32-little>>],
      nvarchar_param("@" <> name, value)
    ]

    send_message(s, 0x03, body)
    {0x04, tokens} = recv_message(s)
    decode(tokens)
  end

  @doc "A transaction-manager request: :begin, :commit or :rollback."
  def transaction(s, kind) do
    request =
      case kind do
        :begin -> <<5::16-little, 0, 0>>
        :commit -> <<7::16-little, 0, 0>>
        :rollback -> <<8::16-little, 0, 0>>
      end

    send_message(s, 0x0E, [all_headers(), request])
    {0x04, tokens} = recv_message(s)
    decode(tokens)
  end

  def rows(tokens), do: for({:row, values} <- tokens, do: values)
  def columns(tokens), do: hd(for({:colmetadata, cols} <- tokens, do: cols))

  def counts(tokens),
    do: for({:done, _kind, status, count} <- tokens, (status &&& 0x10) != 0, do: count)

  def errors(tokens), do: for({:error, number, message} <- tokens, do: {number, message})
  def infos(tokens), do: for({:info, number, message} <- tokens, do: {number, message})

  # ------------------------------------------------------------- encoding

  defp login7(opts) do
    user = Keyword.get(opts, :user, "tester")
    password = Keyword.get(opts, :password, "")
    database = Keyword.get(opts, :database, "kurwadb")

    strings = [
      {"host", :host},
      {user, :user},
      {scramble(password), :password},
      {"tds-client", :app},
      {"", :server},
      {"", :unused},
      {"", :library},
      {"", :language},
      {database, :database}
    ]

    fixed = 94

    {offsets, data, _} =
      Enum.reduce(strings, {[], [], fixed}, fn {value, kind}, {offs, data, at} ->
        bytes = if kind == :password, do: value, else: ucs2(value)
        chars = div(byte_size(bytes), 2)
        entry = <<at::16-little, chars::16-little>>

        entry =
          if kind == :database,
            do: [
              entry,
              :binary.copy(<<0>>, 6),
              <<at + byte_size(bytes)::16-little, 0::16, at + byte_size(bytes)::16-little, 0::16,
                at + byte_size(bytes)::16-little, 0::16, 0::32>>
            ],
            else: entry

        {[offs, entry], [data, bytes], at + byte_size(bytes)}
      end)

    body = IO.iodata_to_binary([offsets, data])
    total = 36 + byte_size(body)

    header =
      <<total::32-little, 0x74000004::32-little, 4096::32-little, 7::32, 0::32, 0::32, 0xE0, 0x03,
        0, 0, 0::32, 0x409::32-little>>

    [header, body]
  end

  defp scramble(password) do
    for <<b <- ucs2(password)>>,
      into: <<>>,
      do: <<bxor((b &&& 0x0F) <<< 4 ||| (b &&& 0xF0) >>> 4, 0xA5)>>
  end

  defp all_headers, do: <<22::32-little, 18::32-little, 2::16-little, 0::64, 1::32-little>>

  defp nvarchar_param(name, value) do
    v = ucs2(value)

    [
      byte_size(ucs2(name)) |> div(2),
      ucs2(name),
      0,
      0xE7,
      <<8000::16-little>>,
      <<0x09, 0x04, 0xD0, 0x00, 0x34>>,
      <<byte_size(v)::16-little>>,
      v
    ]
  end

  defp ucs2(s), do: :unicode.characters_to_binary(s, :utf8, {:utf16, :little})
  defp utf8(b), do: :unicode.characters_to_binary(b, {:utf16, :little}, :utf8)

  defp send_message(s, type, payload) do
    payload = IO.iodata_to_binary(payload)

    :ok =
      :gen_tcp.send(s, <<type, 0x01, byte_size(payload) + 8::16, 0::16, 1, 0, payload::binary>>)
  end

  defp recv_message(s, acc \\ []) do
    {:ok, <<type, status, len::16, _::32>>} = :gen_tcp.recv(s, 8, 5_000)
    {:ok, body} = :gen_tcp.recv(s, len - 8, 5_000)

    if (status &&& 1) == 1,
      do: {type, IO.iodata_to_binary(Enum.reverse([body | acc]))},
      else: recv_message(s, [body | acc])
  end

  defp prelogin_encryption(response) do
    options =
      for <<token, offset::16, len::16 <- binary_part(response, 0, 25)>>, token != 0xFF,
        do: {token, offset, len}

    {_, offset, _} = List.keyfind(options, 1, 0)
    :binary.at(response, offset)
  end

  # ------------------------------------------------------------- decoding

  def decode(tokens), do: decode(tokens, nil, [])

  defp decode(<<>>, _cols, acc), do: Enum.reverse(acc)

  defp decode(<<0x81, count::16-little, rest::binary>>, _cols, acc) do
    {cols, rest} =
      Enum.map_reduce(1..count, rest, fn _, rest ->
        <<_user::32, _flags::16, type, rest::binary>> = rest

        {type, rest} =
          case type do
            0xE7 ->
              <<_max::16, _coll::binary-size(5), rest::binary>> = rest
              {:text, rest}

            0x68 ->
              <<_len, rest::binary>> = rest
              {:bool, rest}

            0x26 ->
              <<_len, rest::binary>> = rest
              {:int, rest}
          end

        <<chars, name::binary-size(chars * 2), rest::binary>> = rest
        {{utf8(name), type}, rest}
      end)

    decode(rest, cols, [{:colmetadata, Enum.map(cols, &elem(&1, 0))} | acc])
  end

  defp decode(<<0xD1, rest::binary>>, cols, acc) do
    {values, rest} = Enum.map_reduce(cols, rest, fn {_, type}, rest -> value(type, rest) end)
    decode(rest, cols, [{:row, values} | acc])
  end

  defp decode(<<t, status::16-little, _cmd::16, count::64-little, rest::binary>>, cols, acc)
       when t in [0xFD, 0xFE, 0xFF],
       do: decode(rest, cols, [{:done, t, status, count} | acc])

  defp decode(<<t, len::16-little, body::binary-size(len), rest::binary>>, cols, acc)
       when t in [0xAA, 0xAB] do
    <<number::32-little, _state, _class, chars::16-little, message::binary-size(chars * 2),
      _::binary>> = body

    decode(rest, cols, [{if(t == 0xAA, do: :error, else: :info), number, utf8(message)} | acc])
  end

  defp decode(<<0xE3, len::16-little, body::binary-size(len), rest::binary>>, cols, acc),
    do: decode(rest, cols, [{:envchange, :binary.first(body)} | acc])

  defp decode(<<0xAD, len::16-little, _::binary-size(len), rest::binary>>, cols, acc),
    do: decode(rest, cols, [:loginack | acc])

  defp decode(<<0x79, status::32-little-signed, rest::binary>>, cols, acc),
    do: decode(rest, cols, [{:return_status, status} | acc])

  defp decode(
         <<0xAC, _ord::16, chars, _name::binary-size(chars * 2), _status, _user::32, _flags::16,
           0x26, 4, 4, v::32-little-signed, rest::binary>>,
         cols,
         acc
       ),
       do: decode(rest, cols, [{:return_value, v} | acc])

  defp decode(<<0xAE, 0xFF, rest::binary>>, cols, acc), do: decode(rest, cols, acc)

  defp value(:text, <<0xFFFF::16, rest::binary>>), do: {nil, rest}
  defp value(:text, <<len::16-little, v::binary-size(len), rest::binary>>), do: {utf8(v), rest}
  defp value(_, <<0, rest::binary>>), do: {nil, rest}
  defp value(:bool, <<1, v, rest::binary>>), do: {v == 1, rest}
  defp value(:int, <<8, v::64-little-signed, rest::binary>>), do: {v, rest}
  defp value(:int, <<4, v::32-little-signed, rest::binary>>), do: {v, rest}
end
