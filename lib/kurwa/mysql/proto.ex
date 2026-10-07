defmodule Kurwa.Mysql.Proto do
  @moduledoc """
  The MySQL client/server protocol, as a MySQL 8 server speaks it, and as
  MariaDB clients accept it.

  Every packet is a 3-byte little-endian length, a 1-byte sequence id, and a
  payload; the sequence starts at 0 with each command and goes up by one for
  every packet either side sends. Integers are little-endian throughout, and
  "length-encoded" integers and strings (`lenenc`) are how variable-length
  fields are carried.

  Only the protocol 4.1 forms are here, with `CLIENT_DEPRECATE_EOF` honoured
  when the client asks for it: result sets then end with an OK packet instead
  of an EOF one.
  """

  import Bitwise

  # Capability flags.
  @long_password 0x1
  @found_rows 0x2
  @long_flag 0x4
  @connect_with_db 0x8
  @protocol_41 0x200
  @transactions 0x2000
  @secure_connection 0x8000
  @multi_statements 0x10000
  @multi_results 0x20000
  @ps_multi_results 0x40000
  @plugin_auth 0x80000
  @connect_attrs 0x100000
  @plugin_auth_lenenc 0x200000
  @deprecate_eof 0x1000000

  @server_capabilities @long_password ||| @found_rows ||| @long_flag ||| @connect_with_db |||
                         @protocol_41 ||| @transactions ||| @secure_connection |||
                         @multi_statements ||| @multi_results ||| @ps_multi_results |||
                         @plugin_auth ||| @connect_attrs ||| @plugin_auth_lenenc |||
                         @deprecate_eof

  # Status flags.
  @status_in_trans 0x1
  @status_autocommit 0x2
  @status_more_results 0x8

  @utf8mb4 255

  def capabilities, do: @server_capabilities
  def deprecate_eof?(client_caps), do: (client_caps &&& @deprecate_eof) != 0
  def multi_statements?(client_caps), do: (client_caps &&& @multi_statements) != 0

  @doc "Status flags for an OK or EOF packet."
  def status(in_transaction?, more_results?) do
    @status_autocommit ||| if(in_transaction?, do: @status_in_trans, else: 0) |||
      if(more_results?, do: @status_more_results, else: 0)
  end

  # ------------------------------------------------------------------ framing

  @doc "Splits one packet off the front of `buffer`: `{:ok, seq, payload, rest}` or `:more`."
  def packet(<<len::24-little, seq, rest::binary>>) when byte_size(rest) >= len do
    <<payload::binary-size(^len), rest::binary>> = rest
    {:ok, seq, payload, rest}
  end

  def packet(_partial), do: :more

  @doc "Frames `payloads` as packets numbered from `seq`. Returns `{iodata, next_seq}`."
  def frame(payloads, seq) do
    Enum.map_reduce(payloads, seq, fn payload, seq ->
      payload = IO.iodata_to_binary(payload)
      {[<<byte_size(payload)::24-little, rem(seq, 256)>>, payload], seq + 1}
    end)
  end

  # ------------------------------------------------------------- connection

  @doc "Initial Handshake Packet, protocol version 10."
  def handshake(version, connection_id, nonce, plugin) do
    <<part1::binary-size(8), part2::binary-size(12)>> = nonce
    caps = @server_capabilities

    [
      10,
      version,
      0,
      <<connection_id::32-little>>,
      part1,
      0,
      <<caps &&& 0xFFFF::16-little>>,
      @utf8mb4,
      <<@status_autocommit::16-little>>,
      <<caps >>> 16::16-little>>,
      21,
      :binary.copy(<<0>>, 10),
      part2,
      0,
      plugin,
      0
    ]
  end

  @doc """
  Parses HandshakeResponse41 into a map: `caps`, `user`, `auth`, `database`,
  `plugin`. Returns `{:ok, map}` or `{:error, :ssl}` for an SSLRequest.
  """
  def handshake_response(<<caps::32-little, _max::32-little, _charset, _filler::binary-size(23)>>)
      when (caps &&& 0x800) != 0,
      do: {:error, :ssl}

  def handshake_response(
        <<caps::32-little, _max::32-little, _charset, _filler::binary-size(23), rest::binary>>
      ) do
    {user, rest} = cstring(rest)

    {auth, rest} =
      cond do
        (caps &&& @plugin_auth_lenenc) != 0 -> read_lenenc_string(rest)
        (caps &&& @secure_connection) != 0 -> one_byte_string(rest)
        true -> cstring(rest)
      end

    {database, rest} = if (caps &&& @connect_with_db) != 0, do: cstring(rest), else: {nil, rest}
    {plugin, _rest} = if (caps &&& @plugin_auth) != 0, do: cstring(rest), else: {nil, rest}

    {:ok,
     %{
       caps: caps,
       user: user,
       auth: auth,
       database: if(database in [nil, ""], do: nil, else: database),
       plugin: plugin
     }}
  rescue
    _ in [MatchError, ArgumentError] -> {:error, :malformed}
  end

  @doc "AuthSwitchRequest: use `plugin`, with this nonce."
  def auth_switch(plugin, nonce), do: [0xFE, plugin, 0, nonce, 0]

  @doc "AuthMoreData: caching_sha2_password's \"fast auth success\"."
  def fast_auth_success, do: <<0x01, 0x03>>

  # ------------------------------------------------------------- responses

  @doc "OK packet."
  def ok(affected \\ 0, status, warnings \\ 0, header \\ 0x00) do
    [header, lenenc_int(affected), lenenc_int(0), <<status::16-little, warnings::16-little>>]
  end

  @doc "EOF packet."
  def eof(status, warnings \\ 0), do: [0xFE, <<warnings::16-little, status::16-little>>]

  @doc "ERR packet."
  def err(code, sqlstate, message) do
    Kurwa.Metrics.error(:mysql)
    [0xFF, <<code::16-little>>, ?#, sqlstate, message]
  end

  @doc "Column Definition 41 for `{name, type}`."
  def column(name, type) do
    {code, charset, length, flags} = Kurwa.Mysql.Types.column(type)

    [
      lenenc_string("def"),
      lenenc_string("kurwadb"),
      lenenc_string(""),
      lenenc_string(""),
      lenenc_string(name),
      lenenc_string(name),
      0x0C,
      <<charset::16-little, length::32-little, code, flags::16-little, 0, 0::16>>
    ]
  end

  @doc "A text-protocol row: every value as a string, NULL as 0xFB."
  def text_row(values) do
    Enum.map(values, fn
      nil -> 0xFB
      value -> lenenc_string(value)
    end)
  end

  @doc "A binary-protocol row: a null bitmap offset by two bits, then the values."
  def binary_row(encoded) do
    bitmap_size = div(length(encoded) + 7 + 2, 8)

    bitmap =
      encoded
      |> Enum.with_index(2)
      |> Enum.reduce(0, fn
        {nil, i}, acc -> acc ||| 1 <<< i
        {_, _}, acc -> acc
      end)

    [0x00, <<bitmap::size(bitmap_size * 8)-little>>, Enum.reject(encoded, &is_nil/1)]
  end

  @doc "COM_STMT_PREPARE_OK."
  def prepare_ok(id, columns, params) do
    [0x00, <<id::32-little, columns::16-little, params::16-little, 0, 0::16>>]
  end

  # ---------------------------------------------------------------- execute

  @doc """
  Parses a COM_STMT_EXECUTE body (after the command byte). `types` are the
  parameter types from the previous execute of the statement, used when the
  client does not send them again. Returns `{:ok, id, values, types}`.
  """
  def execute(
        <<id::32-little, _flags, _iterations::32-little, rest::binary>>,
        count,
        previous_types
      ) do
    if count == 0 do
      {:ok, id, [], []}
    else
      bitmap_size = div(count + 7, 8)
      <<bitmap::binary-size(^bitmap_size), bound, rest::binary>> = rest

      {types, rest} =
        if bound == 1 do
          <<raw::binary-size(^count * 2), rest::binary>> = rest
          {for(<<type, flags <- raw>>, do: {type, flags}), rest}
        else
          {previous_types, rest}
        end

      nulls =
        for i <- 0..(count - 1), do: (:binary.at(bitmap, div(i, 8)) >>> rem(i, 8) &&& 1) == 1

      {values, _rest} = values(Enum.zip(types, nulls), rest, [])
      {:ok, id, values, types}
    end
  rescue
    _ in [MatchError, ArgumentError, FunctionClauseError] -> {:error, :malformed}
  end

  defp values([], rest, acc), do: {Enum.reverse(acc), rest}
  defp values([{_type, true} | more], rest, acc), do: values(more, rest, [nil | acc])

  defp values([{{type, flags}, false} | more], rest, acc) do
    {value, rest} = Kurwa.Mysql.Types.decode(type, flags, rest)
    values(more, rest, [value | acc])
  end

  # ---------------------------------------------------------------- lenenc

  @doc "A length-encoded integer."
  def lenenc_int(n) when n < 251, do: <<n>>
  def lenenc_int(n) when n < 0x10000, do: <<0xFC, n::16-little>>
  def lenenc_int(n) when n < 0x1000000, do: <<0xFD, n::24-little>>
  def lenenc_int(n), do: <<0xFE, n::64-little>>

  @doc "A length-encoded string."
  def lenenc_string(s) when is_binary(s), do: [lenenc_int(byte_size(s)), s]

  @doc "Reads a length-encoded integer: `{n, rest}`."
  def read_lenenc(<<0xFC, n::16-little, rest::binary>>), do: {n, rest}
  def read_lenenc(<<0xFD, n::24-little, rest::binary>>), do: {n, rest}
  def read_lenenc(<<0xFE, n::64-little, rest::binary>>), do: {n, rest}
  def read_lenenc(<<n, rest::binary>>) when n < 251, do: {n, rest}

  defp read_lenenc_string(binary) do
    {len, rest} = read_lenenc(binary)
    <<s::binary-size(^len), rest::binary>> = rest
    {s, rest}
  end

  defp one_byte_string(<<len, s::binary-size(len), rest::binary>>), do: {s, rest}

  def cstring(binary) do
    case :binary.split(binary, <<0>>) do
      [s, rest] -> {s, rest}
      [s] -> {s, <<>>}
    end
  end
end
