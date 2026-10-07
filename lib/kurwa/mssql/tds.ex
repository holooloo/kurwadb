defmodule Kurwa.Mssql.Tds do
  @moduledoc """
  TDS, the protocol of Microsoft SQL Server: framing, the login exchange, the
  token stream of responses, and RPC parameters.

  A TDS message travels in packets: an 8-byte header - type, status (bit 0 is
  end-of-message), a big-endian length that counts the header, a SPID, a
  packet id - and a payload. Everything inside is little-endian, and text is
  UTF-16LE (UCS-2 in the spec's words).

  This is TDS 7.4, what SQL Server 2012 and later speak, and what every
  current driver offers. References are to [MS-TDS] by section name.
  """

  import Bitwise

  # Packet types.
  @sql_batch 0x01
  @rpc 0x03
  @reply 0x04
  @attention 0x06
  @transaction_manager 0x0E
  @login7 0x10
  @prelogin 0x12

  def packet_type(:sql_batch), do: @sql_batch
  def packet_type(:rpc), do: @rpc
  def packet_type(:reply), do: @reply
  def packet_type(:attention), do: @attention
  def packet_type(:transaction_manager), do: @transaction_manager
  def packet_type(:login7), do: @login7
  def packet_type(:prelogin), do: @prelogin

  def type_name(@sql_batch), do: :sql_batch
  def type_name(@rpc), do: :rpc
  def type_name(@attention), do: :attention
  def type_name(@transaction_manager), do: :transaction_manager
  def type_name(@login7), do: :login7
  def type_name(@prelogin), do: :prelogin
  def type_name(other), do: {:unknown, other}

  @version 0x74000004

  # ----------------------------------------------------------------- packets

  @doc """
  One whole message from the front of `buffer`, joining packets up to the one
  with end-of-message set: `{:ok, type, payload, rest}`, `:more`, or
  `{:error, reason}`.
  """
  def message(buffer), do: message(buffer, nil, [])

  defp message(<<type, status, len::16, _spid::16, _id, _window, rest::binary>>, first, acc)
       when len >= 8 and byte_size(rest) >= len - 8 do
    body_len = len - 8
    <<body::binary-size(^body_len), rest::binary>> = rest

    cond do
      first != nil and type != first -> {:error, :mixed_packet_types}
      (status &&& 0x01) == 1 -> {:ok, type, IO.iodata_to_binary(Enum.reverse([body | acc])), rest}
      true -> message(rest, type, [body | acc])
    end
  end

  defp message(<<_type, _status, len::16, _::binary>>, _first, _acc) when len < 8,
    do: {:error, :bad_length}

  defp message(_partial, _first, _acc), do: :more

  @doc "Frames `payload` as packets of `type`, each at most `size` bytes."
  def packets(type, payload, size) do
    payload = IO.iodata_to_binary(payload)
    chunk = size - 8
    chunks = for <<c::binary-size(^chunk) <- payload>>, do: c

    tail =
      binary_part(payload, length(chunks) * chunk, byte_size(payload) - length(chunks) * chunk)

    chunks = if tail == "" and chunks != [], do: chunks, else: chunks ++ [tail]
    last = length(chunks) - 1

    chunks
    |> Enum.with_index()
    |> Enum.map(fn {c, i} ->
      status = if i == last, do: 0x01, else: 0x00
      <<type, status, byte_size(c) + 8::16, 0::16, rem(i + 1, 256), 0, c::binary>>
    end)
  end

  # ------------------------------------------------------------------ text

  def ucs2(s) when is_binary(s), do: :unicode.characters_to_binary(s, :utf8, {:utf16, :little})

  def utf8(ucs2) do
    case :unicode.characters_to_binary(ucs2, {:utf16, :little}, :utf8) do
      s when is_binary(s) -> s
      _ -> raise ArgumentError, "invalid UTF-16"
    end
  end

  # B_VARCHAR: a byte count of characters; US_VARCHAR: two bytes of them.
  def b_varchar(s), do: [byte_size(ucs2(s)) |> div(2), ucs2(s)]
  def us_varchar(s), do: [<<byte_size(ucs2(s)) |> div(2)::16-little>>, ucs2(s)]

  # --------------------------------------------------------------- prelogin

  @doc "Parses a PRELOGIN payload into a map of option => value."
  def prelogin(payload), do: prelogin_options(payload, payload, %{})

  defp prelogin_options(<<0xFF, _::binary>>, _all, acc), do: acc

  defp prelogin_options(<<token, offset::16, len::16, rest::binary>>, all, acc) do
    value = if offset + len <= byte_size(all), do: binary_part(all, offset, len), else: <<>>
    prelogin_options(rest, all, Map.put(acc, token, value))
  end

  defp prelogin_options(_, _all, acc), do: acc

  @doc "The client's encryption option: :off, :on, :not_supported or :required."
  def encryption(options) do
    case Map.get(options, 0x01) do
      <<0x00>> -> :off
      <<0x01>> -> :on
      <<0x02>> -> :not_supported
      <<0x03>> -> :required
      _ -> :not_supported
    end
  end

  @doc "A PRELOGIN response: version, encryption, instance, thread id, MARS off."
  def prelogin_response(encryption) do
    enc =
      case encryption do
        :off -> 0x00
        :on -> 0x01
        :not_supported -> 0x02
        :required -> 0x03
      end

    # SQL Server 2022, 16.0
    options = [
      {0x00, <<16, 0, 0x10, 0x00, 0::16>>},
      {0x01, <<enc>>},
      {0x02, <<0>>},
      {0x03, <<>>},
      {0x04, <<0>>}
    ]

    header_len = length(options) * 5 + 1

    {headers, data, _} =
      Enum.reduce(options, {[], [], header_len}, fn {token, value}, {hs, ds, offset} ->
        {[hs, <<token, offset::16, byte_size(value)::16>>], [ds, value],
         offset + byte_size(value)}
      end)

    [headers, 0xFF, data]
  end

  # ----------------------------------------------------------------- login7

  @doc "Parses LOGIN7: user, password, database, app, packet size, TDS version, flags."
  def login7(
        <<_len::32-little, version::32-little, packet_size::32-little, _prog::32, _pid::32,
          _conn::32, _flags1, flags2, _type_flags, flags3, _tz::32, _lcid::32,
          offsets::binary-size(58), _::binary>> = all
      ) do
    <<_host::32, user_off::16-little, user_len::16-little, pass_off::16-little,
      pass_len::16-little, app_off::16-little, app_len::16-little, _server::32,
      ext_off::16-little, ext_len::16-little, _clt_int::32, _language::32, db_off::16-little,
      db_len::16-little, _client_id::binary-size(6), _sspi_off::16-little, sspi_len::16-little,
      _rest::binary>> = offsets

    text = fn off, chars -> all |> binary_part(off, chars * 2) |> utf8() end

    password =
      all
      |> binary_part(pass_off, pass_len * 2)
      |> :binary.bin_to_list()
      |> Enum.map(fn b ->
        b |> bxor(0xA5) |> then(&((&1 &&& 0x0F) <<< 4 ||| (&1 &&& 0xF0) >>> 4))
      end)
      |> :erlang.list_to_binary()
      |> utf8()

    features =
      if (flags3 &&& 0x10) != 0 and ext_len >= 4 do
        <<feature_off::32-little>> = binary_part(all, ext_off, 4)
        feature_ids(binary_part(all, feature_off, byte_size(all) - feature_off), [])
      else
        []
      end

    {:ok,
     %{
       version: version,
       packet_size: packet_size,
       user: text.(user_off, user_len),
       password: password,
       app: text.(app_off, app_len),
       database: text.(db_off, db_len),
       integrated: (flags2 &&& 0x80) != 0 or sspi_len > 0,
       features: features
     }}
  rescue
    _ in [MatchError, ArgumentError, FunctionClauseError] -> {:error, :malformed}
  end

  def login7(_), do: {:error, :malformed}

  defp feature_ids(<<0xFF, _::binary>>, acc), do: Enum.reverse(acc)

  defp feature_ids(<<id, len::32-little, _data::binary-size(len), rest::binary>>, acc),
    do: feature_ids(rest, [id | acc])

  defp feature_ids(_, acc), do: Enum.reverse(acc)

  # ----------------------------------------------------------------- tokens

  @doc "LOGINACK: TDS 7.4, as SQL Server 16.0."
  def loginack do
    body = [1, <<@version::32>>, b_varchar("Microsoft SQL Server"), <<16, 0, 0x10, 0x00>>]
    token(0xAD, body)
  end

  @doc "ENVCHANGE for a varchar-valued change: database (1), language (2), packet size (4)."
  def envchange(type, new, old) when type in [1, 2, 4],
    do: token(0xE3, [type, b_varchar(new), b_varchar(old)])

  # Collation (7): Latin1_General_CI_AS, the SQL Server default.
  def envchange(:collation), do: token(0xE3, [7, 5, collation(), 0])

  # Transactions begin (8), commit (9), rollback (10): an 8-byte descriptor.
  def envchange(:begin, descriptor), do: token(0xE3, [8, 8, <<descriptor::64-little>>, 0])
  def envchange(:commit, descriptor), do: token(0xE3, [9, 0, 8, <<descriptor::64-little>>])
  def envchange(:rollback, descriptor), do: token(0xE3, [10, 0, 8, <<descriptor::64-little>>])

  def collation, do: <<0x09, 0x04, 0xD0, 0x00, 0x34>>

  @doc "FEATUREEXTACK with no features acknowledged."
  def featureextack, do: [0xAE, 0xFF]

  @doc "ERROR or INFO (`kind`), with SQL Server's message number, state and class."
  def notice(kind, number, class, text, state \\ 1) do
    body = [
      <<number::32-little, state, class>>,
      us_varchar(text),
      b_varchar("kurwadb"),
      b_varchar(""),
      <<1::32-little>>
    ]

    token(if(kind == :error, do: 0xAA, else: 0xAB), body)
  end

  # DONE, DONEPROC, DONEINPROC: status, current command, row count.
  @done_more 0x0001
  @done_error 0x0002
  @done_count 0x0010
  @done_attn 0x0020

  def done(kind, opts \\ []) do
    status =
      if(opts[:more], do: @done_more, else: 0) ||| if(opts[:error], do: @done_error, else: 0) |||
        if(opts[:count] != nil, do: @done_count, else: 0) |||
        if(opts[:attention], do: @done_attn, else: 0)

    token =
      case kind do
        :done -> 0xFD
        :proc -> 0xFE
        :in_proc -> 0xFF
      end

    count = Keyword.get(opts, :count) || 0
    <<token, status::16-little, Keyword.get(opts, :command, 0)::16-little, count::64-little>>
  end

  def return_status(n), do: <<0x79, n::32-little-signed>>

  @doc """
  RETURNVALUE for a procedure's OUTPUT parameter, typed by the family it was
  declared with: an integer, a bit, or text.
  """
  def return_value(ordinal, name, value, family) do
    {type_info, data} =
      case {family, value} do
        {:int, nil} ->
          {[0x26, 8], <<0>>}

        {:int, v} when v >= -2_147_483_648 and v <= 2_147_483_647 ->
          {[0x26, 4], <<4, v::32-little-signed>>}

        {:int, v} ->
          {[0x26, 8], <<8, v::64-little-signed>>}

        {:bit, nil} ->
          {[0x68, 1], <<0>>}

        {:bit, v} ->
          {[0x68, 1], <<1, if(v in [true, 1], do: 1, else: 0)>>}

        {_, nil} ->
          {[0xE7, <<8000::16-little>>, collation()], <<0xFFFF::16>>}

        {_, v} ->
          {[0xE7, <<8000::16-little>>, collation()],
           [<<byte_size(ucs2(to_text(v)))::16-little>>, ucs2(to_text(v))]}
      end

    [0xAC, <<ordinal::16-little>>, b_varchar(name), 0x01, <<0::32, 0::16>>, type_info, data]
  end

  @doc "RETURNVALUE for an int OUTPUT parameter (sp_prepare's handle)."
  def return_value(ordinal, name, value) do
    [
      0xAC,
      <<ordinal::16-little>>,
      b_varchar(name),
      0x01,
      <<0::32, 0::16>>,
      0x26,
      4,
      4,
      <<value::32-little-signed>>
    ]
  end

  @doc "COLMETADATA for `{name, type}` columns."
  def colmetadata(columns) do
    [
      0x81,
      <<length(columns)::16-little>>,
      Enum.map(columns, fn {name, type} ->
        [<<0::32>>, flags(type), type_info(type), b_varchar(name)]
      end)
    ]
  end

  defp flags(_type), do: <<0x0001::16-little>>

  # NVARCHAR(4000), BITN, INTN.
  defp type_info(type) when type in [:text, :text_array, :name, :unknown],
    do: [0xE7, <<8000::16-little>>, collation()]

  defp type_info(:bool), do: [0x68, 1]
  defp type_info(:int8), do: [0x26, 8]
  defp type_info(type) when type in [:int4, :oid], do: [0x26, 4]
  defp type_info(:int2), do: [0x26, 2]

  @doc "ROW: one value per column, as COLMETADATA described it."
  def row(values, columns) do
    [0xD1, Enum.zip_with(values, columns, fn value, {_name, type} -> value(value, type) end)]
  end

  defp value(nil, type) when type in [:text, :text_array, :name, :unknown], do: <<0xFFFF::16>>
  defp value(nil, _type), do: <<0>>

  defp value(v, type) when type in [:text, :text_array, :name, :unknown] do
    s = ucs2(to_text(v))
    [<<byte_size(s)::16-little>>, s]
  end

  defp value(v, :bool), do: <<1, if(v, do: 1, else: 0)>>
  defp value(v, :int8), do: <<8, v::64-little-signed>>
  defp value(v, type) when type in [:int4, :oid], do: <<4, v::32-little-signed>>
  defp value(v, :int2), do: <<2, v::16-little-signed>>

  defp to_text(v) when is_binary(v), do: v
  defp to_text(true), do: "1"
  defp to_text(false), do: "0"
  defp to_text(v), do: to_string(v)

  defp token(type, body) do
    body = IO.iodata_to_binary(body)
    [type, <<byte_size(body)::16-little>>, body]
  end

  # -------------------------------------------------------------------- RPC

  @doc """
  Skips ALL_HEADERS at the front of a SQL batch or RPC payload, returning
  the transaction descriptor it carried (0 outside a transaction) and the rest.
  """
  def all_headers(<<total::32-little, _::binary>> = payload)
      when total >= 4 and total <= byte_size(payload) do
    size = total - 4
    <<_::32, headers::binary-size(^size), rest::binary>> = payload
    {descriptor(headers), rest}
  end

  def all_headers(payload), do: {0, payload}

  # Header type 2 is the transaction descriptor; skip any other.
  defp descriptor(<<len::32-little, type::16-little, data::binary>> = h)
       when len >= 6 and byte_size(h) >= len do
    case {type, data} do
      {2, <<d::64-little, _::binary>>} -> d
      _ -> descriptor(binary_part(h, len, byte_size(h) - len))
    end
  end

  defp descriptor(_), do: 0

  @doc """
  Parses the RPC requests in a payload (after ALL_HEADERS). Each is
  `{proc, params}` where `proc` is a name or a well-known id and `params` a
  list of `{name, value, output?}`.
  """
  def rpcs(payload) do
    {first, rest} = rpc(payload)
    more(rest, [first])
  end

  # A batch separator (0xFF in TDS 7.2+, 0x80 and 0xFE before) comes only
  # between requests - a request itself may start with 0xFFFF, a procedure id.
  defp more(<<>>, acc), do: Enum.reverse(acc)

  defp more(<<sep, rest::binary>>, acc) when sep in [0xFF, 0x80, 0xFE] do
    {request, rest} = rpc(rest)
    more(rest, [request | acc])
  end

  defp rpc(<<0xFFFF::16, id::16-little, _flags::16-little, rest::binary>>) do
    {params, rest} = params(rest, [])
    {{proc_name(id), params}, rest}
  end

  defp rpc(<<len::16-little, rest::binary>>) do
    size = len * 2
    <<name::binary-size(^size), _flags::16-little, rest::binary>> = rest
    {params, rest} = params(rest, [])
    {{name |> utf8() |> String.downcase() |> String.replace_prefix("sys.", ""), params}, rest}
  end

  defp proc_name(10), do: "sp_executesql"
  defp proc_name(11), do: "sp_prepare"
  defp proc_name(12), do: "sp_execute"
  defp proc_name(13), do: "sp_prepexec"
  defp proc_name(15), do: "sp_unprepare"
  defp proc_name(id), do: {:proc_id, id}

  # Parameters run until the payload ends or a batch separator starts the next RPC.
  defp params(<<>>, acc), do: {Enum.reverse(acc), <<>>}

  defp params(<<sep, _::binary>> = rest, acc) when sep in [0xFF, 0x80, 0xFE],
    do: {Enum.reverse(acc), rest}

  defp params(<<name_len, rest::binary>>, acc) do
    size = name_len * 2
    <<name::binary-size(^size), status, rest::binary>> = rest
    {value, rest} = typed_value(rest)
    params(rest, [{utf8(name), value, (status &&& 0x01) != 0} | acc])
  end

  @doc "Reads one TYPE_INFO and its value: `{value, rest}`."
  # Fixed-length types.
  def typed_value(<<0x1F, rest::binary>>), do: {nil, rest}
  def typed_value(<<0x30, v, rest::binary>>), do: {v, rest}
  def typed_value(<<0x32, v, rest::binary>>), do: {v != 0, rest}
  def typed_value(<<0x34, v::16-little-signed, rest::binary>>), do: {v, rest}
  def typed_value(<<0x38, v::32-little-signed, rest::binary>>), do: {v, rest}
  def typed_value(<<0x7F, v::64-little-signed, rest::binary>>), do: {v, rest}
  def typed_value(<<0x3E, v::64-float-little, rest::binary>>), do: {v, rest}
  def typed_value(<<0x3B, v::32-float-little, rest::binary>>), do: {v, rest}

  # Byte-length types: INTN, BITN, FLTN, GUID, and the rest carried as raw bytes.
  def typed_value(<<0x26, _max, 0, rest::binary>>), do: {nil, rest}
  def typed_value(<<0x26, _max, 1, v::8-signed, rest::binary>>), do: {v, rest}
  def typed_value(<<0x26, _max, 2, v::16-little-signed, rest::binary>>), do: {v, rest}
  def typed_value(<<0x26, _max, 4, v::32-little-signed, rest::binary>>), do: {v, rest}
  def typed_value(<<0x26, _max, 8, v::64-little-signed, rest::binary>>), do: {v, rest}
  def typed_value(<<0x68, _max, 0, rest::binary>>), do: {nil, rest}
  def typed_value(<<0x68, _max, 1, v, rest::binary>>), do: {v != 0, rest}
  def typed_value(<<0x6D, _max, 0, rest::binary>>), do: {nil, rest}
  def typed_value(<<0x6D, _max, 4, v::32-float-little, rest::binary>>), do: {v, rest}
  def typed_value(<<0x6D, _max, 8, v::64-float-little, rest::binary>>), do: {v, rest}
  def typed_value(<<0x24, _max, 0, rest::binary>>), do: {nil, rest}
  def typed_value(<<0x24, _max, 16, g::binary-size(16), rest::binary>>), do: {guid(g), rest}

  # DECIMAL/NUMERIC: length, precision, scale, then sign and magnitude.
  def typed_value(<<t, _len, _precision, _scale, 0, rest::binary>>) when t in [0x6A, 0x6C],
    do: {nil, rest}

  def typed_value(<<t, _len, _precision, scale, n, sign, rest::binary>>) when t in [0x6A, 0x6C] do
    size = n - 1
    <<magnitude::binary-size(^size), rest::binary>> = rest
    value = :binary.decode_unsigned(magnitude, :little)
    value = if sign == 1, do: value, else: -value
    {if(scale == 0, do: value, else: value / :math.pow(10, scale)), rest}
  end

  # Date and time types: a scale for some, then a length and the bytes.
  def typed_value(<<t, len, raw::binary-size(len), rest::binary>>) when t in [0x28, 0x6F, 0x6E],
    do: {{:raw, t, raw}, rest}

  def typed_value(<<t, _scale, len, raw::binary-size(len), rest::binary>>)
      when t in [0x29, 0x2A, 0x2B],
      do: {{:raw, t, raw}, rest}

  # Two-byte-length types: N/VARCHAR and N/CHAR carry a collation; 0xFFFF as the
  # max length means a (MAX) type, sent as PLP chunks.
  def typed_value(<<t, max::16-little, _collation::binary-size(5), rest::binary>>)
      when t in [0xE7, 0xEF] do
    {bytes, rest} = if max == 0xFFFF, do: plp(rest), else: ushort(rest)
    {bytes && utf8(bytes), rest}
  end

  def typed_value(<<t, max::16-little, _collation::binary-size(5), rest::binary>>)
      when t in [0xA7, 0xAF] do
    if max == 0xFFFF, do: plp(rest), else: ushort(rest)
  end

  def typed_value(<<t, max::16-little, rest::binary>>) when t in [0xA5, 0xAD] do
    if max == 0xFFFF, do: plp(rest), else: ushort(rest)
  end

  def typed_value(<<t, _::binary>>), do: throw({:tds_unsupported_type, t})

  defp ushort(<<0xFFFF::16, rest::binary>>), do: {nil, rest}
  defp ushort(<<len::16-little, v::binary-size(len), rest::binary>>), do: {v, rest}

  # PLP: a total length (all ones for NULL), then chunks ending at a zero one.
  defp plp(<<0xFFFFFFFFFFFFFFFF::64, rest::binary>>), do: {nil, rest}
  defp plp(<<_total::64-little, rest::binary>>), do: chunks(rest, [])

  defp chunks(<<0::32, rest::binary>>, acc), do: {IO.iodata_to_binary(Enum.reverse(acc)), rest}

  defp chunks(<<len::32-little, c::binary-size(len), rest::binary>>, acc),
    do: chunks(rest, [c | acc])

  defp guid(<<a::32-little, b::16-little, c::16-little, d::binary-size(2), e::binary-size(6)>>) do
    hex = &Base.encode16(&1, case: :upper)
    "#{hex.(<<a::32>>)}-#{hex.(<<b::16>>)}-#{hex.(<<c::16>>)}-#{hex.(d)}-#{hex.(e)}"
  end
end
