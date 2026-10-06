defmodule Kurwa.Mysql.Types do
  @moduledoc """
  kurwadb's few types as MySQL column types, and MySQL values in and out.

  A key is `VARCHAR` in utf8mb4, an answer is `TINYINT(1)` - MySQL's boolean -
  and a TTL or a count is `BIGINT`. The binary protocol of prepared statements
  carries integers as fixed-width little-endian and strings length-encoded;
  the text protocol carries everything as a string.
  """

  import Bitwise

  alias Kurwa.Mysql.Proto

  @tiny 0x01
  @short 0x02
  @long 0x03
  @float 0x04
  @double 0x05
  @null 0x06
  @longlong 0x08
  @int24 0x09
  @year 0x0D
  @var_string 0xFD

  @utf8mb4 255
  @binary 63

  @doc "`{type code, charset, display length, flags}` for a column."
  def column(type) when type in [:text, :text_array, :name, :unknown],
    do: {@var_string, @utf8mb4, 1020, 0}

  def column(:bool), do: {@tiny, @binary, 1, 0}
  def column(:int8), do: {@longlong, @binary, 20, 0}
  def column(type) when type in [:int4, :oid], do: {@long, @binary, 11, 0}
  def column(:int2), do: {@short, @binary, 6, 0}

  @doc "A value for the text protocol."
  def text(nil), do: nil
  def text(true), do: "1"
  def text(false), do: "0"
  def text(n) when is_integer(n), do: Integer.to_string(n)
  def text(s) when is_binary(s), do: s

  @doc "A value for the binary protocol, by column type."
  def binary(nil, _type), do: nil
  def binary(value, :bool), do: <<if(value, do: 1, else: 0)>>
  def binary(value, :int8), do: <<value::64-little-signed>>
  def binary(value, type) when type in [:int4, :oid], do: <<value::32-little-signed>>
  def binary(value, :int2), do: <<value::16-little-signed>>
  def binary(value, _text), do: Proto.lenenc_string(text(value))

  @doc "Decodes one binary-protocol parameter of `type`: `{value, rest}`."
  def decode(@null, _flags, rest), do: {nil, rest}
  def decode(@tiny, flags, <<n::8-bits, rest::binary>>), do: {int(n, flags), rest}

  def decode(type, flags, <<n::16-bits, rest::binary>>) when type in [@short, @year],
    do: {int(n, flags), rest}

  def decode(type, flags, <<n::32-bits, rest::binary>>) when type in [@long, @int24],
    do: {int(n, flags), rest}

  def decode(@longlong, flags, <<n::64-bits, rest::binary>>), do: {int(n, flags), rest}
  def decode(@float, _flags, <<f::32-float-little, rest::binary>>), do: {f, rest}
  def decode(@double, _flags, <<f::64-float-little, rest::binary>>), do: {f, rest}

  # Dates and times: a length byte and that many bytes. Nothing here takes one
  # as a key, so it is passed on as the raw bytes and refused where it lands.
  def decode(type, _flags, <<len, raw::binary-size(len), rest::binary>>)
      when type in [0x07, 0x0A, 0x0B, 0x0C],
      do: {raw, rest}

  # Every string, blob, decimal and JSON type is length-encoded.
  def decode(_string, _flags, rest) do
    {len, rest} = Proto.read_lenenc(rest)
    <<s::binary-size(^len), rest::binary>> = rest
    {s, rest}
  end

  defp int(bits, flags) do
    size = bit_size(bits)

    if (flags &&& 0x80) != 0 do
      <<n::size(^size)-little-unsigned>> = bits
      n
    else
      <<n::size(^size)-little-signed>> = bits
      n
    end
  end
end
