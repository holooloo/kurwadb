defmodule Kurwa.Pg.Types do
  @moduledoc """
  The few PostgreSQL types kurwadb answers in, and their wire encodings.

  A key is `text`, an answer is `bool`, a TTL or a count is `int8`. Clients ask
  for each column in text or binary format, and parameters can arrive in either,
  so both are here.
  """

  @type t :: :text | :text_array | :bool | :int8 | :int4 | :int2 | :oid | :name | :unknown

  @oids %{
    text: {25, -1},
    text_array: {1009, -1},
    bool: {16, 1},
    int8: {20, 8},
    int4: {23, 4},
    int2: {21, 2},
    oid: {26, 4},
    name: {19, 64},
    unknown: {705, -2}
  }

  # varchar (1043) and varchar[] (1015) read the same as text and text[].
  @by_oid @oids |> Map.new(fn {type, {oid, _}} -> {oid, type} end) |> Map.put(1015, :text_array)

  @doc "`{oid, typlen}` for a type."
  def oid(type), do: Map.fetch!(@oids, type)

  @doc "The type for an oid, or `:text` for anything we do not know."
  def from_oid(oid), do: Map.get(@by_oid, oid, :text)

  @doc "Encodes an Elixir value as a column of `type` in `format` (0 text, 1 binary)."
  def encode(nil, _type, _format), do: nil

  def encode(value, :bool, 0), do: if(value, do: "t", else: "f")
  def encode(value, :bool, 1), do: if(value, do: <<1>>, else: <<0>>)
  def encode(value, :int8, 1), do: <<value::64-signed>>
  def encode(value, type, 1) when type in [:int4, :oid], do: <<value::32-signed>>
  def encode(value, :int2, 1), do: <<value::16-signed>>
  def encode(value, _type, _format) when is_integer(value), do: Integer.to_string(value)
  def encode(true, _type, _format), do: "t"
  def encode(false, _type, _format), do: "f"
  def encode(value, _type, _format) when is_binary(value), do: value

  @doc """
  Decodes a bound parameter. Text-format values stay strings - the statement
  decides what it needs from them - except where the client declared an integer
  or boolean type and sent it in binary.
  """
  def decode(nil, _oid, _format), do: nil

  # A text[] in text format is an array literal; varchar[] (1015) is the same.
  def decode(value, oid, 0) when oid in [1009, 1015] do
    case Kurwa.Sql.ArrayLiteral.parse(value) do
      {:ok, list} -> list
      :error -> value
    end
  end

  def decode(value, _oid, 0), do: value

  # The binary array format: dimensions, a null flag, the element type, then
  # (size, lower bound) per dimension and each element length-prefixed.
  def decode(<<1::32, _flags::32, _elem::32, count::32, _lower::32, rest::binary>>, oid, 1)
      when oid in [1009, 1015],
      do: elements(rest, count, [])

  def decode(<<0::32, _flags::32, _elem::32>>, oid, 1) when oid in [1009, 1015], do: []

  def decode(value, oid, 1) do
    case {from_oid(oid), value} do
      {:int8, <<n::64-signed>>} -> n
      {:int4, <<n::32-signed>>} -> n
      {:int2, <<n::16-signed>>} -> n
      {:oid, <<n::32>>} -> n
      {:bool, <<b>>} -> b != 0
      {_text, value} -> value
    end
  end

  defp elements(_rest, 0, acc), do: Enum.reverse(acc)
  defp elements(<<-1::32-signed, rest::binary>>, n, acc), do: elements(rest, n - 1, [nil | acc])

  defp elements(<<len::32, value::binary-size(len), rest::binary>>, n, acc),
    do: elements(rest, n - 1, [value | acc])
end
