defmodule Kurwa.Mongo.Bson do
  @moduledoc """
  BSON, MongoDB's binary documents, in and out.

  A document is `{:doc, [{key, value}]}` - a list rather than a map, because
  BSON keys are ordered and MongoDB depends on it: a command is named by its
  first key. Values:

      "text"                 string
      42                     int32 when it fits, int64 when it does not
      {:int64, n}            int64 whatever the size (cursor ids must be)
      1.5                    double
      true / false / nil     boolean, null
      {:doc, pairs}          embedded document
      [..]                   array
      {:oid, <<12 bytes>>}   ObjectId
      {:binary, subtype, b}  binary
      {:datetime, ms}        UTC datetime
      {:timestamp, t, i}     internal timestamp
      {:raw, type, bytes}    anything else, carried through untouched
  """

  @doc "Decodes one document from the front of `binary`: `{:ok, doc, rest}` or `:error`."
  def decode(<<size::32-little-signed, _::binary>> = binary)
      when size >= 5 and byte_size(binary) >= size do
    body_size = size - 5
    <<_::32, body::binary-size(^body_size), 0, rest::binary>> = binary
    {:ok, {:doc, elements(body, [])}, rest}
  rescue
    _ in [MatchError, ArgumentError, FunctionClauseError] -> :error
  end

  def decode(_), do: :error

  @doc "Decodes a whole binary that is exactly one document."
  def decode!(binary) do
    {:ok, doc, <<>>} = decode(binary)
    doc
  end

  defp elements(<<>>, acc), do: Enum.reverse(acc)

  defp elements(<<type, rest::binary>>, acc) do
    [key, rest] = :binary.split(rest, <<0>>)
    {value, rest} = value(type, rest)
    elements(rest, [{key, value} | acc])
  end

  defp value(0x01, <<f::64-float-little, rest::binary>>), do: {f, rest}
  defp value(0x02, <<len::32-little, s::binary-size(len - 1), 0, rest::binary>>), do: {s, rest}

  defp value(0x03, <<size::32-little, _::binary>> = binary) do
    <<doc::binary-size(^size), rest::binary>> = binary
    {decode!(doc), rest}
  end

  defp value(0x04, <<size::32-little, _::binary>> = binary) do
    <<doc::binary-size(^size), rest::binary>> = binary
    {:doc, pairs} = decode!(doc)
    {Enum.map(pairs, &elem(&1, 1)), rest}
  end

  defp value(0x05, <<len::32-little, subtype, b::binary-size(len), rest::binary>>),
    do: {{:binary, subtype, b}, rest}

  defp value(0x07, <<oid::binary-size(12), rest::binary>>), do: {{:oid, oid}, rest}
  defp value(0x08, <<b, rest::binary>>), do: {b != 0, rest}
  defp value(0x09, <<ms::64-little-signed, rest::binary>>), do: {{:datetime, ms}, rest}
  defp value(0x0A, rest), do: {nil, rest}
  defp value(0x10, <<n::32-little-signed, rest::binary>>), do: {n, rest}
  defp value(0x11, <<i::32-little, t::32-little, rest::binary>>), do: {{:timestamp, t, i}, rest}
  defp value(0x12, <<n::64-little-signed, rest::binary>>), do: {{:int64, n}, rest}
  defp value(0x13, <<d::binary-size(16), rest::binary>>), do: {{:raw, 0x13, d}, rest}
  defp value(0xFF, rest), do: {{:raw, 0xFF, <<>>}, rest}
  defp value(0x7F, rest), do: {{:raw, 0x7F, <<>>}, rest}

  defp value(0x0B, rest) do
    [pattern, rest] = :binary.split(rest, <<0>>)
    [options, rest] = :binary.split(rest, <<0>>)
    {{:raw, 0x0B, pattern <> <<0>> <> options <> <<0>>}, rest}
  end

  @doc "Encodes a document."
  def encode({:doc, pairs}) do
    body = IO.iodata_to_binary(Enum.map(pairs, fn {k, v} -> element(to_string(k), v) end))
    <<byte_size(body) + 5::32-little, body::binary, 0>>
  end

  defp element(key, value) do
    {type, bytes} = encode_value(value)
    [type, key, 0, bytes]
  end

  defp encode_value(f) when is_float(f), do: {0x01, <<f::64-float-little>>}

  defp encode_value(s) when is_binary(s),
    do: {0x02, <<byte_size(s) + 1::32-little, s::binary, 0>>}

  defp encode_value({:doc, _} = doc), do: {0x03, encode(doc)}

  defp encode_value(list) when is_list(list),
    do:
      {0x04,
       encode(
         {:doc, list |> Enum.with_index() |> Enum.map(fn {v, i} -> {Integer.to_string(i), v} end)}
       )}

  defp encode_value({:binary, subtype, b}),
    do: {0x05, <<byte_size(b)::32-little, subtype, b::binary>>}

  defp encode_value({:oid, oid}), do: {0x07, oid}
  defp encode_value(true), do: {0x08, <<1>>}
  defp encode_value(false), do: {0x08, <<0>>}
  defp encode_value({:datetime, ms}), do: {0x09, <<ms::64-little-signed>>}
  defp encode_value(nil), do: {0x0A, <<>>}
  defp encode_value({:timestamp, t, i}), do: {0x11, <<i::32-little, t::32-little>>}
  defp encode_value({:int64, n}), do: {0x12, <<n::64-little-signed>>}
  defp encode_value({:raw, type, bytes}), do: {type, bytes}

  defp encode_value(n) when is_integer(n) and n >= -2_147_483_648 and n <= 2_147_483_647,
    do: {0x10, <<n::32-little-signed>>}

  defp encode_value(n) when is_integer(n), do: {0x12, <<n::64-little-signed>>}

  # ----------------------------------------------------------------- access

  @doc "The value at `key`, or `default`."
  def get({:doc, pairs}, key, default \\ nil) do
    case List.keyfind(pairs, key, 0) do
      {_, value} -> value
      nil -> default
    end
  end

  @doc "The first key: what a command document is called."
  def name({:doc, [{key, _} | _]}), do: key
  def name(_), do: nil

  @doc "A plain integer from any BSON number."
  def int({:int64, n}), do: n
  def int(n) when is_integer(n), do: n
  def int(f) when is_float(f), do: trunc(f)
  def int(_), do: nil

  @doc "MongoDB's truthiness for flags like `ordered`: booleans and numbers."
  def truthy?(nil, default), do: default
  def truthy?(false, _), do: false
  def truthy?(0, _), do: false
  def truthy?(f, _) when f == 0.0, do: false
  def truthy?(_, _), do: true

  @doc "The canonical bytes of one value, for keying a non-string _id."
  def value_bytes(value) do
    {type, bytes} = encode_value(value)
    <<type, IO.iodata_to_binary(bytes)::binary>>
  end
end
