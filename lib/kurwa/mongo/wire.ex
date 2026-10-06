defmodule Kurwa.Mongo.Wire do
  @moduledoc """
  MongoDB's message framing: a 16-byte header, then OP_MSG - what every
  current driver speaks - or OP_QUERY, which drivers still use for the first
  `hello` of a connection.

  An OP_MSG has a body section (kind 0, one document) and optional document
  sequences (kind 1): `insert` sends its documents as a sequence named
  `documents` rather than inside the body. Both are folded into one command
  document here, so the commands see the same shape either way.

  Compression (OP_COMPRESSED) is not advertised, so no driver uses it.
  """

  import Bitwise

  alias Kurwa.Mongo.Bson

  @op_reply 1
  @op_query 2004
  @op_msg 2013

  @checksum_present 0x1
  @more_to_come 0x2

  # MongoDB's own limit on a message, which hello also advertises.
  @max_message 48_000_000

  def max_message, do: @max_message

  @doc """
  One message from the front of `buffer`:

      {:ok, {:msg, request_id, command, more_to_come?}, rest}
      {:ok, {:query, request_id, collection, command}, rest}
      {:ok, {:unsupported, request_id, opcode}, rest}
      :more | {:error, reason}
  """
  def decode(<<len::32-little-signed, _::binary>>) when len < 16 or len > @max_message,
    do: {:error, {:bad_length, len}}

  def decode(<<len::32-little-signed, _::binary>> = buffer) when byte_size(buffer) >= len do
    body_len = len - 16

    <<_::32, request_id::32-little-signed, _response_to::32-little, opcode::32-little,
      body::binary-size(^body_len), rest::binary>> = buffer

    {:ok, message(opcode, request_id, body), rest}
  rescue
    _ in [MatchError, ArgumentError, FunctionClauseError] -> {:error, :malformed}
  end

  def decode(_partial), do: :more

  defp message(@op_msg, id, <<flags::32-little, sections::binary>>) do
    sections =
      if (flags &&& @checksum_present) != 0,
        do: binary_part(sections, 0, byte_size(sections) - 4),
        else: sections

    {:doc, body} = sections(sections, nil, [])
    {:msg, id, {:doc, body}, (flags &&& @more_to_come) != 0}
  end

  defp message(@op_query, id, <<_flags::32-little, rest::binary>>) do
    [collection, rest] = :binary.split(rest, <<0>>)
    <<_skip::32-little, _return::32-little, rest::binary>> = rest
    {:ok, query, _rest} = Bson.decode(rest)

    # A legacy command may be wrapped as {$query: {...}, $readPreference: ...}.
    command =
      case Bson.get(query, "$query") do
        {:doc, _} = inner -> inner
        _ -> query
      end

    {:query, id, collection, command}
  end

  defp message(opcode, id, _body), do: {:unsupported, id, opcode}

  # Folds kind-1 sequences into the body as arrays named by their identifier.
  defp sections(<<>>, {:doc, body}, sequences), do: {:doc, body ++ Enum.reverse(sequences)}

  defp sections(<<0, rest::binary>>, _body, sequences) do
    {:ok, doc, rest} = Bson.decode(rest)
    sections(rest, doc, sequences)
  end

  defp sections(<<1, size::32-little, rest::binary>>, body, sequences) do
    payload_size = size - 4
    <<payload::binary-size(^payload_size), rest::binary>> = rest
    [identifier, docs] = :binary.split(payload, <<0>>)
    sections(rest, body, [{identifier, documents(docs, [])} | sequences])
  end

  defp documents(<<>>, acc), do: Enum.reverse(acc)

  defp documents(binary, acc) do
    {:ok, doc, rest} = Bson.decode(binary)
    documents(rest, [doc | acc])
  end

  @doc "An OP_MSG reply to `request_id`."
  def reply_msg(request_id, doc) do
    body = [<<0::32-little, 0>>, Bson.encode(doc)]
    header(request_id, @op_msg, body)
  end

  @doc "An OP_REPLY to a legacy OP_QUERY: one document, no cursor."
  def reply_query(request_id, doc) do
    body = [<<0::32-little, 0::64-little, 0::32-little, 1::32-little>>, Bson.encode(doc)]
    header(request_id, @op_reply, body)
  end

  defp header(response_to, opcode, body) do
    body = IO.iodata_to_binary(body)
    id = System.unique_integer([:positive]) |> rem(2_000_000_000)

    <<byte_size(body) + 16::32-little, id::32-little, response_to::32-little, opcode::32-little,
      body::binary>>
  end
end
