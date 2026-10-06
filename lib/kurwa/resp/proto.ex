defmodule Kurwa.Resp.Proto do
  @moduledoc """
  RESP, the Redis protocol: requests in, replies out, versions 2 and 3.

  A request is an array of bulk strings - `*2\\r\\n$4\\r\\nPING\\r\\n...` - or,
  from a person at a terminal, an inline line of words. Replies are built from
  a few terms and encoded for whichever version the connection said `HELLO` in:

      {:simple, "OK"}     +OK
      {:error, "ERR x"}   -ERR x
      42                  :42
      "bytes"             $5 bytes
      nil                 $-1 in RESP2, _ in RESP3
      [..]                *n
      {:map, [{k, v}]}    a flat array in RESP2, %n in RESP3
      true / false        :1 / :0 in RESP2, #t / #f in RESP3
  """

  # Bounds what one connection can make the node hold for a single request.
  @max_bulk 512 * 1024 * 1024
  @max_args 1_048_576

  @doc "Decodes one request from the front of `buffer`: `{:ok, args, rest}`, `:more` or `{:error, reason}`."
  def decode(<<?*, rest::binary>> = buffer) do
    case line(rest) do
      {:ok, count, rest} -> array(rest, count, [], buffer)
      :more -> :more
      :error -> {:error, "invalid multibulk length"}
    end
  end

  def decode(<<>>), do: :more

  # Inline: words separated by spaces, ending at \n.
  def decode(buffer) do
    case :binary.split(buffer, "\n") do
      [line, rest] ->
        case line |> String.trim_trailing("\r") |> String.split(" ", trim: true) do
          [] -> decode(rest)
          words -> {:ok, words, rest}
        end

      [_partial] ->
        if byte_size(buffer) > 64 * 1024, do: {:error, "inline request too long"}, else: :more
    end
  end

  defp array(rest, count, _acc, _buffer) when count < 0 or count > @max_args,
    do: if(count == -1, do: {:ok, [], rest}, else: {:error, "invalid multibulk length"})

  defp array(rest, 0, acc, _buffer), do: {:ok, Enum.reverse(acc), rest}

  defp array(<<?$, rest::binary>>, count, acc, buffer) do
    case line(rest) do
      {:ok, len, rest} when len >= 0 and len <= @max_bulk ->
        case rest do
          <<value::binary-size(^len), "\r\n", rest::binary>> ->
            array(rest, count - 1, [value | acc], buffer)

          _ when byte_size(rest) < len + 2 ->
            :more

          _ ->
            {:error, "invalid bulk string"}
        end

      {:ok, _len, _rest} ->
        {:error, "invalid bulk length"}

      :more ->
        :more

      :error ->
        {:error, "invalid bulk length"}
    end
  end

  defp array(<<>>, _count, _acc, _buffer), do: :more
  defp array(_rest, _count, _acc, _buffer), do: {:error, "expected '$'"}

  defp line(binary) do
    case :binary.split(binary, "\r\n") do
      [digits, rest] ->
        case Integer.parse(digits) do
          {n, ""} -> {:ok, n, rest}
          _ -> :error
        end

      [_] ->
        if byte_size(binary) > 32, do: :error, else: :more
    end
  end

  @doc "Encodes a reply term for protocol version `version` (2 or 3)."
  def encode({:simple, s}, _v), do: [?+, s, "\r\n"]
  def encode({:error, s}, _v), do: [?-, s, "\r\n"]
  def encode(n, _v) when is_integer(n), do: [?:, Integer.to_string(n), "\r\n"]
  def encode(:null_array, 2), do: "*-1\r\n"
  def encode(:null_array, 3), do: "_\r\n"
  def encode(nil, 2), do: "$-1\r\n"
  def encode(nil, 3), do: "_\r\n"
  def encode(true, 2), do: ":1\r\n"
  def encode(false, 2), do: ":0\r\n"
  def encode(true, 3), do: "#t\r\n"
  def encode(false, 3), do: "#f\r\n"

  def encode(b, _v) when is_binary(b),
    do: [?$, Integer.to_string(byte_size(b)), "\r\n", b, "\r\n"]

  def encode(list, v) when is_list(list),
    do: [?*, Integer.to_string(length(list)), "\r\n", Enum.map(list, &encode(&1, v))]

  def encode({:map, pairs}, 2),
    do: encode(Enum.flat_map(pairs, fn {k, val} -> [k, val] end), 2)

  def encode({:map, pairs}, 3),
    do: [
      ?%,
      Integer.to_string(length(pairs)),
      "\r\n",
      Enum.map(pairs, fn {k, val} -> [encode(k, 3), encode(val, 3)] end)
    ]
end
