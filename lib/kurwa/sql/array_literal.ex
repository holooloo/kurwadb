defmodule Kurwa.Sql.ArrayLiteral do
  @moduledoc """
  PostgreSQL's text form of a one-dimensional array: `{a,b,"c,d","e\\"f",NULL}`.

  It is how an array arrives as a string literal (`key = ANY('{a,b}')`) and how
  a text-format array parameter is sent. Elements are unquoted unless they hold
  a delimiter, a quote, a backslash or whitespace; an unquoted `NULL` is null.
  """

  @spec parse(binary()) :: {:ok, [binary() | nil]} | :error
  def parse(text) do
    case String.trim(text) do
      "{}" -> {:ok, []}
      "{" <> rest -> elements(rest, [])
      _ -> :error
    end
  end

  defp elements(<<?", rest::binary>>, acc) do
    with {:ok, element, rest} <- quoted(rest, []), do: next(rest, [element | acc])
  end

  defp elements(rest, acc) do
    case :binary.match(rest, [",", "}"]) do
      {pos, 1} ->
        <<raw::binary-size(^pos), rest::binary>> = rest

        element =
          case String.trim(raw) do
            "NULL" -> nil
            value -> value
          end

        next(rest, [element | acc])

      :nomatch ->
        :error
    end
  end

  defp next(<<c, rest::binary>>, acc) when c in ~c" \t\r\n", do: next(rest, acc)
  defp next(<<?,, rest::binary>>, acc), do: elements(String.trim_leading(rest), acc)

  defp next(<<?}, rest::binary>>, acc),
    do: if(String.trim(rest) == "", do: {:ok, Enum.reverse(acc)}, else: :error)

  defp next(_rest, _acc), do: :error

  defp quoted(<<?\\, c, rest::binary>>, acc), do: quoted(rest, [c | acc])

  defp quoted(<<?", rest::binary>>, acc),
    do: {:ok, acc |> Enum.reverse() |> :erlang.list_to_binary(), rest}

  defp quoted(<<c, rest::binary>>, acc), do: quoted(rest, [c | acc])
  defp quoted(<<>>, _acc), do: :error
end
