defmodule Kurwa.Sql.Lexer do
  @moduledoc """
  Tokens for the SQL kurwadb understands.

  Shared by every SQL frontend, so the dialect decides the two things that
  differ between them: how a quoted identifier is written (`"x"` in PostgreSQL,
  `` `x` `` in MySQL) and how a parameter is (`$1` against `?`). Keywords are
  not special here; the parser recognises them by position.

      {:ident, "select"}      bare words, lowercased
      {:qident, "My-Set"}     quoted identifiers, as written
      {:string, "order:1"}
      {:number, 42}
      {:param, 1}             1-based
      {:op, "::"}
  """

  @type token ::
          {:ident, binary()}
          | {:qident, binary()}
          | {:string, binary()}
          | {:number, number()}
          | {:param, pos_integer()}
          | {:op, binary()}

  @doc "Tokenises `sql`. Positional `?` parameters are numbered in order."
  @spec tokenize(binary(), :pg | :mysql) :: {:ok, [token()]} | {:error, binary()}
  def tokenize(sql, dialect \\ :pg) when is_binary(sql) do
    lex(sql, dialect, 1, [])
  end

  defp lex(<<>>, _d, _n, acc), do: {:ok, Enum.reverse(acc)}

  defp lex(<<c, rest::binary>>, d, n, acc) when c in ~c" \t\r\n\f", do: lex(rest, d, n, acc)

  defp lex(<<"--", rest::binary>>, d, n, acc) do
    case :binary.split(rest, "\n") do
      [_comment, rest] -> lex(rest, d, n, acc)
      [_comment] -> lex(<<>>, d, n, acc)
    end
  end

  # MySQL also comments with #.
  defp lex(<<"#", rest::binary>>, :mysql = d, n, acc) do
    case :binary.split(rest, "\n") do
      [_comment, rest] -> lex(rest, d, n, acc)
      [_comment] -> lex(<<>>, d, n, acc)
    end
  end

  defp lex(<<"/*", rest::binary>>, d, n, acc) do
    case :binary.split(rest, "*/") do
      [_comment, rest] -> lex(rest, d, n, acc)
      [_unterminated] -> {:error, "unterminated /* comment"}
    end
  end

  defp lex(<<q, rest::binary>>, d, n, acc) when q in [?', ?"] or (q == ?` and d == :mysql) do
    case quoted(rest, q, d, []) do
      # In MySQL, without ANSI_QUOTES, "x" is a string, as 'x' is.
      {:ok, text, rest} ->
        string? = q == ?' or (q == ?" and d == :mysql)
        token = if string?, do: {:string, text}, else: {:qident, text}
        lex(rest, d, n, [token | acc])

      :error ->
        {:error, "unterminated quoted #{if q == ?', do: "string", else: "identifier"}"}
    end
  end

  # E'...' escape strings: only the escapes a client library actually emits.
  defp lex(<<e, ?', rest::binary>>, d, n, acc) when e in [?e, ?E] do
    case quoted(rest, ?', :escape, []) do
      {:ok, text, rest} -> lex(rest, d, n, [{:string, text} | acc])
      :error -> {:error, "unterminated quoted string"}
    end
  end

  defp lex(<<?$, rest::binary>>, :pg = d, n, acc) do
    case Integer.parse(rest) do
      {index, rest} when index > 0 -> lex(rest, d, n, [{:param, index} | acc])
      _ -> {:error, "expected a parameter number after $"}
    end
  end

  defp lex(<<??, rest::binary>>, :mysql = d, n, acc), do: lex(rest, d, n + 1, [{:param, n} | acc])

  defp lex(<<c, _::binary>> = sql, d, n, acc) when c in ?0..?9 do
    {number, rest} = number(sql)
    lex(rest, d, n, [{:number, number} | acc])
  end

  defp lex(<<c, _::binary>> = sql, d, n, acc)
       when c in ?a..?z or c in ?A..?Z or c == ?_ or c >= 128 do
    {word, rest} = word(sql, [])
    lex(rest, d, n, [{:ident, String.downcase(word)} | acc])
  end

  defp lex(<<op::binary-size(2), rest::binary>>, d, n, acc)
       when op in ["::", "<>", "!=", "<=", ">=", "||", "!~", "~*"],
       do: lex(rest, d, n, [{:op, op} | acc])

  defp lex(<<c, rest::binary>>, d, n, acc), do: lex(rest, d, n, [{:op, <<c>>} | acc])

  defp quoted(<<q, q, rest::binary>>, q, mode, acc), do: quoted(rest, q, mode, [q | acc])
  defp quoted(<<q, rest::binary>>, q, _mode, acc), do: {:ok, finish(acc), rest}

  defp quoted(<<?\\, c, rest::binary>>, q, mode, acc) when mode == :escape or mode == :mysql do
    char =
      case c do
        ?n -> ?\n
        ?t -> ?\t
        ?r -> ?\r
        ?0 -> 0
        other -> other
      end

    quoted(rest, q, mode, [char | acc])
  end

  defp quoted(<<c, rest::binary>>, q, mode, acc), do: quoted(rest, q, mode, [c | acc])
  defp quoted(<<>>, _q, _mode, _acc), do: :error

  defp finish(acc), do: acc |> Enum.reverse() |> :erlang.list_to_binary()

  defp number(sql) do
    case Float.parse(sql) do
      {float, rest} ->
        {integer, int_rest} = Integer.parse(sql)
        if int_rest == rest, do: {integer, rest}, else: {float, rest}

      :error ->
        Integer.parse(sql)
    end
  end

  defp word(<<c, rest::binary>>, acc)
       when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_ or c == ?$ or c >= 128,
       do: word(rest, [c | acc])

  defp word(rest, acc), do: {finish(acc), rest}
end
