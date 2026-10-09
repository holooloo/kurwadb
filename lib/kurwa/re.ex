defmodule Kurwa.Re do
  @moduledoc """
  Regexes compiled once.

  Since OTP 28 a compiled regex cannot live in a module's literals, so a
  `~r` in code is compiled again every time it runs - about 300 ns on top of
  the match itself. That is noise in a catalog query and real money on a
  request path, so the regexes on request paths go through here: compiled on
  first use, kept in `:persistent_term`.
  """

  @doc """
  A `:binary.compile_pattern/1` for `patterns`, compiled once: `:binary.match/2`
  given a list builds its search automaton on every call - 13 µs for eleven
  short patterns, far more than the search.
  """
  def pattern(patterns) when is_list(patterns) do
    key = {__MODULE__, :pattern, patterns}

    case :persistent_term.get(key, nil) do
      nil ->
        compiled = :binary.compile_pattern(patterns)
        :persistent_term.put(key, compiled)
        compiled

      compiled ->
        compiled
    end
  end

  @doc "The compiled regex for `source` and `opts`."
  def get(source, opts \\ "") do
    key = {__MODULE__, source, opts}

    case :persistent_term.get(key, nil) do
      nil ->
        regex = Regex.compile!(source, opts)
        :persistent_term.put(key, regex)
        regex

      regex ->
        regex
    end
  end
end
