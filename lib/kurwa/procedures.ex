defmodule Kurwa.Procedures do
  @moduledoc """
  Stored procedures, kept as `.sql` files deployed with the node.

  A procedure's definition is a value, and this store keeps keys - so
  procedures are not created over the wire with `CREATE PROCEDURE`; they are
  files in `procedures_dir` (`KURWA_PROCEDURES_DIR`), one or more per file
  separated by `GO` lines as SQL Server scripts are, read when the node
  starts. Every node is meant to carry the same files, which deployment
  ensures; `hash/0` is a fingerprint of what this node loaded, shown in
  `/info`, so a mismatch between nodes is visible rather than silent.

  A file that does not parse stops the node from starting, with the file and
  the reason: a procedure is configuration, and a broken one should be loud.
  `reload/0` reads the directory again at run time, and on error keeps what
  was loaded.

  The language is `Kurwa.Sql.Procedural`'s T-SQL subset.
  """

  alias Kurwa.Sql.Procedural

  require Logger

  @key {__MODULE__, :loaded}

  @doc "Loads the configured directory at start; raises if a file is broken."
  def load! do
    case read(Kurwa.Config.get(:procedures_dir)) do
      {:ok, loaded} ->
        put(loaded)

      {:error, message} ->
        raise ArgumentError, "kurwadb: stored procedures did not load: #{message}"
    end
  end

  @doc "Reads the directory again. On error, keeps what was loaded."
  def reload do
    with {:ok, loaded} <- read(Kurwa.Config.get(:procedures_dir)) do
      put(loaded)
      {:ok, map_size(loaded.procedures)}
    end
  end

  @doc "The procedure called `name` (any case, with or without dbo.), or nil."
  def lookup(name) when is_binary(name) do
    name =
      name
      |> String.downcase()
      |> String.replace_prefix("dbo.", "")
      |> String.replace_prefix("kurwadb.dbo.", "")

    Map.get(get().procedures, name)
  end

  def lookup(_), do: nil

  @doc "The names loaded, sorted."
  def names, do: get().procedures |> Map.keys() |> Enum.sort()

  @doc "A fingerprint of every definition loaded: equal on nodes that carry the same files."
  def hash, do: get().hash

  @doc "Installs procedures directly - for tests, and for code that builds them."
  def put_sources(sources) when is_list(sources) do
    with {:ok, procedures} <- parse_all(Enum.map(sources, &{"(given)", &1})), do: put(procedures)
  end

  defp get, do: :persistent_term.get(@key, %{procedures: %{}, hash: hash_of([])})

  defp put(%{procedures: _} = loaded) do
    :persistent_term.put(@key, loaded)

    if map_size(loaded.procedures) > 0,
      do: Logger.info("kurwadb: #{map_size(loaded.procedures)} stored procedures, #{loaded.hash}")

    :ok
  end

  defp read(nil), do: {:ok, %{procedures: %{}, hash: hash_of([])}}

  defp read(dir) do
    if File.dir?(dir) do
      dir
      |> Path.join("**/*.sql")
      |> Path.wildcard()
      |> Enum.sort()
      |> Enum.flat_map(fn file ->
        file |> File.read!() |> chunks() |> Enum.map(&{Path.relative_to(file, dir), &1})
      end)
      |> parse_all()
    else
      {:error, "procedures_dir #{inspect(dir)} is not a directory"}
    end
  end

  # A script's batches, split at lines that are only GO.
  defp chunks(text) do
    text
    |> String.split(~r/^\s*GO\s*;?\s*$/im)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp parse_all(sources) do
    Enum.reduce_while(sources, {:ok, %{}, []}, fn {file, sql}, {:ok, acc, texts} ->
      cond do
        script_setting?(sql) ->
          {:cont, {:ok, acc, texts}}

        true ->
          case Procedural.parse_procedure(sql) do
            {:ok, proc} ->
              if Map.has_key?(acc, proc.name),
                do: {:halt, {:error, "#{file}: procedure #{proc.name} is defined twice"}},
                else: {:cont, {:ok, Map.put(acc, proc.name, proc), [sql | texts]}}

            {:error, _code, message} ->
              {:halt, {:error, "#{file}: #{message}"}}
          end
      end
    end)
    |> case do
      {:ok, procedures, texts} -> {:ok, %{procedures: procedures, hash: hash_of(texts)}}
      error -> error
    end
  end

  # SET ANSI_NULLS ON, SET QUOTED_IDENTIFIER ON, USE kurwadb: the lines SQL
  # Server's script generator puts between procedures. Nothing to keep.
  defp script_setting?(sql) do
    sql
    |> String.split(~r/[;\n]/)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == "" or String.starts_with?(&1, "--")))
    |> Enum.all?(&Regex.match?(~r/^(SET\s+\w+\s+(ON|OFF)|USE\s+\[?\w+\]?)$/i, &1))
  end

  defp hash_of(texts) do
    digest = texts |> Enum.sort() |> Enum.join("\n--\n") |> then(&:crypto.hash(:sha256, &1))
    "sha256:" <> (digest |> Base.encode16(case: :lower) |> binary_part(0, 16))
  end
end
