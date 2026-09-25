defmodule Kurwa.Store.Wal do
  @moduledoc """
  Append-only write-ahead log plus periodic snapshot, one pair per shard.

  On-disk layout, inside the shard directory:

      snapshot   every live record at the time of the last compaction
      wal        every record accepted since then

  Both files use the same framing:

      "KWAL1\\n" | <len::32> <crc32::32> <term_to_binary(record)> | ...

  Recovery reads `snapshot` then `wal` and merges the records in file order.
  A torn tail (a write cut short by a crash) fails its CRC or its length check
  and simply ends the replay - everything before it is still good.
  """

  require Logger

  @magic "KWAL1\n"
  @chunk 1_048_576
  @max_entry 16_777_216

  defstruct [:dir, :path, :fd, appended: 0]

  @type t :: %__MODULE__{dir: Path.t(), path: Path.t(), fd: term(), appended: non_neg_integer()}

  @doc "Opens (creating if needed) the log in `dir`."
  @spec open(Path.t()) :: {:ok, t()} | {:error, term()}
  def open(dir) do
    with :ok <- File.mkdir_p(dir) do
      path = Path.join(dir, "wal")
      fresh? = not File.exists?(path)

      case :file.open(path, [:append, :raw, :binary, {:delayed_write, 512 * 1024, 200}]) do
        {:ok, fd} ->
          if fresh?, do: :ok = :file.write(fd, @magic)
          {:ok, %__MODULE__{dir: dir, path: path, fd: fd}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc "Appends one record."
  @spec append(t(), Kurwa.Record.t()) :: {:ok, t()} | {:error, term()}
  def append(%__MODULE__{fd: fd} = wal, record) do
    case :file.write(fd, frame(record)) do
      :ok -> {:ok, %{wal | appended: wal.appended + 1}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Flushes the delayed-write buffer and fsyncs."
  @spec sync(t()) :: :ok | {:error, term()}
  def sync(%__MODULE__{fd: fd}), do: :file.sync(fd)

  @doc "Closes the log (flushing on the way out)."
  @spec close(t()) :: :ok
  def close(%__MODULE__{fd: fd}) do
    _ = :file.sync(fd)
    :file.close(fd)
    :ok
  end

  @doc """
  Replays `snapshot` then `wal` from `dir`, calling `fun.(record, acc)` for each.

  Returns the final accumulator and how many entries came out of the WAL itself,
  which is what decides whether a fresh snapshot is due.
  """
  @spec replay(Path.t(), acc, (Kurwa.Record.t(), acc -> acc)) ::
          {:ok, acc, non_neg_integer()} | {:error, term()}
        when acc: term()
  def replay(dir, acc, fun) when is_function(fun, 2) do
    with {:ok, acc, _snap} <- scan(Path.join(dir, "snapshot"), acc, fun),
         {:ok, acc, from_wal} <- scan(Path.join(dir, "wal"), acc, fun) do
      {:ok, acc, from_wal}
    end
  end

  @doc """
  Writes `records` as the new snapshot and starts a fresh WAL.

  Order matters for crash safety: the snapshot is fsynced and atomically renamed
  into place *before* the old log goes away, so a crash at any point leaves
  either the old (snapshot, wal) pair or the new one - never a gap.
  """
  @spec compact(t(), Enumerable.t()) :: {:ok, t()} | {:error, term()}
  def compact(%__MODULE__{dir: dir} = wal, records) do
    tmp = Path.join(dir, "snapshot.tmp")
    final = Path.join(dir, "snapshot")

    with {:ok, fd} <- :file.open(tmp, [:write, :raw, :binary, {:delayed_write, 512 * 1024, 200}]),
         :ok <- :file.write(fd, @magic),
         :ok <- write_all(fd, records),
         :ok <- :file.sync(fd),
         :ok <- :file.close(fd),
         :ok <- :file.rename(tmp, final),
         :ok <- close(wal),
         :ok <- rm(wal.path) do
      open(dir)
    else
      {:error, reason} ->
        _ = File.rm(tmp)
        {:error, reason}
    end
  end

  defp rm(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      other -> other
    end
  end

  defp write_all(fd, records) do
    records
    |> Stream.chunk_every(1_000)
    |> Enum.reduce_while(:ok, fn batch, :ok ->
      case :file.write(fd, Enum.map(batch, &frame/1)) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp frame(record) do
    payload = :erlang.term_to_binary(record)
    [<<byte_size(payload)::32, :erlang.crc32(payload)::32>>, payload]
  end

  defp scan(path, acc, fun) do
    case :file.open(path, [:read, :raw, :binary, {:read_ahead, @chunk}]) do
      {:error, :enoent} ->
        {:ok, acc, 0}

      {:ok, fd} ->
        try do
          case :file.read(fd, byte_size(@magic)) do
            {:ok, @magic} -> scan_entries(fd, path, "", acc, fun, 0)
            :eof -> {:ok, acc, 0}
            {:ok, _} -> {:error, {:bad_magic, path}}
            {:error, reason} -> {:error, reason}
          end
        after
          :file.close(fd)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp scan_entries(fd, path, buffer, acc, fun, count) do
    case take(buffer) do
      {:entry, payload, rest} ->
        case decode(payload) do
          {:ok, record} ->
            scan_entries(fd, path, rest, fun.(record, acc), fun, count + 1)

          :error ->
            Logger.warning(
              "kurwadb: undecodable entry in #{path} after #{count}, stopping replay"
            )

            {:ok, acc, count}
        end

      :corrupt ->
        Logger.warning("kurwadb: corrupt entry in #{path} after #{count}, stopping replay")
        {:ok, acc, count}

      :more ->
        case :file.read(fd, @chunk) do
          {:ok, data} ->
            scan_entries(fd, path, buffer <> data, acc, fun, count)

          :eof ->
            if buffer != "" do
              Logger.warning("kurwadb: #{byte_size(buffer)} trailing bytes in #{path} ignored")
            end

            {:ok, acc, count}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp take(<<len::32, _crc::32, _rest::binary>>) when len > @max_entry, do: :corrupt

  defp take(<<len::32, crc::32, payload::binary-size(len), rest::binary>>) do
    if :erlang.crc32(payload) == crc, do: {:entry, payload, rest}, else: :corrupt
  end

  defp take(_partial), do: :more

  # Our own file, written by us: plain binary_to_term, because `:safe` would
  # reject the node-name atoms of a node that has not been contacted yet.
  defp decode(payload) do
    case :erlang.binary_to_term(payload) do
      {key, lamport, node, alive?, wall}
      when is_binary(key) and is_integer(lamport) and is_atom(node) and is_boolean(alive?) and
             is_integer(wall) ->
        {:ok, {key, lamport, node, alive?, wall}}

      _ ->
        :error
    end
  rescue
    ArgumentError -> :error
  end
end
