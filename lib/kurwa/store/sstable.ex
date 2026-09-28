defmodule Kurwa.Store.SSTable do
  @moduledoc """
  An immutable, sorted table of records on disk.

  What stays in memory when one of these is open is the part that matters: a
  Bloom filter and a *sparse* index, one entry every #{16} records. Everything
  else lives on disk and is read only when the filter says the key might be
  here. That is what lets a node hold far more keys than it has RAM.

  Layout:

      "KSST1\\n"
      records          len[4] crc32[4] term_to_binary(record), sorted by key
      index            every 16th key with its offset: klen[2] key offset[8]
      bloom            the filter bits
      footer           fixed 49 bytes, ending "KSSTEND"

  The footer is last because the file is written in one forward pass: the sizes
  of the index and the filter are only known once the records are down.

  Reads take one `pread`. The sparse index gives the block a key would be in,
  the block is read whole, and the frames in it are decoded in memory - rather
  than a syscall per record.
  """

  alias Kurwa.Store.Bloom

  @magic "KSST1\n"
  @trailer "KSSTEND"
  @footer_size 49
  @index_every 16
  @max_entry 16_777_216

  defstruct [:path, :fd, :index, :bloom, :bits, :hashes, :live, :records, :data_end]

  @type t :: %__MODULE__{}

  @doc "Writes `records` as a new table at `path`, sorting them first."
  @spec write(Path.t(), [Kurwa.Record.t()]) :: {:ok, non_neg_integer()} | {:error, term()}
  def write(path, records) do
    sorted = Enum.sort_by(records, &elem(&1, 0))
    write_sorted(path, sorted, length(sorted))
  end

  @doc "Writes an already-sorted enumerable, sizing the filter for `capacity`."
  @spec write_sorted(Path.t(), Enumerable.t(), pos_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def write_sorted(path, sorted, capacity) do
    with {:ok, writer} <- writer_open(path, capacity) do
      sorted |> Enum.reduce(writer, &writer_add(&2, &1)) |> writer_close()
    end
  end

  @doc """
  Opens a writer that takes records one at a time, in key order.

  Compaction is driven by `merge/3`, which pushes records at a fold rather than
  yielding them, so the writer has to be the thing that accepts them - otherwise
  the merged output would have to be collected into a list first, which is the
  one thing an on-disk engine must not do.

  `capacity` only sizes the Bloom filter; guessing high costs a few bytes.
  """
  @spec writer_open(Path.t(), pos_integer()) :: {:ok, map()} | {:error, term()}
  def writer_open(path, capacity) do
    with {:ok, fd} <- :file.open(path, [:write, :raw, :binary, {:delayed_write, 512 * 1024, 200}]),
         :ok <- :file.write(fd, @magic) do
      {:ok,
       %{
         path: path,
         fd: fd,
         offset: byte_size(@magic),
         index: [],
         live: 0,
         count: 0,
         bloom: Bloom.new(max(capacity, 1))
       }}
    end
  end

  @doc "Appends one record. Keys must arrive in order."
  @spec writer_add(map(), Kurwa.Record.t()) :: map()
  def writer_add(writer, record) do
    key = elem(record, 0)
    Bloom.add(writer.bloom, key)
    frame = frame(record)
    :ok = :file.write(writer.fd, frame)

    %{
      writer
      | offset: writer.offset + IO.iodata_length(frame),
        index:
          if(rem(writer.count, @index_every) == 0,
            do: [{key, writer.offset} | writer.index],
            else: writer.index
          ),
        live: if(Kurwa.Record.member?(record), do: writer.live + 1, else: writer.live),
        count: writer.count + 1
    }
  end

  @doc "Finishes the table: index, filter, footer, fsync."
  @spec writer_close(map()) :: {:ok, non_neg_integer()} | {:error, term()}
  def writer_close(writer) do
    index = Enum.reverse(writer.index)
    index_binary = encode_index(index)
    bloom_binary = Bloom.to_binary(writer.bloom)

    with :ok <- :file.write(writer.fd, index_binary),
         :ok <- :file.write(writer.fd, bloom_binary),
         :ok <-
           :file.write(writer.fd, [
             <<writer.offset::little-64, length(index)::little-32,
               writer.offset + byte_size(index_binary)::little-64, writer.bloom.bits::little-32,
               writer.bloom.hashes::little-16, writer.live::little-64, writer.count::little-64>>,
             @trailer
           ]),
         :ok <- :file.sync(writer.fd),
         :ok <- :file.close(writer.fd) do
      {:ok, writer.count}
    end
  end

  @doc "Opens a table, loading its filter and sparse index into memory."
  @spec open(Path.t()) :: {:ok, t()} | {:error, term()}
  def open(path) do
    # File.stat, not :file.read_file_info: the latter answers with an Erlang
    # record, which no map pattern will ever match.
    with {:ok, %File.Stat{size: size}} when size > @footer_size <- File.stat(path),
         # deliberately not :raw - a raw handle belongs to the process that
         # opened it, and these are read from whichever process asks
         {:ok, fd} <- :file.open(path, [:read, :binary]),
         {:ok, footer} <- :file.pread(fd, size - @footer_size, @footer_size),
         <<data_end::little-64, index_count::little-32, bloom_offset::little-64, bits::little-32,
           hashes::little-16, live::little-64, records::little-64, @trailer>> <- footer,
         {:ok, index_binary} <- read_at(fd, data_end, bloom_offset - data_end),
         {:ok, bloom} <- read_at(fd, bloom_offset, div(bits, 8)),
         {:ok, index} <- decode_index(index_binary, index_count) do
      {:ok,
       %__MODULE__{
         path: path,
         fd: fd,
         index: index,
         bloom: bloom,
         bits: bits,
         hashes: hashes,
         live: live,
         records: records,
         data_end: data_end
       }}
    else
      {:ok, %File.Stat{}} -> {:error, :too_small}
      {:error, reason} -> {:error, reason}
      :eof -> {:error, :truncated}
      other -> {:error, {:bad_footer, other}}
    end
  end

  @doc "Closes the table's file handle."
  @spec close(t()) :: :ok
  def close(%__MODULE__{fd: fd}), do: :file.close(fd)

  @doc """
  The record for `key`, or `nil`.

  The Bloom filter answers most misses without touching the disk; a hit costs
  one read of the block the sparse index points at.
  """
  @spec get(t(), binary()) :: Kurwa.Record.t() | nil
  def get(%__MODULE__{} = table, key) do
    if Bloom.member?(table.bloom, table.bits, table.hashes, key) do
      {from, to} = block_for(table, key)

      case read_at(table.fd, from, to - from) do
        {:ok, block} -> find(block, key)
        {:error, _reason} -> nil
      end
    end
  end

  @doc "Folds over every record in the table, in key order."
  @spec fold(t(), acc, (Kurwa.Record.t(), acc -> acc)) :: acc when acc: term()
  def fold(%__MODULE__{} = table, acc, fun) do
    scan(table.fd, byte_size(@magic), table.data_end, acc, fun)
  end

  @doc """
  Bytes this table keeps in RAM while open: the filter and the sparse index.

  This is the number the on-disk engine exists for. Everything else about the
  table is on disk.
  """
  @spec memory(t()) :: non_neg_integer()
  def memory(%__MODULE__{bloom: bloom, index: index}) do
    index_bytes =
      index
      |> Tuple.to_list()
      |> Enum.reduce(0, fn {key, _offset}, sum -> sum + byte_size(key) + 16 end)

    byte_size(bloom) + index_bytes
  end

  @doc "Live keys the table held when it was written, and its total record count."
  def stats(%__MODULE__{live: live, records: records}), do: %{live: live, records: records}

  @doc """
  A cursor over the table, in key order.

  `next/1` is the primitive compaction and anti-entropy are built on: merging
  several tables needs to look at one record from each and take the smallest,
  which a fold cannot do. Reads a block at a time, never the whole table.
  """
  @spec reader(t()) :: map()
  def reader(%__MODULE__{} = table) do
    %{fd: table.fd, offset: byte_size(@magic), data_end: table.data_end, buffer: <<>>}
  end

  @doc "A cursor over records already in memory, so a memtable can join a merge."
  @spec list_reader([Kurwa.Record.t()]) :: map()
  def list_reader(records), do: %{list: Enum.sort_by(records, &elem(&1, 0))}

  @doc "The next record and the advanced cursor, or `:done`."
  @spec next(map()) :: {Kurwa.Record.t(), map()} | :done
  def next(%{list: [record | rest]}), do: {record, %{list: rest}}
  def next(%{list: []}), do: :done

  def next(%{buffer: <<len::32, crc::32, payload::binary-size(len), rest::binary>>} = reader) do
    case decode(payload, crc) do
      {:ok, record} -> {record, %{reader | buffer: rest}}
      :error -> :done
    end
  end

  def next(%{offset: offset, data_end: data_end} = reader) when offset < data_end do
    chunk = min(262_144, data_end - offset)

    case read_at(reader.fd, offset, chunk) do
      {:ok, data} ->
        next(%{reader | offset: offset + byte_size(data), buffer: reader.buffer <> data})

      {:error, _reason} ->
        :done
    end
  end

  def next(_reader), do: :done

  @doc """
  Merges cursors into one ordered sequence, one record per key.

  Equal keys are resolved with `Kurwa.Record.merge/2`, so a key present in
  several tables comes out once, as whichever version wins.
  """
  @spec merge([map()], acc, (Kurwa.Record.t(), acc -> acc)) :: acc when acc: term()
  def merge(readers, acc, fun) do
    readers
    |> Enum.map(&next/1)
    |> Enum.reject(&(&1 == :done))
    |> do_merge(acc, fun)
  end

  defp do_merge([], acc, _fun), do: acc

  defp do_merge(heads, acc, fun) do
    smallest = heads |> Enum.map(fn {record, _} -> elem(record, 0) end) |> Enum.min()

    {matching, rest} = Enum.split_with(heads, fn {record, _} -> elem(record, 0) == smallest end)

    winner =
      matching
      |> Enum.map(fn {record, _} -> record end)
      |> Kurwa.Record.merge_all()

    advanced =
      matching
      |> Enum.map(fn {_record, reader} -> next(reader) end)
      |> Enum.reject(&(&1 == :done))

    do_merge(rest ++ advanced, fun.(winner, acc), fun)
  end

  # ------------------------------------------------------------------ private

  defp frame(record) do
    payload = :erlang.term_to_binary(record)
    [<<byte_size(payload)::32, :erlang.crc32(payload)::32>>, payload]
  end

  defp encode_index(index) do
    for {key, offset} <- index,
        into: <<>>,
        do: <<byte_size(key)::little-16, key::binary, offset::little-64>>
  end

  defp decode_index(binary, count), do: decode_index(binary, count, [])

  defp decode_index(_rest, 0, acc), do: {:ok, acc |> Enum.reverse() |> List.to_tuple()}

  defp decode_index(
         <<len::little-16, key::binary-size(len), offset::little-64, rest::binary>>,
         count,
         acc
       ),
       do: decode_index(rest, count - 1, [{key, offset} | acc])

  defp decode_index(_binary, _count, _acc), do: {:error, :bad_index}

  # The block a key would live in: from the last index entry not after it, up to
  # the next one.
  defp block_for(%__MODULE__{index: index, data_end: data_end}, key) do
    size = tuple_size(index)

    case size do
      0 ->
        {byte_size(@magic), data_end}

      _ ->
        i = lower_bound(index, key, 0, size - 1)
        {_, from} = elem(index, i)
        to = if i + 1 < size, do: elem(index, i + 1) |> elem(1), else: data_end
        {from, to}
    end
  end

  # Largest index entry whose key is <= the one we want.
  defp lower_bound(_index, _key, lo, hi) when lo >= hi, do: lo

  defp lower_bound(index, key, lo, hi) do
    mid = div(lo + hi + 1, 2)
    {candidate, _} = elem(index, mid)

    if candidate <= key,
      do: lower_bound(index, key, mid, hi),
      else: lower_bound(index, key, lo, mid - 1)
  end

  defp find(<<len::32, crc::32, payload::binary-size(len), rest::binary>>, key) do
    case decode(payload, crc) do
      {:ok, record} ->
        found = elem(record, 0)

        cond do
          found == key -> record
          found > key -> nil
          true -> find(rest, key)
        end

      :error ->
        nil
    end
  end

  defp find(_partial, _key), do: nil

  defp read_at(_fd, _offset, 0), do: {:ok, <<>>}

  defp read_at(fd, offset, length) when length > 0 do
    case :file.pread(fd, offset, length) do
      {:ok, data} -> {:ok, data}
      :eof -> {:error, :eof}
      error -> error
    end
  end

  defp read_at(_fd, _offset, _length), do: {:error, :bad_range}

  defp scan(_fd, offset, data_end, acc, _fun) when offset >= data_end, do: acc

  defp scan(fd, offset, data_end, acc, fun) do
    chunk = min(1_048_576, data_end - offset)

    case read_at(fd, offset, chunk) do
      {:ok, block} ->
        {acc, consumed} = decode_block(block, acc, fun, 0)

        if consumed == 0 do
          acc
        else
          scan(fd, offset + consumed, data_end, acc, fun)
        end

      {:error, _reason} ->
        acc
    end
  end

  defp decode_block(<<len::32, crc::32, payload::binary-size(len), rest::binary>>, acc, fun, used)
       when len <= @max_entry do
    case decode(payload, crc) do
      {:ok, record} -> decode_block(rest, fun.(record, acc), fun, used + 8 + len)
      :error -> {acc, used}
    end
  end

  defp decode_block(_rest, acc, _fun, used), do: {acc, used}

  defp decode(payload, crc) do
    if :erlang.crc32(payload) == crc do
      case :erlang.binary_to_term(payload) do
        {key, lamport, node, alive?, wall, expires}
        when is_binary(key) and is_integer(lamport) and is_atom(node) and is_boolean(alive?) and
               is_integer(wall) ->
          {:ok, {key, lamport, node, alive?, wall, expires}}

        _ ->
          :error
      end
    else
      :error
    end
  rescue
    ArgumentError -> :error
  end
end
