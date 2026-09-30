defmodule Kurwa.Store.SSTable.Fd do
  @moduledoc """
  Raw file handles, one set per reading process.

  A `:raw` handle in Erlang is a direct syscall; a plain one is a message round
  trip to the process that owns the file. Measured on this store: 1.46 µs
  against 6.41 µs for the same 1 KB read - and that read was 83% of an on-disk
  lookup, so it was the whole cost of reading from disk.

  The catch is that a raw handle belongs to the process that opened it, so a
  table cannot keep one and hand it round. Each process opens its own on first
  use and keeps it in its own dictionary instead: one `open` per process per
  table, and nothing after that.

  The cache is capped. A table that compaction removed is deleted from disk, but
  a process still holding its handle keeps the inode alive, so the oldest
  handles are closed once there are more than #{16} of them. Nothing reads a
  compacted table by accident - callers take the table list from
  `:persistent_term`, which the compaction replaced - so a stale handle is a
  leak to bound, not a correctness problem.
  """

  @key :kurwa_sstable_fds
  @limit 16

  @doc "A raw read handle for `path`, opening one for this process if needed."
  @spec for(Path.t()) :: {:ok, term()} | {:error, term()}
  def for(path) do
    {handles, order} = state()

    case Map.fetch(handles, path) do
      {:ok, fd} ->
        {:ok, fd}

      :error ->
        case :file.open(path, [:read, :raw, :binary]) do
          {:ok, fd} ->
            {handles, order} = evict(Map.put(handles, path, fd), order ++ [path])
            Process.put(@key, {handles, order})
            {:ok, fd}

          error ->
            error
        end
    end
  end

  @doc "Closes this process's handle for `path`, if it has one."
  @spec release(Path.t()) :: :ok
  def release(path) do
    {handles, order} = state()

    case Map.pop(handles, path) do
      {nil, _handles} ->
        :ok

      {fd, handles} ->
        :file.close(fd)
        Process.put(@key, {handles, List.delete(order, path)})
        :ok
    end
  end

  @doc "Closes every handle this process holds."
  @spec release_all() :: :ok
  def release_all do
    {handles, _order} = state()
    Enum.each(handles, fn {_path, fd} -> :file.close(fd) end)
    Process.delete(@key)
    :ok
  end

  @doc "How many handles this process is holding."
  @spec count() :: non_neg_integer()
  def count do
    {handles, _order} = state()
    map_size(handles)
  end

  defp state, do: Process.get(@key, {%{}, []})

  defp evict(handles, order) when map_size(handles) <= @limit, do: {handles, order}

  defp evict(handles, [oldest | rest]) do
    {fd, handles} = Map.pop(handles, oldest)
    if fd, do: :file.close(fd)
    evict(handles, rest)
  end
end
