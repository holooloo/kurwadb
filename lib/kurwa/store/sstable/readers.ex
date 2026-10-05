defmodule Kurwa.Store.SSTable.Readers do
  @moduledoc """
  Long-lived processes that own the raw file handles, one per scheduler.

  `Kurwa.Store.SSTable.Fd` caches a raw handle per process, which only pays off
  in a process that lives. The read path has none: `Kurwa.Quorum` runs every
  replica call in a fresh worker, and a call from another node arrives through
  `:erpc`, which spawns one too. So each read opened the file, read one block
  and closed it again as the worker died - 28.7 µs from a fresh process against
  2.55 µs from one that already held the handle, and slower than the shared
  handle the raw ones replaced.

  A read now goes to the reader for the caller's scheduler: one message there
  with the path and the range, one back with the block. The reader opens each
  table once and keeps it. Blocks are larger than 64 bytes, so the reply is a
  reference-counted binary and nothing is copied.

  When the pool is not running - a bench or a test that opens an engine without
  the application - reads fall back to a handle in the calling process.
  """

  alias Kurwa.Store.SSTable.Fd

  @pt {__MODULE__, :pids}
  @timeout 5_000

  @doc false
  def child_spec(_opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, []}, type: :supervisor}
  end

  @doc "Starts one reader per online scheduler under a supervisor."
  def start_link do
    count = System.schedulers_online()

    children =
      for i <- 1..count do
        %{id: {:reader, i}, start: {__MODULE__, :start_reader, [i]}}
      end

    with {:ok, sup} <- Supervisor.start_link(children, strategy: :one_for_one) do
      publish()
      {:ok, sup}
    end
  end

  @doc false
  def start_reader(i) do
    pid = spawn_link(fn -> loop() end)
    Process.register(pid, name(i))
    # A reader restarted by the supervisor has a new pid; republish once it is
    # registered so callers stop sending to the dead one.
    if :persistent_term.get(@pt, nil), do: publish()
    {:ok, pid}
  end

  @doc "Reads `length` bytes of `path` at `offset`."
  @spec pread(Path.t(), non_neg_integer(), pos_integer()) ::
          {:ok, binary()} | :eof | {:error, term()}
  def pread(path, offset, length) do
    case :persistent_term.get(@pt, nil) do
      nil ->
        local(path, offset, length)

      pids ->
        reader = elem(pids, rem(:erlang.system_info(:scheduler_id), tuple_size(pids)))
        ref = Process.monitor(reader)
        send(reader, {:pread, self(), ref, path, offset, length})

        receive do
          {^ref, reply} ->
            Process.demonitor(ref, [:flush])
            reply

          {:DOWN, ^ref, :process, _, _} ->
            local(path, offset, length)
        after
          @timeout ->
            Process.demonitor(ref, [:flush])
            {:error, :timeout}
        end
    end
  end

  @doc "Closes every reader's handle for `path`, for a table that is going away."
  @spec release(Path.t()) :: :ok
  def release(path) do
    case :persistent_term.get(@pt, nil) do
      nil -> :ok
      pids -> pids |> Tuple.to_list() |> Enum.each(&send(&1, {:release, path}))
    end

    Fd.release(path)
  end

  @doc "Whether reads are going through the pool."
  def running?, do: :persistent_term.get(@pt, nil) != nil

  defp local(path, offset, length) do
    with {:ok, fd} <- Fd.for(path), do: :file.pread(fd, offset, length)
  end

  defp loop do
    receive do
      {:pread, from, ref, path, offset, length} ->
        reply =
          case Fd.for(path) do
            {:ok, fd} -> :file.pread(fd, offset, length)
            error -> error
          end

        send(from, {ref, reply})

      {:release, path} ->
        Fd.release(path)
    end

    loop()
  end

  defp publish do
    pids =
      1..System.schedulers_online()
      |> Enum.map(&Process.whereis(name(&1)))

    if Enum.all?(pids, &is_pid/1), do: :persistent_term.put(@pt, List.to_tuple(pids))
  end

  defp name(i), do: :"kurwa_sstable_reader_#{i}"
end
