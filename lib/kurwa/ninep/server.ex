defmodule Kurwa.NineP.Server do
  @moduledoc """
  9P2000 server: one process per connection, one fid table per connection.

  Requests are served strictly in order. 9P allows a client to have several
  requests in flight and to `Tflush` the ones it no longer wants, but every
  kurwadb operation is a bounded quorum round trip, so serialising them keeps the
  fid table trivially consistent and makes `Tflush` a no-op that is still correct
  (there is never anything outstanding to cancel).

  This is a *client* frontend only. Nodes replicate to each other over Erlang
  distribution, never over 9P: 9P is stateful and walk-oriented, which is the
  wrong shape for fan-out to replicas, and it has nothing to say about quorums.
  """

  use ThousandIsland.Handler

  alias Kurwa.NineP.Fs
  alias Kurwa.NineP.Proto

  require Logger

  import Bitwise, only: [&&&: 2]

  # Big enough that a directory read is one round trip, small enough to bound a
  # connection's memory. Clients negotiate down from here in Tversion.
  @max_msize 65_560
  @min_msize 512

  # Rread header: size[4] type[1] tag[2] count[4]
  @rread_overhead 11

  @impl ThousandIsland.Handler
  def handle_connection(socket, _state) do
    Kurwa.Metrics.connect(:ninep, socket)
    {:continue, fresh(@max_msize)}
  end

  @impl ThousandIsland.Handler
  def handle_data(data, socket, state) do
    dispatch(state.buffer <> data, socket, state)
  end

  defp fresh(msize), do: %{msize: msize, buffer: <<>>, fids: %{}}

  defp dispatch(buffer, socket, state) do
    case Proto.decode(buffer) do
      {:ok, tag, message, rest} ->
        {reply, state} = handle_message(message, state)
        :ok = ThousandIsland.Socket.send(socket, Proto.encode(tag, reply))
        dispatch(rest, socket, state)

      :more ->
        {:continue, %{state | buffer: buffer}}

      {:error, reason} ->
        Logger.warning("kurwadb 9p: dropping connection, undecodable message: #{inspect(reason)}")
        {:close, %{state | buffer: <<>>}}
    end
  end

  # Tversion resets the session: every fid is forgotten, per the spec.
  defp handle_message({:tversion, msize, version}, _state) do
    negotiated = msize |> min(@max_msize) |> max(@min_msize)

    if String.starts_with?(version, Proto.version()) do
      {{:rversion, negotiated, Proto.version()}, fresh(negotiated)}
    else
      {{:rversion, negotiated, "unknown"}, fresh(negotiated)}
    end
  end

  defp handle_message({:tauth, _afid, _uname, _aname}, state) do
    {{:rerror, "authentication is not required"}, state}
  end

  defp handle_message({:tattach, fid, _afid, _uname, aname}, state) do
    cond do
      Map.has_key?(state.fids, fid) ->
        {{:rerror, "fid is already in use"}, state}

      aname not in ["", "/"] ->
        {{:rerror, "kurwadb exports a single tree; attach with an empty aname"}, state}

      true ->
        root = Fs.root()
        {{:rattach, Fs.qid(root)}, put_fid(state, fid, root)}
    end
  end

  defp handle_message({:twalk, fid, newfid, names}, state) do
    with {:ok, entry} <- fetch_fid(state, fid),
         :ok <- ensure_closed(entry),
         :ok <- ensure_free(state, fid, newfid) do
      case walk_all(entry.path, names, []) do
        # Everything resolved: newfid now points at the destination.
        {:ok, path, qids} ->
          {{:rwalk, qids}, put_fid(state, newfid, path)}

        # Nothing resolved and there was something to resolve: report why.
        {:error, reason, []} ->
          {{:rerror, reason}, state}

        # Partial walk: 9P wants the qids we did reach and no new fid.
        {:error, _reason, qids} ->
          {{:rwalk, qids}, state}
      end
    else
      {:error, reason} -> {{:rerror, reason}, state}
    end
  end

  defp handle_message({:topen, fid, mode}, state) do
    with {:ok, entry} <- fetch_fid(state, fid),
         :ok <- ensure_closed(entry),
         :ok <- Fs.openable(entry.path, mode),
         {:ok, entry} <- open_entry(entry, mode) do
      {{:ropen, Fs.qid(entry.path), 0}, %{state | fids: Map.put(state.fids, fid, entry)}}
    else
      {:error, reason} -> {{:rerror, reason}, state}
    end
  end

  defp handle_message({:tcreate, fid, name, perm, mode}, state) do
    with {:ok, entry} <- fetch_fid(state, fid),
         :ok <- ensure_closed(entry),
         :ok <- valid_name(name),
         {:ok, path} <- Fs.create(entry.path, name, perm) do
      # Contents stay unresolved here: a fresh set directory cannot be listed, and
      # that must not make an otherwise successful create look like a failure.
      entry = %{entry | path: path, mode: mode, contents: :lazy}
      {{:rcreate, Fs.qid(path), 0}, %{state | fids: Map.put(state.fids, fid, entry)}}
    else
      {:error, reason} -> {{:rerror, reason}, state}
    end
  end

  defp handle_message({:tread, fid, offset, count}, state) do
    count = min(count, state.msize - @rread_overhead)

    with {:ok, entry} <- fetch_fid(state, fid),
         :ok <- ensure_open(entry),
         {:ok, entry} <- resolve(entry) do
      state = %{state | fids: Map.put(state.fids, fid, entry)}

      case entry.contents do
        {:file, data} -> {{:rread, slice(data, offset, count)}, state}
        {:dir, entries} -> {dir_read(entries, offset, count), state}
      end
    else
      {:error, reason} -> {{:rerror, reason}, state}
    end
  end

  defp handle_message({:twrite, fid, _offset, data}, state) do
    with {:ok, entry} <- fetch_fid(state, fid),
         :ok <- ensure_open(entry),
         {:ok, written} <- Fs.write(entry.path, data) do
      {{:rwrite, written}, state}
    else
      {:error, reason} -> {{:rerror, reason}, state}
    end
  end

  defp handle_message({:tclunk, fid}, state) do
    case fetch_fid(state, fid) do
      {:ok, _entry} -> {{:rclunk}, %{state | fids: Map.delete(state.fids, fid)}}
      {:error, reason} -> {{:rerror, reason}, state}
    end
  end

  # The fid goes away whether or not the removal worked - that is what the spec
  # says, and it is what keeps a client's fid table in step with ours.
  defp handle_message({:tremove, fid}, state) do
    case fetch_fid(state, fid) do
      {:ok, entry} ->
        state = %{state | fids: Map.delete(state.fids, fid)}

        case Fs.remove(entry.path) do
          :ok -> {{:rremove}, state}
          {:error, reason} -> {{:rerror, reason}, state}
        end

      {:error, reason} ->
        {{:rerror, reason}, state}
    end
  end

  defp handle_message({:tstat, fid}, state) do
    case fetch_fid(state, fid) do
      {:ok, entry} -> {{:rstat, Fs.stat(entry.path)}, state}
      {:error, reason} -> {{:rerror, reason}, state}
    end
  end

  defp handle_message({:twstat, _fid, _stat}, state) do
    {{:rerror, "a key has nothing to change: wstat is not supported"}, state}
  end

  defp handle_message({:tflush, _oldtag}, state) do
    {{:rflush}, state}
  end

  defp handle_message(other, state) do
    {{:rerror, "unexpected message: #{inspect(elem(other, 0))}"}, state}
  end

  defp put_fid(state, fid, path) do
    %{state | fids: Map.put(state.fids, fid, %{path: path, mode: nil, contents: nil})}
  end

  defp fetch_fid(state, fid) do
    case Map.fetch(state.fids, fid) do
      {:ok, entry} -> {:ok, entry}
      :error -> {:error, "unknown fid"}
    end
  end

  defp ensure_free(state, fid, newfid) do
    if newfid == fid or not Map.has_key?(state.fids, newfid),
      do: :ok,
      else: {:error, "fid is already in use"}
  end

  defp ensure_closed(%{mode: nil}), do: :ok
  defp ensure_closed(_entry), do: {:error, "fid is already open"}

  defp ensure_open(%{mode: nil}), do: {:error, "fid is not open"}
  defp ensure_open(_entry), do: :ok

  # Contents are snapshotted at open time, so a read sequence sees one consistent
  # view even while another client is writing. A write-only open reads nothing,
  # so it must not fail on a path that cannot be read at all (/ctl).
  defp open_entry(entry, mode) do
    if (mode &&& 3) in [1, 2] do
      {:ok, %{entry | mode: mode, contents: {:file, ""}}}
    else
      with {:ok, contents} <- snapshot(entry.path) do
        {:ok, %{entry | mode: mode, contents: contents}}
      end
    end
  end

  defp resolve(%{contents: :lazy} = entry) do
    with {:ok, contents} <- snapshot(entry.path), do: {:ok, %{entry | contents: contents}}
  end

  defp resolve(entry), do: {:ok, entry}

  defp snapshot(path) do
    case Fs.contents(path) do
      {:file, data} -> {:ok, {:file, data}}
      {:dir, stats} -> {:ok, {:dir, encode_entries(stats)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp encode_entries(stats) do
    Enum.map(stats, fn stat -> IO.iodata_to_binary(Proto.encode_stat(stat)) end)
  end

  defp walk_all(path, [], qids), do: {:ok, path, Enum.reverse(qids)}

  defp walk_all(path, [name | rest], qids) do
    case Fs.walk(path, name) do
      {:ok, next} -> walk_all(next, rest, [Fs.qid(next) | qids])
      {:error, reason} -> {:error, reason, Enum.reverse(qids)}
    end
  end

  defp valid_name(name) do
    cond do
      name in ["", ".", ".."] -> {:error, "invalid name"}
      String.contains?(name, "/") -> {:error, "a name cannot contain /"}
      String.contains?(name, <<0>>) -> {:error, "a name cannot contain NUL"}
      true -> :ok
    end
  end

  defp slice(data, offset, count) do
    size = byte_size(data)

    if offset >= size do
      ""
    else
      binary_part(data, offset, min(count, size - offset))
    end
  end

  # A directory read must return whole stat entries, and a client may only resume
  # at an offset that fell on an entry boundary.
  defp dir_read(entries, offset, count) do
    case seek(entries, offset) do
      :error -> {:rerror, "bad directory offset"}
      rest -> {:rread, take(rest, count, [])}
    end
  end

  defp seek(entries, 0), do: entries
  defp seek([], _offset), do: :error

  defp seek([entry | rest], offset) do
    size = byte_size(entry)
    if offset >= size, do: seek(rest, offset - size), else: :error
  end

  defp take([], _count, acc), do: IO.iodata_to_binary(Enum.reverse(acc))

  defp take([entry | rest], count, acc) do
    size = byte_size(entry)

    if size <= count,
      do: take(rest, count - size, [entry | acc]),
      else: IO.iodata_to_binary(Enum.reverse(acc))
  end
end
