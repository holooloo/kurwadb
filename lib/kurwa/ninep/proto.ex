defmodule Kurwa.NineP.Proto do
  @moduledoc """
  9P2000 wire codec: bytes in, tagged tuples out, and back.

  Deliberately pure - no sockets, no state, no kurwadb concepts. The framing is
  tiny: every message is

      size[4] type[1] tag[2] <fields>

  little-endian throughout, with two variable-length conventions that are easy to
  get wrong and are therefore spelled out here:

  * `name[s]` is a 2-byte length followed by that many bytes of UTF-8;
  * `stat[n]` is a 2-byte *count* followed by a stat structure that itself starts
    with its own 2-byte size. The two lengths differ by exactly 2, and clients
    reject the message if they do not.

  Only 9P2000 is spoken. `9P2000.u` and `9P2000.L` extensions are declined by
  replying with the base version, which is what the spec asks for.
  """

  alias Kurwa.NineP.Stat

  @version "9P2000"
  @notag 0xFFFF
  @nofid 0xFFFFFFFF

  # qid.type
  @qtdir 0x80
  @qtfile 0x00

  # stat.mode
  @dmdir 0x80000000

  # Topen/Tcreate mode
  @oread 0
  @owrite 1
  @ordwr 2
  @oexec 3
  @otrunc 0x10
  @orclose 0x40

  @types %{
    tversion: 100,
    rversion: 101,
    tauth: 102,
    rauth: 103,
    tattach: 104,
    rattach: 105,
    rerror: 107,
    tflush: 108,
    rflush: 109,
    twalk: 110,
    rwalk: 111,
    topen: 112,
    ropen: 113,
    tcreate: 114,
    rcreate: 115,
    tread: 116,
    rread: 117,
    twrite: 118,
    rwrite: 119,
    tclunk: 120,
    rclunk: 121,
    tremove: 122,
    rremove: 123,
    tstat: 124,
    rstat: 125,
    twstat: 126,
    rwstat: 127
  }

  @codes Map.new(@types, fn {name, code} -> {code, name} end)

  def version, do: @version
  def notag, do: @notag
  def nofid, do: @nofid
  def qtdir, do: @qtdir
  def qtfile, do: @qtfile
  def dmdir, do: @dmdir

  @doc "Open modes, for readers of `Topen`."
  def modes, do: %{oread: @oread, owrite: @owrite, ordwr: @ordwr, oexec: @oexec}
  def otrunc, do: @otrunc
  def orclose, do: @orclose

  @doc """
  Pulls one message off the front of `buffer`.

  `:more` means the buffer holds only part of a message - read again and retry.
  """
  @spec decode(binary()) ::
          {:ok, non_neg_integer(), tuple(), binary()} | :more | {:error, term()}
  def decode(<<size::little-32, _::binary>>) when size < 7, do: {:error, {:short_message, size}}

  def decode(<<size::little-32, rest::binary>> = all) when byte_size(all) >= size do
    payload_size = size - 4
    <<payload::binary-size(^payload_size), tail::binary>> = rest
    <<type, tag::little-16, fields::binary>> = payload

    case Map.fetch(@codes, type) do
      {:ok, name} ->
        case decode_body(name, fields) do
          {:ok, message} -> {:ok, tag, message, tail}
          {:error, reason} -> {:error, reason}
        end

      :error ->
        {:error, {:unknown_type, type}}
    end
  end

  def decode(_partial), do: :more

  @doc "Encodes a reply (or request) with its tag."
  @spec encode(non_neg_integer(), tuple()) :: iodata()
  def encode(tag, message) do
    {name, fields} = encode_body(message)
    payload = [<<Map.fetch!(@types, name), tag::little-16>>, fields]
    size = 4 + IO.iodata_length(payload)
    [<<size::little-32>>, payload]
  end

  @doc "Encodes a `{type, version, path}` qid."
  def encode_qid({type, version, path}) do
    <<type::8, version::little-32, path::little-64>>
  end

  @doc "Encodes a stat structure, including its own leading size."
  def encode_stat(%Stat{} = stat) do
    body = [
      <<stat.type::little-16, stat.dev::little-32>>,
      encode_qid(stat.qid),
      <<stat.mode::little-32, stat.atime::little-32, stat.mtime::little-32,
        stat.length::little-64>>,
      string(stat.name),
      string(stat.uid),
      string(stat.gid),
      string(stat.muid)
    ]

    [<<IO.iodata_length(body)::little-16>>, body]
  end

  @doc "A 2-byte length followed by the string itself."
  def string(value) when is_binary(value), do: [<<byte_size(value)::little-16>>, value]

  defp decode_body(:tversion, <<msize::little-32, rest::binary>>) do
    with {:ok, version, _} <- take_string(rest), do: {:ok, {:tversion, msize, version}}
  end

  defp decode_body(:rversion, <<msize::little-32, rest::binary>>) do
    with {:ok, version, _} <- take_string(rest), do: {:ok, {:rversion, msize, version}}
  end

  defp decode_body(:tauth, <<afid::little-32, rest::binary>>) do
    with {:ok, uname, rest} <- take_string(rest),
         {:ok, aname, _} <- take_string(rest) do
      {:ok, {:tauth, afid, uname, aname}}
    end
  end

  defp decode_body(:tattach, <<fid::little-32, afid::little-32, rest::binary>>) do
    with {:ok, uname, rest} <- take_string(rest),
         {:ok, aname, _} <- take_string(rest) do
      {:ok, {:tattach, fid, afid, uname, aname}}
    end
  end

  defp decode_body(:rattach, <<qid::binary-size(13)>>), do: {:ok, {:rattach, decode_qid(qid)}}

  defp decode_body(:twalk, <<fid::little-32, newfid::little-32, count::little-16, rest::binary>>) do
    with {:ok, names} <- take_strings(rest, count, []) do
      {:ok, {:twalk, fid, newfid, names}}
    end
  end

  defp decode_body(:rwalk, <<count::little-16, rest::binary>>) do
    expected = count * 13

    case rest do
      <<qids::binary-size(^expected), _::binary>> ->
        {:ok, {:rwalk, for(<<qid::binary-size(13) <- qids>>, do: decode_qid(qid))}}

      _ ->
        {:error, :bad_rwalk}
    end
  end

  defp decode_body(:topen, <<fid::little-32, mode::8>>), do: {:ok, {:topen, fid, mode}}

  defp decode_body(:ropen, <<qid::binary-size(13), iounit::little-32>>),
    do: {:ok, {:ropen, decode_qid(qid), iounit}}

  defp decode_body(:tcreate, <<fid::little-32, rest::binary>>) do
    with {:ok, name, <<perm::little-32, mode::8>>} <- take_string(rest) do
      {:ok, {:tcreate, fid, name, perm, mode}}
    else
      {:ok, _name, _rest} -> {:error, :bad_tcreate}
      error -> error
    end
  end

  defp decode_body(:rcreate, <<qid::binary-size(13), iounit::little-32>>),
    do: {:ok, {:rcreate, decode_qid(qid), iounit}}

  defp decode_body(:tread, <<fid::little-32, offset::little-64, count::little-32>>),
    do: {:ok, {:tread, fid, offset, count}}

  defp decode_body(:rread, <<count::little-32, rest::binary>>) do
    case rest do
      <<data::binary-size(^count), _::binary>> -> {:ok, {:rread, data}}
      _ -> {:error, :bad_rread}
    end
  end

  defp decode_body(:twrite, <<fid::little-32, offset::little-64, count::little-32, rest::binary>>) do
    case rest do
      <<data::binary-size(^count), _::binary>> -> {:ok, {:twrite, fid, offset, data}}
      _ -> {:error, :bad_twrite}
    end
  end

  defp decode_body(:rwrite, <<count::little-32>>), do: {:ok, {:rwrite, count}}
  defp decode_body(:tclunk, <<fid::little-32>>), do: {:ok, {:tclunk, fid}}
  defp decode_body(:rclunk, <<>>), do: {:ok, {:rclunk}}
  defp decode_body(:tremove, <<fid::little-32>>), do: {:ok, {:tremove, fid}}
  defp decode_body(:rremove, <<>>), do: {:ok, {:rremove}}
  defp decode_body(:tstat, <<fid::little-32>>), do: {:ok, {:tstat, fid}}

  defp decode_body(:rstat, <<_n::little-16, rest::binary>>) do
    case decode_stat(rest) do
      {:ok, stat} -> {:ok, {:rstat, stat}}
      error -> error
    end
  end

  defp decode_body(:twstat, <<fid::little-32, _n::little-16, rest::binary>>),
    do: {:ok, {:twstat, fid, rest}}

  defp decode_body(:rwstat, <<>>), do: {:ok, {:rwstat}}
  defp decode_body(:tflush, <<oldtag::little-16>>), do: {:ok, {:tflush, oldtag}}
  defp decode_body(:rflush, <<>>), do: {:ok, {:rflush}}

  defp decode_body(:rerror, rest) do
    with {:ok, ename, _} <- take_string(rest), do: {:ok, {:rerror, ename}}
  end

  defp decode_body(name, _fields), do: {:error, {:bad_body, name}}

  defp encode_body({:rversion, msize, version}),
    do: {:rversion, [<<msize::little-32>>, string(version)]}

  defp encode_body({:tversion, msize, version}),
    do: {:tversion, [<<msize::little-32>>, string(version)]}

  defp encode_body({:tattach, fid, afid, uname, aname}),
    do: {:tattach, [<<fid::little-32, afid::little-32>>, string(uname), string(aname)]}

  defp encode_body({:rattach, qid}), do: {:rattach, encode_qid(qid)}
  defp encode_body({:rauth, qid}), do: {:rauth, encode_qid(qid)}

  defp encode_body({:tauth, afid, uname, aname}),
    do: {:tauth, [<<afid::little-32>>, string(uname), string(aname)]}

  defp encode_body({:rerror, ename}), do: {:rerror, string(ename)}

  defp encode_body({:twalk, fid, newfid, names}) do
    {:twalk,
     [<<fid::little-32, newfid::little-32, length(names)::little-16>>, Enum.map(names, &string/1)]}
  end

  defp encode_body({:rwalk, qids}),
    do: {:rwalk, [<<length(qids)::little-16>>, Enum.map(qids, &encode_qid/1)]}

  defp encode_body({:topen, fid, mode}), do: {:topen, <<fid::little-32, mode::8>>}
  defp encode_body({:ropen, qid, iounit}), do: {:ropen, [encode_qid(qid), <<iounit::little-32>>]}

  defp encode_body({:tcreate, fid, name, perm, mode}),
    do: {:tcreate, [<<fid::little-32>>, string(name), <<perm::little-32, mode::8>>]}

  defp encode_body({:rcreate, qid, iounit}),
    do: {:rcreate, [encode_qid(qid), <<iounit::little-32>>]}

  defp encode_body({:tread, fid, offset, count}),
    do: {:tread, <<fid::little-32, offset::little-64, count::little-32>>}

  defp encode_body({:rread, data}), do: {:rread, [<<IO.iodata_length(data)::little-32>>, data]}

  defp encode_body({:twrite, fid, offset, data}),
    do: {:twrite, [<<fid::little-32, offset::little-64, byte_size(data)::little-32>>, data]}

  defp encode_body({:rwrite, count}), do: {:rwrite, <<count::little-32>>}
  defp encode_body({:tclunk, fid}), do: {:tclunk, <<fid::little-32>>}
  defp encode_body({:rclunk}), do: {:rclunk, <<>>}
  defp encode_body({:tremove, fid}), do: {:tremove, <<fid::little-32>>}
  defp encode_body({:rremove}), do: {:rremove, <<>>}
  defp encode_body({:tstat, fid}), do: {:tstat, <<fid::little-32>>}

  defp encode_body({:rstat, %Stat{} = stat}) do
    blob = encode_stat(stat)
    {:rstat, [<<IO.iodata_length(blob)::little-16>>, blob]}
  end

  defp encode_body({:twstat, fid, stat}),
    do: {:twstat, [<<fid::little-32, byte_size(stat)::little-16>>, stat]}

  defp encode_body({:rwstat}), do: {:rwstat, <<>>}
  defp encode_body({:tflush, oldtag}), do: {:tflush, <<oldtag::little-16>>}
  defp encode_body({:rflush}), do: {:rflush, <<>>}

  @doc "Decodes a whole directory read into its stat entries."
  @spec decode_stats(binary()) :: {:ok, [Stat.t()]} | {:error, term()}
  def decode_stats(<<>>), do: {:ok, []}

  def decode_stats(<<size::little-16, rest::binary>>) when byte_size(rest) >= size do
    <<body::binary-size(^size), tail::binary>> = rest

    with {:ok, stat} <- decode_stat(<<size::little-16, body::binary>>),
         {:ok, more} <- decode_stats(tail) do
      {:ok, [stat | more]}
    end
  end

  def decode_stats(_partial), do: {:error, :truncated_stat_stream}

  defp decode_qid(<<type::8, version::little-32, path::little-64>>), do: {type, version, path}

  @doc """
  Decodes one stat structure, including its own leading size.

  Public because a client reading a directory gets a stream of these and has to
  take them apart; `decode_stats/1` does the stream.
  """
  @spec decode_stat(binary()) :: {:ok, Stat.t()} | {:error, term()}
  def decode_stat(
        <<_size::little-16, type::little-16, dev::little-32, qid::binary-size(13),
          mode::little-32, atime::little-32, mtime::little-32, length::little-64, rest::binary>>
      ) do
    with {:ok, name, rest} <- take_string(rest),
         {:ok, uid, rest} <- take_string(rest),
         {:ok, gid, rest} <- take_string(rest),
         {:ok, muid, _rest} <- take_string(rest) do
      {:ok,
       %Stat{
         type: type,
         dev: dev,
         qid: decode_qid(qid),
         mode: mode,
         atime: atime,
         mtime: mtime,
         length: length,
         name: name,
         uid: uid,
         gid: gid,
         muid: muid
       }}
    end
  end

  def decode_stat(_), do: {:error, :bad_stat}

  defp take_string(<<len::little-16, rest::binary>>) when byte_size(rest) >= len do
    <<value::binary-size(^len), tail::binary>> = rest
    {:ok, value, tail}
  end

  defp take_string(_), do: {:error, :bad_string}

  defp take_strings(_rest, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp take_strings(rest, count, acc) do
    with {:ok, value, tail} <- take_string(rest) do
      take_strings(tail, count - 1, [value | acc])
    end
  end
end
