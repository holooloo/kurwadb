defmodule Kurwa.NineP.ServerTest do
  @moduledoc """
  End-to-end 9P: a real TCP socket, real wire bytes, and the real store behind it.
  """

  use ExUnit.Case, async: false

  import Bitwise, only: [|||: 2]

  import Kurwa.TestHelpers, only: [unique_key: 1]

  alias Kurwa.NineP.Proto

  @dmdir 0x80000000
  @qtdir 0x80
  @oread 0
  @owrite 1

  setup do
    listener = start_supervised!({ThousandIsland, port: 0, handler_module: Kurwa.NineP.Server})
    {:ok, {_ip, port}} = ThousandIsland.listener_info(listener)

    {:ok, socket} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw], 2_000)

    on_exit(fn -> :gen_tcp.close(socket) end)

    {:ok, socket: socket}
  end

  describe "session" do
    test "negotiates 9P2000 and clamps msize", %{socket: socket} do
      assert {:rversion, msize, "9P2000"} = rpc(socket, {:tversion, 8_192, "9P2000"})
      assert msize == 8_192

      assert {:rversion, msize, "9P2000"} = rpc(socket, {:tversion, 1_000_000, "9P2000"})
      assert msize < 1_000_000
    end

    test "declines a version it does not speak", %{socket: socket} do
      assert {:rversion, _msize, "unknown"} = rpc(socket, {:tversion, 8_192, "9P1874"})
    end

    test "answers the .u and .L dialects with plain 9P2000", %{socket: socket} do
      assert {:rversion, _, "9P2000"} = rpc(socket, {:tversion, 8_192, "9P2000.L"})
    end

    test "attach lands on a directory", %{socket: socket} do
      handshake(socket)
      assert {:rattach, {@qtdir, 0, _path}} = attach(socket)
    end

    test "attach refuses a second use of the same fid", %{socket: socket} do
      handshake(socket)
      attach(socket)

      assert {:rerror, message} = rpc(socket, {:tattach, 0, Proto.nofid(), "glenda", ""})
      assert message =~ "already in use"
    end

    test "authentication is declined, not demanded", %{socket: socket} do
      handshake(socket)
      assert {:rerror, message} = rpc(socket, {:tauth, 1, "glenda", ""})
      assert message =~ "not required"
    end

    test "an unknown fid is an error, not a crash", %{socket: socket} do
      handshake(socket)
      assert {:rerror, "unknown fid"} = rpc(socket, {:tstat, 42})
      assert {:rerror, "unknown fid"} = rpc(socket, {:tclunk, 42})
    end
  end

  describe "keys" do
    setup %{socket: socket} do
      handshake(socket)
      attach(socket)
      {:ok, key: unique_key("9p")}
    end

    test "walking to a key that is not a member fails", %{socket: socket, key: key} do
      assert {:rwalk, [{@qtdir, 0, _}]} = rpc(socket, {:twalk, 0, 1, ["keys"]})
      assert {:rerror, "file does not exist"} = rpc(socket, {:twalk, 1, 2, [key]})
    end

    test "create adds the key, and walking to it then succeeds", %{socket: socket, key: key} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["keys"]})

      assert {:rcreate, {0, 0, _}, _iounit} = rpc(socket, {:tcreate, 1, key, 0o666, @owrite})
      assert Kurwa.member?(key)

      assert {:rwalk, [_keys_qid, {0, 0, _}]} = rpc(socket, {:twalk, 0, 2, ["keys", key]})
    end

    test "stat of a key file is an empty read-only file", %{socket: socket, key: key} do
      :ok = Kurwa.add(key)

      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["keys", key]})
      assert {:rstat, stat} = rpc(socket, {:tstat, 1})

      assert stat.name == key
      assert stat.length == 0
      assert Bitwise.band(stat.mode, @dmdir) == 0
    end

    test "remove deletes the key and consumes the fid", %{socket: socket, key: key} do
      :ok = Kurwa.add(key)

      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["keys", key]})
      assert {:rremove} = rpc(socket, {:tremove, 1})
      refute Kurwa.member?(key)

      assert {:rerror, "unknown fid"} = rpc(socket, {:tstat, 1})
    end

    test "reading a key file gives nothing, because a key has no contents", %{
      socket: socket,
      key: key
    } do
      :ok = Kurwa.add(key)

      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["keys", key]})
      assert {:ropen, _qid, _iounit} = rpc(socket, {:topen, 1, @oread})
      assert {:rread, ""} = rpc(socket, {:tread, 1, 0, 4_096})
    end

    test "a partial walk returns the qids it reached and no new fid", %{socket: socket, key: key} do
      assert {:rwalk, [{@qtdir, 0, _}]} = rpc(socket, {:twalk, 0, 1, ["keys", key]})
      # fid 1 was not assigned, since the walk did not finish
      assert {:rerror, "unknown fid"} = rpc(socket, {:tstat, 1})
    end

    test "binary keys go through /b64", %{socket: socket} do
      key = <<0, 255, 7>>
      name = Base.url_encode64(key, padding: false)

      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["b64"]})
      assert {:rcreate, _, _} = rpc(socket, {:tcreate, 1, name, 0o666, @owrite})
      assert Kurwa.member?(key)

      {:rwalk, _} = rpc(socket, {:twalk, 0, 2, ["b64", name]})
      assert {:rstat, stat} = rpc(socket, {:tstat, 2})
      assert stat.name == name
    end

    test "a name that is not base64 in /b64 is refused", %{socket: socket} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["b64"]})
      assert {:rerror, message} = rpc(socket, {:tcreate, 1, "not base64!", 0o666, @owrite})
      assert message =~ "base64url"
    end

    test "a key cannot be created as a directory", %{socket: socket, key: key} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["keys"]})

      assert {:rerror, message} = rpc(socket, {:tcreate, 1, key, @dmdir ||| 0o777, @owrite})
      assert message =~ "not a directory"
    end
  end

  describe "sets" do
    setup %{socket: socket} do
      handshake(socket)
      attach(socket)
      {:ok, key: unique_key("9p-set")}
    end

    test "a key created under /sets/<set> lands in that set", %{socket: socket, key: key} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["sets", "alpha"]})
      assert {:rcreate, _, _} = rpc(socket, {:tcreate, 1, key, 0o666, @owrite})

      assert Kurwa.Namespace.member?("alpha", key) == {:ok, true}
      assert Kurwa.Namespace.member?("beta", key) == {:ok, false}
      assert Kurwa.fetch(key) == {:ok, false}
    end

    test "mkdir of a set succeeds and is a no-op", %{socket: socket} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["sets"]})

      assert {:rcreate, {@qtdir, 0, _}, _} =
               rpc(socket, {:tcreate, 1, "gamma", @dmdir ||| 0o777, @oread})
    end

    test "a set name the store cannot represent is refused", %{socket: socket} do
      # A walk that fails on its *first* element is an Rerror; one that fails
      # later is a short Rwalk, so ask for the bad element on its own.
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["sets"]})
      assert {:rerror, message} = rpc(socket, {:twalk, 1, 2, ["has space"]})
      assert message =~ "set name"

      assert {:rwalk, [{@qtdir, 0, _sets}]} = rpc(socket, {:twalk, 0, 3, ["sets", "has space"]})
    end

    test "a plain file cannot be created directly in /sets", %{socket: socket} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["sets"]})
      assert {:rerror, message} = rpc(socket, {:tcreate, 1, "delta", 0o666, @owrite})
      assert message =~ "sets holds sets"
    end
  end

  describe "control and status files" do
    setup %{socket: socket} do
      handshake(socket)
      attach(socket)
      :ok
    end

    test "the root lists its entries", %{socket: socket} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, []})
      assert {:ropen, _, _} = rpc(socket, {:topen, 1, @oread})
      assert {:rread, data} = rpc(socket, {:tread, 1, 0, 8_192})

      assert {:ok, stats} = Proto.decode_stats(data)
      names = Enum.map(stats, & &1.name)

      assert Enum.sort(names) == ["b64", "ctl", "keys", "ring", "sets", "stats"]
    end

    test "a directory read resumes only on an entry boundary", %{socket: socket} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, []})
      {:ropen, _, _} = rpc(socket, {:topen, 1, @oread})

      {:rread, all} = rpc(socket, {:tread, 1, 0, 8_192})
      {:ok, [first | _]} = Proto.decode_stats(all)
      entry_size = IO.iodata_length(Proto.encode_stat(first))

      # a count that holds exactly one entry returns exactly one entry
      assert {:rread, head} = rpc(socket, {:tread, 1, 0, entry_size})
      assert {:ok, [^first]} = Proto.decode_stats(head)

      # ...and a count too small for even one entry returns nothing, never a
      # half-written stat
      assert {:rread, ""} = rpc(socket, {:tread, 1, 0, entry_size - 1})

      assert {:rread, tail} = rpc(socket, {:tread, 1, entry_size, 8_192})
      assert {:ok, rest} = Proto.decode_stats(tail)
      assert length(rest) == 5
      refute first.name in Enum.map(rest, & &1.name)

      # mid-entry offset is an error, not a garbled entry
      assert {:rerror, "bad directory offset"} = rpc(socket, {:tread, 1, 3, 8_192})
    end

    test "the key directories refuse to be listed", %{socket: socket} do
      for dir <- ["keys", "b64"] do
        {:rwalk, _} = rpc(socket, {:twalk, 0, 1, [dir]})
        assert {:rerror, message} = rpc(socket, {:topen, 1, @oread})
        assert message =~ "no scans"
        {:rclunk} = rpc(socket, {:tclunk, 1})
      end
    end

    test "/sets lists as empty, because sets have no existence of their own", %{socket: socket} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["sets"]})
      assert {:ropen, _, _} = rpc(socket, {:topen, 1, @oread})
      assert {:rread, ""} = rpc(socket, {:tread, 1, 0, 8_192})
    end

    test "stats reports the node", %{socket: socket} do
      assert read_file(socket, "stats") =~ "node #{node()}"
    end

    test "ring reports membership and quorum settings", %{socket: socket} do
      content = read_file(socket, "ring")

      assert content =~ "member #{node()}"
      assert content =~ "vnodes "
      assert content =~ "n 1"
    end

    test "ctl takes one command per write", %{socket: socket} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["ctl"]})
      assert {:ropen, _, _} = rpc(socket, {:topen, 1, @owrite})

      assert {:rwrite, 4} = rpc(socket, {:twrite, 1, 0, "sync"})
      assert {:rwrite, 8} = rpc(socket, {:twrite, 1, 0, "compact\n"})
      assert {:rwrite, 2} = rpc(socket, {:twrite, 1, 0, "gc"})

      assert {:rerror, message} = rpc(socket, {:twrite, 1, 0, "drop everything"})
      assert message =~ "unknown control message"
    end

    test "ctl cannot be read and stats cannot be written", %{socket: socket} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["ctl"]})
      assert {:rerror, message} = rpc(socket, {:topen, 1, @oread})
      assert message =~ "write-only"

      {:rwalk, _} = rpc(socket, {:twalk, 0, 2, ["stats"]})
      assert {:rerror, "permission denied"} = rpc(socket, {:topen, 2, @owrite})
    end

    test "wstat is refused, because a key has nothing to change", %{socket: socket} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["stats"]})
      assert {:rerror, message} = rpc(socket, {:twstat, 1, ""})
      assert message =~ "wstat"
    end

    test "flush is answered even though nothing is ever outstanding", %{socket: socket} do
      assert {:rflush} = rpc(socket, {:tflush, 7})
    end

    test "a fid must be opened before it is read", %{socket: socket} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["stats"]})
      assert {:rerror, "fid is not open"} = rpc(socket, {:tread, 1, 0, 10})
    end

    test "a fid cannot be opened twice", %{socket: socket} do
      {:rwalk, _} = rpc(socket, {:twalk, 0, 1, ["stats"]})
      {:ropen, _, _} = rpc(socket, {:topen, 1, @oread})
      assert {:rerror, "fid is already open"} = rpc(socket, {:topen, 1, @oread})
    end
  end

  test "two messages in one packet are both answered", %{socket: socket} do
    handshake(socket)

    both =
      IO.iodata_to_binary([
        Proto.encode(1, {:tattach, 0, Proto.nofid(), "glenda", ""}),
        Proto.encode(2, {:tstat, 0})
      ])

    :ok = :gen_tcp.send(socket, both)

    assert {:rattach, _} = recv(socket, 1)
    assert {:rstat, %{name: "/"}} = recv(socket, 2)
  end

  defp handshake(socket) do
    {:rversion, _msize, "9P2000"} = rpc(socket, {:tversion, 8_192, "9P2000"})
    :ok
  end

  defp attach(socket), do: rpc(socket, {:tattach, 0, Proto.nofid(), "glenda", ""})

  defp read_file(socket, name) do
    fid = :erlang.unique_integer([:positive]) |> rem(1_000) |> Kernel.+(100)

    {:rwalk, _} = rpc(socket, {:twalk, 0, fid, [name]})
    {:ropen, _, _} = rpc(socket, {:topen, fid, @oread})
    {:rread, data} = rpc(socket, {:tread, fid, 0, 8_192})
    {:rclunk} = rpc(socket, {:tclunk, fid})

    data
  end

  defp rpc(socket, message, tag \\ 1) do
    :ok = :gen_tcp.send(socket, Proto.encode(tag, message))
    recv(socket, tag)
  end

  defp recv(socket, tag) do
    {:ok, <<size::little-32>> = header} = :gen_tcp.recv(socket, 4, 2_000)
    {:ok, body} = :gen_tcp.recv(socket, size - 4, 2_000)

    {:ok, ^tag, reply, ""} = Proto.decode(header <> body)
    reply
  end
end
