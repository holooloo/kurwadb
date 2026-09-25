defmodule Kurwa.NineP.ProtoTest do
  use ExUnit.Case, async: true

  import Bitwise, only: [|||: 2]

  alias Kurwa.NineP.Proto
  alias Kurwa.NineP.Stat

  @messages [
    {:tversion, 8192, "9P2000"},
    {:rversion, 8192, "9P2000"},
    {:tattach, 1, 0xFFFFFFFF, "glenda", ""},
    {:rattach, {0x80, 0, 1}},
    {:twalk, 1, 2, ["keys", "order:1"]},
    {:twalk, 1, 2, []},
    {:rwalk, [{0x80, 0, 5}, {0, 0, 99}]},
    {:topen, 2, 0},
    {:ropen, {0, 0, 7}, 0},
    {:tcreate, 2, "order:1", 0o666, 1},
    {:rcreate, {0, 0, 7}, 0},
    {:tread, 2, 0, 4096},
    {:rread, "some bytes"},
    {:twrite, 2, 0, "compact"},
    {:rwrite, 7},
    {:tclunk, 2},
    {:rclunk},
    {:tremove, 2},
    {:rremove},
    {:tstat, 2},
    {:twstat, 2, ""},
    {:rwstat},
    {:tflush, 17},
    {:rflush},
    {:rerror, "file does not exist"}
  ]

  test "every message survives a round trip" do
    for message <- @messages do
      encoded = message |> then(&Proto.encode(7, &1)) |> IO.iodata_to_binary()

      assert {:ok, 7, ^message, ""} = Proto.decode(encoded),
             "failed to round-trip #{inspect(message)}"
    end
  end

  test "a stat round-trips through Rstat" do
    stat = %Stat{
      qid: {0x80, 3, 12_345},
      mode: 0x80000000 ||| 0o555,
      atime: 1,
      mtime: 2,
      length: 42,
      name: "keys",
      uid: "kurwa",
      gid: "kurwa",
      muid: "kurwa"
    }

    encoded = IO.iodata_to_binary(Proto.encode(9, {:rstat, stat}))

    assert {:ok, 9, {:rstat, decoded}, ""} = Proto.decode(encoded)
    assert decoded == stat
  end

  test "the two stat lengths differ by exactly two, as clients require" do
    blob = IO.iodata_to_binary(Proto.encode_stat(%Stat{name: "k"}))
    <<inner::little-16, _rest::binary>> = blob

    assert inner + 2 == byte_size(blob)

    # and in Rstat the outer count wraps the whole blob
    <<_size::little-32, _type, _tag::little-16, outer::little-16, rest::binary>> =
      IO.iodata_to_binary(Proto.encode(1, {:rstat, %Stat{name: "k"}}))

    assert outer == byte_size(rest)
    assert outer == byte_size(blob)
  end

  test "size covers the whole message including itself" do
    encoded = IO.iodata_to_binary(Proto.encode(1, {:tclunk, 3}))

    assert <<size::little-32, _::binary>> = encoded
    assert size == byte_size(encoded)
    # size[4] type[1] tag[2] fid[4]
    assert size == 11
  end

  test "a partial message asks for more bytes instead of guessing" do
    encoded = IO.iodata_to_binary(Proto.encode(1, {:twalk, 1, 2, ["keys", "x"]}))

    for cut <- 1..(byte_size(encoded) - 1) do
      assert Proto.decode(binary_part(encoded, 0, cut)) == :more
    end

    assert {:ok, 1, _, ""} = Proto.decode(encoded)
  end

  test "decodes messages that arrive glued together, and keeps the remainder" do
    first = IO.iodata_to_binary(Proto.encode(1, {:tclunk, 1}))
    second = IO.iodata_to_binary(Proto.encode(2, {:tstat, 9}))

    assert {:ok, 1, {:tclunk, 1}, rest} = Proto.decode(first <> second <> "extra")
    assert {:ok, 2, {:tstat, 9}, "extra"} = Proto.decode(rest)
  end

  test "rejects a message type it does not speak" do
    assert {:error, {:unknown_type, 200}} = Proto.decode(<<7::little-32, 200, 1::little-16>>)
  end

  test "rejects a size that cannot hold a header" do
    assert {:error, {:short_message, 4}} = Proto.decode(<<4::little-32, 0, 0, 0>>)
  end

  test "little-endian, not big-endian" do
    assert IO.iodata_to_binary(Proto.encode(0x0102, {:tclunk, 0x03040506})) ==
             <<11, 0, 0, 0, 120, 2, 1, 6, 5, 4, 3>>
  end
end
