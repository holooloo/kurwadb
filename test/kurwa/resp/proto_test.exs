defmodule Kurwa.Resp.ProtoTest do
  use ExUnit.Case, async: true

  alias Kurwa.Resp.Proto

  test "an array of bulk strings, and what is left after it" do
    assert {:ok, ["SET", "k", "v"], "rest"} =
             Proto.decode("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\nrest")
  end

  test "binary-safe bulk strings" do
    assert {:ok, ["a\r\nb"], ""} = Proto.decode("*1\r\n$4\r\na\r\nb\r\n")
  end

  test "partial input asks for more, at every cut" do
    full = "*2\r\n$9\r\nSISMEMBER\r\n$3\r\nabc\r\n"

    for cut <- 0..(byte_size(full) - 1) do
      assert Proto.decode(binary_part(full, 0, cut)) == :more, "cut at #{cut}"
    end
  end

  test "inline commands, as typed at a terminal" do
    assert {:ok, ["PING"], ""} = Proto.decode("PING\r\n")
    assert {:ok, ["EXISTS", "a", "b"], "x"} = Proto.decode("EXISTS a  b\nx")
  end

  test "malformed input is an error, not a crash" do
    assert {:error, _} = Proto.decode("*x\r\n")
    assert {:error, _} = Proto.decode("*1\r\n+nope\r\n")
  end

  test "replies, in RESP2 and RESP3" do
    enc = &IO.iodata_to_binary(Proto.encode(&1, &2))

    assert enc.({:simple, "OK"}, 2) == "+OK\r\n"
    assert enc.({:error, "ERR x"}, 2) == "-ERR x\r\n"
    assert enc.(3, 2) == ":3\r\n"
    assert enc.("ab", 2) == "$2\r\nab\r\n"
    assert enc.(nil, 2) == "$-1\r\n"
    assert enc.(nil, 3) == "_\r\n"
    assert enc.(true, 3) == "#t\r\n"
    assert enc.([1, "a"], 2) == "*2\r\n:1\r\n$1\r\na\r\n"
    assert enc.({:map, [{"k", 1}]}, 2) == "*2\r\n$1\r\nk\r\n:1\r\n"
    assert enc.({:map, [{"k", 1}]}, 3) == "%1\r\n$1\r\nk\r\n:1\r\n"
  end
end
