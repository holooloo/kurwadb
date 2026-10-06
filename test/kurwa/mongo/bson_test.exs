defmodule Kurwa.Mongo.BsonTest do
  use ExUnit.Case, async: true

  alias Kurwa.Mongo.Bson

  # The two examples from bsonspec.org, byte for byte.
  test "the spec's {hello: world}" do
    bytes = <<0x16, 0, 0, 0, 0x02, "hello", 0, 0x06, 0, 0, 0, "world", 0, 0>>
    assert Bson.encode({:doc, [{"hello", "world"}]}) == bytes
    assert Bson.decode!(bytes) == {:doc, [{"hello", "world"}]}
  end

  test "the spec's {BSON: [awesome, 5.05, 1986]}" do
    bytes =
      <<0x31, 0, 0, 0, 0x04, "BSON", 0, 0x26, 0, 0, 0, 0x02, "0", 0, 0x08, 0, 0, 0, "awesome", 0,
        0x01, "1", 0, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x14, 0x40, 0x10, "2", 0, 0xC2, 0x07, 0,
        0, 0, 0>>

    assert Bson.encode({:doc, [{"BSON", ["awesome", 5.05, 1986]}]}) == bytes
    assert Bson.decode!(bytes) == {:doc, [{"BSON", ["awesome", 5.05, 1986]}]}
  end

  test "every type round-trips, in order" do
    doc =
      {:doc,
       [
         {"find", "seen"},
         {"n", {:int64, 9_000_000_000}},
         {"small", 7},
         {"oid", {:oid, :binary.copy(<<0xAB>>, 12)}},
         {"bin", {:binary, 0, "raw"}},
         {"at", {:datetime, 1_700_000_000_000}},
         {"ts", {:timestamp, 1, 2}},
         {"null", nil},
         {"yes", true},
         {"nested", {:doc, [{"$in", ["a", {:doc, [{"x", 1}]}]}]}}
       ]}

    assert Bson.decode!(Bson.encode(doc)) == doc
  end

  test "a truncated document is an error, not a crash" do
    full = Bson.encode({:doc, [{"hello", "world"}]})
    assert Bson.decode(binary_part(full, 0, byte_size(full) - 3)) == :error
  end
end
