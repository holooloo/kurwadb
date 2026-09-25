defmodule Kurwa.KeyTest do
  use ExUnit.Case, async: true

  alias Kurwa.Key

  test "the default set is a zero-length prefix" do
    assert Key.encode("user:1") == <<0, "user:1">>
    assert Key.encode(nil, "user:1") == Key.encode("user:1")
    assert Key.decode(Key.encode("user:1")) == {nil, "user:1"}
  end

  test "a named set carries its name in the prefix" do
    encoded = Key.encode("blacklist", "user:1")

    assert encoded == <<9, "blacklist", "user:1">>
    assert Key.decode(encoded) == {"blacklist", "user:1"}
  end

  test "round-trips arbitrary binary keys" do
    for key <- ["", "plain", <<0, 1, 255>>, :crypto.strong_rand_bytes(64)] do
      assert Key.decode(Key.encode(key)) == {nil, key}
      assert Key.decode(Key.encode("set", key)) == {"set", key}
    end
  end

  test "a length prefix cannot be forged by a key that looks like a separator" do
    # The reason the prefix is a length and not a "\\0" separator: this key in the
    # default set must not be the same storage key as user:1 in the blacklist set.
    tricky = Key.encode(<<9, "blacklist", "user:1">>)
    real = Key.encode("blacklist", "user:1")

    refute tricky == real
    assert Key.decode(tricky) == {nil, <<9, "blacklist", "user:1">>}
    assert Key.decode(real) == {"blacklist", "user:1"}
  end

  test "keys in different sets are different storage keys" do
    assert Key.encode("a", "k") != Key.encode("b", "k")
    assert Key.encode("a", "k") != Key.encode("k")
  end

  describe "valid_name?/1" do
    test "accepts what survives a URL segment and a directory name" do
      assert Key.valid_name?("blacklist")
      assert Key.valid_name?("tenant.42_v2-b")
      assert Key.valid_name?("A1")
      assert Key.valid_name?(String.duplicate("a", 255))
    end

    test "rejects the rest" do
      refute Key.valid_name?("")
      refute Key.valid_name?("_leading")
      refute Key.valid_name?("has space")
      refute Key.valid_name?("has/slash")
      refute Key.valid_name?("ключи")
      refute Key.valid_name?(String.duplicate("a", 256))
      refute Key.valid_name?(:atom)
      refute Key.valid_name?(nil)
    end
  end

  test "encoding an invalid name is a programmer error, not a silent fallback" do
    assert_raise ArgumentError, fn -> Key.encode("has space", "k") end
  end
end
