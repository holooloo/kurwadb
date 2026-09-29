defmodule Kurwa.Store.BloomTest do
  use ExUnit.Case, async: true

  alias Kurwa.Store.Bloom

  test "everything added is found again - false negatives are impossible" do
    keys = for i <- 1..5_000, do: "order:#{i}"
    filter = Bloom.new(length(keys))
    Enum.each(keys, &Bloom.add(filter, &1))

    binary = Bloom.to_binary(filter)

    assert Enum.all?(keys, &Bloom.member?(binary, filter.bits, filter.hashes, &1)),
           "a bloom filter may never say no to a key it holds"
  end

  test "false positives stay near the rate it was sized for" do
    keys = for i <- 1..10_000, do: "present:#{i}"
    filter = Bloom.new(length(keys), 0.01)
    Enum.each(keys, &Bloom.add(filter, &1))
    binary = Bloom.to_binary(filter)

    absent = for i <- 1..10_000, do: "absent:#{i}"
    hits = Enum.count(absent, &Bloom.member?(binary, filter.bits, filter.hashes, &1))

    assert hits / length(absent) < 0.03, "expected ~1% false positives, got #{hits} of 10000"
  end

  test "costs about a byte and a quarter per key, against 128 in ETS" do
    filter = Bloom.new(1_000_000, 0.01)
    assert %{per_key: per_key} = Bloom.size(filter, 1_000_000)

    assert per_key < 1.5
    assert per_key > 1.0
  end

  test "an empty filter says no to everything" do
    filter = Bloom.new(100)
    binary = Bloom.to_binary(filter)

    refute Enum.any?(1..500, &Bloom.member?(binary, filter.bits, filter.hashes, "k#{&1}"))
  end

  test "survives the round trip through bytes" do
    filter = Bloom.new(64)
    Bloom.add(filter, "kept")

    binary = Bloom.to_binary(filter)
    assert byte_size(binary) == div(filter.bits, 8)
    assert Bloom.member?(binary, filter.bits, filter.hashes, "kept")
  end

  test "handles binary keys that are not text" do
    filter = Bloom.new(10)
    key = <<0, 255, 10, 0>>
    Bloom.add(filter, key)

    assert Bloom.member?(Bloom.to_binary(filter), filter.bits, filter.hashes, key)
  end

  describe "the native implementation" do
    @describetag :native

    setup do
      unless Bloom.native?(), do: raise("built without cargo; these compare the two paths")
      :ok
    end

    test "hashes identically to the Elixir one" do
      keys = ["", "a", "order:1029", <<0, 255, 10>>, :crypto.strong_rand_bytes(100)]

      for key <- keys do
        assert Kurwa.Native.bloom_fnv1a(key) == Bloom.fnv1a(key),
               "the two implementations must agree bit for bit on #{inspect(key)}"
      end
    end

    test "agrees with the Elixir one on every answer" do
      # A disagreement here is not a performance problem, it is a filter that
      # says no to a key it holds.
      present = for i <- 1..3_000, do: "present:#{i}"
      filter = Bloom.new(length(present))
      Enum.each(present, &Bloom.add(filter, &1))
      binary = Bloom.to_binary(filter)

      probes = present ++ for(i <- 1..3_000, do: "absent:#{i}")

      disagreements =
        Enum.filter(probes, fn key ->
          Kurwa.Native.bloom_member(binary, filter.bits, filter.hashes, key) !=
            Bloom.member_elixir?(binary, filter.bits, filter.hashes, key)
        end)

      assert disagreements == []
    end

    test "still never says no to a key it holds" do
      keys = for i <- 1..5_000, do: <<i::32, "bulk">>
      filter = Bloom.new(length(keys))
      Enum.each(keys, &Bloom.add(filter, &1))
      binary = Bloom.to_binary(filter)

      assert Enum.all?(keys, &Bloom.member?(binary, filter.bits, filter.hashes, &1))
    end

    test "a malformed filter answers no rather than taking the VM down" do
      # A NIF has no supervisor above it: a panic kills the whole node, so the
      # bad cases have to return instead of raising.
      assert Kurwa.Native.bloom_member(<<>>, 64, 3, "k") == false
      assert Kurwa.Native.bloom_member(<<0, 0>>, 4096, 7, "k") == false
      assert Kurwa.Native.bloom_member(<<0::512>>, 0, 7, "k") == false
    end
  end
end
