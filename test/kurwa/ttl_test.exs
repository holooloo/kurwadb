defmodule Kurwa.TtlTest do
  @moduledoc """
  Keys that expire on their own.

  The jobs a key-only store is actually used for - deduplication, idempotency
  keys, rate-limit buckets - all want the key gone after a while, so these
  assert the behaviour end to end rather than just the record predicate.
  """

  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [unique_key: 1]

  alias Kurwa.Extractor
  alias Kurwa.Key
  alias Kurwa.Record
  alias Kurwa.Store

  describe "the record itself" do
    test "a key with no expiry is a member without ever reading the clock" do
      record = Record.new("k", 1, node(), true)

      assert Record.expires_at(record) == :never
      assert Record.member?(record)
      assert Record.ttl(record) == :never
      refute Record.expired?(record, System.system_time(:millisecond) + 1_000_000)
    end

    test "an expired record is no longer a member, but is still not a tombstone" do
      past = System.system_time(:millisecond) - 1
      record = Record.new("k", 1, node(), true, nil, past)

      refute Record.member?(record)
      assert Record.alive?(record), "expiry is not a delete: the tombstone flag stays set"
      assert Record.ttl(record) == 0
    end

    test "expiry does not touch the merge order" do
      # The record with the higher stamp wins even when it is the one that expires.
      plain = Record.new("k", 1, node(), true)
      expiring = Record.new("k", 2, node(), true, nil, System.system_time(:millisecond) + 60_000)

      assert Record.merge(plain, expiring) == expiring
      assert Record.merge(expiring, plain) == expiring
    end

    test "every replica reaches the same verdict without talking to anyone" do
      # Expiry is a pure function of the record, which is why an expired key
      # needs no tombstone to stay gone.
      record = Record.new("k", 1, :a@host, true, nil, 1_000)

      assert Record.expired?(record, 1_001)
      refute Record.expired?(record, 999)
    end
  end

  describe "through the public API" do
    test "a key with a ttl is a member until it is not" do
      key = unique_key("ttl")

      assert Kurwa.add(key, ttl: 120) == :ok
      assert Kurwa.member?(key)

      Process.sleep(200)
      refute Kurwa.member?(key)
    end

    test "a key without a ttl stays" do
      key = unique_key("forever")

      :ok = Kurwa.add(key)
      Process.sleep(120)

      assert Kurwa.member?(key)
      assert Kurwa.ttl(key) == {:ok, :never}
    end

    test "ttl/1 reports what is left, and nil once the key is gone" do
      key = unique_key("ttl-report")

      :ok = Kurwa.add(key, ttl: 10_000)
      assert {:ok, left} = Kurwa.ttl(key)
      assert left > 8_000 and left <= 10_000

      assert Kurwa.ttl(unique_key("absent")) == {:ok, nil}
    end

    test "adding again replaces the deadline instead of extending it" do
      key = unique_key("replace")

      :ok = Kurwa.add(key, ttl: 60_000)
      :ok = Kurwa.add(key, ttl: 500)

      assert {:ok, left} = Kurwa.ttl(key)
      assert left <= 500
    end

    test "a plain add clears an expiry that was there" do
      key = unique_key("clear")

      :ok = Kurwa.add(key, ttl: 200)
      :ok = Kurwa.add(key)

      assert Kurwa.ttl(key) == {:ok, :never}
      Process.sleep(250)
      assert Kurwa.member?(key)
    end

    test "an explicit delete still beats a live ttl" do
      key = unique_key("delete-wins")

      :ok = Kurwa.add(key, ttl: 60_000)
      :ok = Kurwa.delete(key)

      refute Kurwa.member?(key)
    end

    test "named sets carry their own expiry" do
      key = unique_key("ns-ttl")

      :ok = Kurwa.Namespace.add("alpha", key, ttl: 120)
      :ok = Kurwa.Namespace.add("beta", key)

      Process.sleep(200)

      assert Kurwa.Namespace.member?("alpha", key) == {:ok, false}
      assert Kurwa.Namespace.member?("beta", key) == {:ok, true}
    end

    test "a ttl that is not a positive number is a programmer error" do
      assert_raise ArgumentError, fn -> Kurwa.add(unique_key("bad"), ttl: 0) end
      assert_raise ArgumentError, fn -> Kurwa.add(unique_key("bad"), ttl: -5) end
    end
  end

  describe "sweeping" do
    test "an expired key is only dropped after the grace period, then uncounted" do
      key = unique_key("sweep")
      storage_key = Key.encode(key)

      :ok = Kurwa.add(key, ttl: 50)
      Process.sleep(80)

      # Gone as an answer, still on disk: a replica that was away must not be
      # able to resurrect it by pushing back an older record.
      refute Kurwa.member?(key)
      assert {:ok, record} = Store.get(storage_key)
      assert Record.alive?(record)

      # The sweep cutoff is now minus the tombstone TTL, so nothing goes yet.
      assert Store.gc() == 0
      assert {:ok, _} = Store.get(storage_key)

      original = Application.get_env(:kurwadb, :tombstone_ttl)
      Kurwa.Config.put(:tombstone_ttl, 0)

      try do
        assert Store.gc() >= 1
        assert Store.get(storage_key) == {:ok, nil}
      after
        Kurwa.Config.put(:tombstone_ttl, original)
      end
    end
  end

  describe "with the extractor cache on" do
    setup do
      original = Application.get_all_env(:kurwadb)
      Kurwa.Config.put(:cache, true)
      Kurwa.Config.put(:cache_broadcast, false)
      Kurwa.Config.put(:cache_ttl, 60_000)
      Extractor.flush()

      on_exit(fn ->
        for k <- [:cache, :cache_broadcast, :cache_ttl] do
          Kurwa.Config.put(k, Keyword.get(original, k))
        end

        Extractor.flush()
      end)

      :ok
    end

    test "a cached yes cannot outlive the key it is about" do
      key = unique_key("cache-ttl")

      # cache_ttl is a minute, the key lives 150ms: the entry must be capped to
      # the key, not to the cache setting.
      :ok = Kurwa.add(key, ttl: 150)
      assert Kurwa.member?(key)

      Process.sleep(250)
      refute Kurwa.member?(key)
    end

    test "the cap also applies to an answer learned by reading" do
      key = unique_key("cache-read-ttl")

      :ok = Kurwa.add(key, ttl: 150)
      Extractor.flush()

      # populate the cache from a quorum read rather than from the write
      assert Kurwa.member?(key)

      Process.sleep(250)
      refute Kurwa.member?(key)
    end
  end
end
