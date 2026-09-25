defmodule Kurwa.ExtractorTest do
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [unique_key: 1]

  alias Kurwa.Coordinator
  alias Kurwa.Extractor
  alias Kurwa.Key

  setup do
    original = Application.get_all_env(:kurwadb)
    Extractor.flush()

    on_exit(fn ->
      for setting <- [:cache, :cache_ttl, :cache_negative_ttl, :cache_broadcast] do
        Application.put_env(:kurwadb, setting, Keyword.get(original, setting))
      end

      Extractor.flush()
    end)

    :ok
  end

  describe "with the cache off (the default)" do
    test "reads and writes behave exactly as the uncached API" do
      key = unique_key("passthrough")

      assert Extractor.member?(key) == {:ok, false}
      assert Extractor.add(key) == :ok
      assert Extractor.member?(key) == {:ok, true}
      assert Extractor.delete(key) == :ok
      assert Extractor.member?(key) == {:ok, false}
    end

    test "nothing is cached, so nothing can go stale" do
      key = unique_key("nocache")
      before = Extractor.stats()

      :ok = Extractor.add(key)
      {:ok, true} = Extractor.member?(key)
      {:ok, true} = Extractor.member?(key)

      after_reads = Extractor.stats()

      assert after_reads.hits == before.hits
      assert after_reads.misses == before.misses
      refute after_reads.enabled
    end
  end

  describe "with the cache on" do
    setup do
      Application.put_env(:kurwadb, :cache, true)
      Application.put_env(:kurwadb, :cache_broadcast, false)
      :ok
    end

    test "the first read misses and the second hits" do
      key = unique_key("cached")
      before = Extractor.stats()

      assert Extractor.member?(key) == {:ok, false}
      assert Extractor.stats().misses == before.misses + 1

      assert Extractor.member?(key) == {:ok, false}
      assert Extractor.stats().hits == before.hits + 1
      assert Extractor.stats().misses == before.misses + 1
    end

    test "a write is written through, so the next read never leaves the node" do
      key = unique_key("write-through")

      :ok = Extractor.add(key)
      before = Extractor.stats()

      assert Extractor.member?(key) == {:ok, true}
      assert Extractor.stats().hits == before.hits + 1
      assert Extractor.stats().misses == before.misses
    end

    test "a delete is written through as a cached false" do
      key = unique_key("delete-through")

      :ok = Extractor.add(key)
      :ok = Extractor.delete(key)
      before = Extractor.stats()

      assert Extractor.member?(key) == {:ok, false}
      assert Extractor.stats().hits == before.hits + 1
    end

    test "entries expire, and positive and negative answers expire separately" do
      present = unique_key("ttl-present")
      absent = unique_key("ttl-absent")

      Application.put_env(:kurwadb, :cache_ttl, 10_000)
      Application.put_env(:kurwadb, :cache_negative_ttl, 1)

      :ok = Extractor.add(present)
      {:ok, false} = Extractor.member?(absent)

      Process.sleep(30)
      before = Extractor.stats()

      # the negative answer has expired
      assert Extractor.member?(absent) == {:ok, false}
      assert Extractor.stats().misses == before.misses + 1

      # the positive one has not
      assert Extractor.member?(present) == {:ok, true}
      assert Extractor.stats().hits == before.hits + 1
    end

    test "a write that bypassed this node stays stale until its TTL - by design" do
      key = unique_key("stale")

      :ok = Extractor.add(key)
      assert Extractor.member?(key) == {:ok, true}

      # delete underneath the cache, the way another node's coordinator would
      assert Coordinator.delete(Key.encode(key)) == :ok

      assert Extractor.member?(key) == {:ok, true}
      assert Coordinator.member?(Key.encode(key)) == {:ok, false}

      # ...until someone invalidates it, which is what the write broadcast does
      :ok = Extractor.invalidate(key)
      assert Extractor.member?(key) == {:ok, false}
    end

    test "an unreachable quorum is not cached" do
      key = unique_key("error")
      before = Extractor.stats()

      Application.put_env(:kurwadb, :strict_quorum, true)

      try do
        assert {:error, {:quorum_not_met, _}} = Extractor.member?(key, r: 3)
      after
        Application.put_env(:kurwadb, :strict_quorum, false)
      end

      # the next reader tries the cluster again rather than reusing the failure
      assert Extractor.member?(key) == {:ok, false}
      assert Extractor.stats().misses == before.misses + 2
    end

    test "named sets get their own cache entries" do
      key = unique_key("set-cache")

      :ok = Extractor.add(key, set: "alpha")

      assert Extractor.member?(key, set: "alpha") == {:ok, true}
      assert Extractor.member?(key, set: "beta") == {:ok, false}
      assert Extractor.member?(key) == {:ok, false}
    end

    test "flush drops everything without touching the data" do
      key = unique_key("flush")
      :ok = Extractor.add(key)

      Extractor.flush()
      before = Extractor.stats()

      assert Extractor.member?(key) == {:ok, true}
      assert Extractor.stats().misses == before.misses + 1
    end

    test "concurrent readers of a cold key make one trip to the cluster" do
      key = unique_key("herd")
      :ok = Coordinator.add(Key.encode(key))
      Extractor.flush()

      before = Extractor.stats()

      results =
        1..20
        |> Enum.map(fn _ -> Task.async(fn -> Extractor.member?(key) end) end)
        |> Task.await_many(5_000)

      assert Enum.all?(results, &(&1 == {:ok, true}))

      after_reads = Extractor.stats()
      trips = after_reads.misses - before.misses

      assert trips >= 1

      assert trips + (after_reads.coalesced - before.coalesced) + (after_reads.hits - before.hits) ==
               20
    end
  end

  test "the public API goes through the extractor, so Kurwa.member? is cached too" do
    Application.put_env(:kurwadb, :cache, true)
    Application.put_env(:kurwadb, :cache_broadcast, false)

    key = unique_key("public")
    :ok = Kurwa.add(key)
    before = Extractor.stats()

    assert Kurwa.member?(key)
    assert Extractor.stats().hits == before.hits + 1
  end
end
