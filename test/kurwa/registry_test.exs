defmodule Kurwa.RegistryTest do
  @moduledoc """
  The registry answers "which sets exist" without a scan, by storing set names
  as keys in a reserved set that gets a shard of its own.
  """

  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [unique_key: 1, eventually: 1]

  alias Kurwa.Key
  alias Kurwa.Namespace
  alias Kurwa.Registry
  alias Kurwa.Store

  defp fresh_set, do: "set#{System.unique_integer([:positive])}"

  describe "the reserved namespace" do
    test "cannot be reached by anything a caller can spell" do
      # The registry lives in "_sets", and a leading underscore is exactly what
      # valid_name?/1 refuses - so encode/2 can never collide with it.
      refute Key.valid_name?("_sets")
      assert_raise ArgumentError, fn -> Key.encode("_sets", "anything") end
    end

    test "registry keys are recognised as system keys, ordinary ones are not" do
      assert Key.system?(Key.registry_key("alpha"))
      refute Key.system?(Key.encode("alpha", "a-key"))
      refute Key.system?(Key.encode("a-key"))
    end

    test "a registry key round-trips to its set name" do
      assert Key.registry_name(Key.registry_key("black.list-1")) == {:ok, "black.list-1"}
      assert Key.registry_name(Key.encode("alpha", "k")) == :error
      assert Key.registry_name(Key.encode("k")) == :error
    end

    test "system keys land in their own shard, so listing does not fold everything" do
      assert Store.shard_for(Key.registry_key("alpha")) == Store.system_shard()
      refute Store.shard_for(Key.encode("alpha", "k")) == Store.system_shard()
    end
  end

  describe "registering" do
    test "the first add to a set records that the set exists" do
      set = fresh_set()
      refute set in Registry.local()

      :ok = Namespace.add(set, unique_key("k"))

      assert eventually(fn -> set in Registry.local() end)
    end

    test "adding to the default set registers nothing" do
      before = Registry.local()
      :ok = Kurwa.add(unique_key("plain"))

      Process.sleep(80)
      assert Registry.local() == before
    end

    test "registering is idempotent and does not repeat the write" do
      set = fresh_set()

      for i <- 1..20, do: :ok = Namespace.add(set, "k#{i}")
      assert eventually(fn -> set in Registry.local() end)

      # one record for the set, however many keys went into it
      names = Registry.local()
      assert Enum.count(names, &(&1 == set)) == 1
    end

    test "a failed write is not remembered as done" do
      set = fresh_set()
      was_w = Application.get_env(:kurwadb, :w)
      was_strict = Application.get_env(:kurwadb, :strict_quorum)

      # Three acks demanded on a one-node ring, strictly: the registry write
      # cannot succeed.
      Kurwa.Config.put(:w, 3)
      Kurwa.Config.put(:strict_quorum, true)

      try do
        Registry.register(set)

        # The write is dropped from the dedup cache, so the next add retries.
        # It is not asserted to be absent from the *store*: a quorum that fails
        # still leaves the record on whichever replicas did take it.
        assert eventually(fn -> not Registry.registered_here?(set) end)
      after
        Kurwa.Config.put(:w, was_w)
        Kurwa.Config.put(:strict_quorum, was_strict)
      end

      :ok = Namespace.add(set, "k2")
      assert eventually(fn -> Registry.registered_here?(set) end)
      assert set in Registry.local()
    end
  end

  describe "listing" do
    test "returns the sets and says who could not be asked" do
      set = fresh_set()
      :ok = Namespace.add(set, unique_key("k"))
      assert eventually(fn -> set in Registry.local() end)

      assert {:ok, %{sets: sets, unreachable: unreachable}} = Namespace.list()
      assert set in sets
      assert unreachable == %{}
      assert sets == Enum.sort(sets)
    end

    test "lists sets that ever held a key, which is not the same as sets that hold one" do
      set = fresh_set()
      key = unique_key("only")

      :ok = Namespace.add(set, key)
      assert eventually(fn -> set in Registry.local() end)

      :ok = Namespace.delete(set, key)

      # Dropping it when the set empties would mean counting the set's keys,
      # and counting is a scan. So it stays listed, on purpose.
      assert set in Registry.local()
      assert Namespace.member?(set, key) == {:ok, false}
    end
  end

  describe "forgetting" do
    test "removes the name and leaves the keys alone" do
      set = fresh_set()
      key = unique_key("kept")

      :ok = Namespace.add(set, key)
      assert eventually(fn -> set in Registry.local() end)

      assert Namespace.forget(set) == :ok
      refute set in Registry.local()
      assert Namespace.member?(set, key) == {:ok, true}
    end

    test "a later add registers the set again" do
      set = fresh_set()
      :ok = Namespace.add(set, "k1")
      assert eventually(fn -> set in Registry.local() end)

      :ok = Namespace.forget(set)
      refute set in Registry.local()

      :ok = Namespace.add(set, "k2")
      assert eventually(fn -> set in Registry.local() end)
    end
  end
end
