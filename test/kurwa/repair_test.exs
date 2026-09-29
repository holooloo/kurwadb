defmodule Kurwa.RepairTest do
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [unique_key: 1]

  alias Kurwa.Key
  alias Kurwa.Record
  alias Kurwa.Repair

  describe "digest/1" do
    test "is stable for an unchanged store" do
      assert Repair.digest(node()) == Repair.digest(node())
    end

    test "changes when a record changes" do
      key = unique_key("digest")

      before = Repair.digest(node())
      :ok = Kurwa.add(key)
      after_add = Repair.digest(node())

      assert after_add.keys == before.keys + 1
      assert after_add.buckets != before.buckets
    end

    test "changes when a key is deleted, because a tombstone is state too" do
      key = unique_key("digest-delete")
      :ok = Kurwa.add(key)

      before = Repair.digest(node())
      :ok = Kurwa.delete(key)

      assert Repair.digest(node()).buckets != before.buckets
    end

    test "counts only the keys the two nodes share" do
      :ok = Kurwa.add(unique_key("shared"))

      # A node that is not in the ring shares nothing with us, so there is
      # nothing to compare and the vector is empty rather than "all different".
      stranger = Repair.digest(:nowhere@nohost)

      assert stranger.keys == 0
      assert stranger.buckets |> Tuple.to_list() |> Enum.all?(&(&1 == 0))
    end

    test "has one slot per configured bucket" do
      assert tuple_size(Repair.digest(node()).buckets) == Kurwa.Config.get(:repair_buckets)
    end
  end

  describe "bucket/2" do
    test "returns the records that hash into it, and nothing else" do
      key = unique_key("bucket")
      storage_key = Key.encode(key)
      :ok = Kurwa.add(key)

      buckets = Kurwa.Config.get(:repair_buckets)
      index = :erlang.phash2(storage_key, buckets) + 1

      records = Repair.bucket(node(), index)

      assert Enum.any?(records, &(Record.key(&1) == storage_key))
      assert Enum.all?(records, &(:erlang.phash2(Record.key(&1), buckets) + 1 == index))
    end

    test "is empty for a node that shares nothing with us" do
      :ok = Kurwa.add(unique_key("nothing-shared"))
      assert Repair.bucket(:nowhere@nohost, 1) == []
    end
  end

  describe "a round against ourselves" do
    test "finds nothing to do, since a store never diverges from itself" do
      :ok = Kurwa.add(unique_key("self"))

      assert {:ok, result} = Repair.run(node())
      assert result.diverged == 0
      assert result.repaired == 0
      assert result.keys > 0
    end
  end

  test "an unreachable peer is an error, not a crash" do
    assert {:error, _reason} = Repair.run(:nowhere@nohost)
    assert Process.alive?(Process.whereis(Kurwa.Repair))
  end
end
