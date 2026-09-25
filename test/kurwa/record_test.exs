defmodule Kurwa.RecordTest do
  use ExUnit.Case, async: true

  import Kurwa.TestHelpers, only: [record: 2]

  alias Kurwa.Record

  describe "merge/2" do
    test "the higher lamport wins" do
      old = record("k", lamport: 1)
      new = record("k", lamport: 2, alive?: false)

      assert Record.merge(old, new) == new
      assert Record.merge(new, old) == new
    end

    test "equal lamports are broken by node name, not by argument order" do
      a = record("k", lamport: 5, node: :a@test)
      b = record("k", lamport: 5, node: :b@test, alive?: false)

      assert Record.merge(a, b) == b
      assert Record.merge(b, a) == b
    end

    test "is idempotent" do
      rec = record("k", lamport: 3)
      assert Record.merge(rec, rec) == rec
    end

    test "treats a missing replica as losing" do
      rec = record("k", lamport: 1)

      assert Record.merge(nil, rec) == rec
      assert Record.merge(rec, nil) == rec
      assert Record.merge(nil, nil) == nil
    end

    test "converges regardless of the order replicas are merged in" do
      records = [
        record("k", lamport: 1, node: :a@test),
        record("k", lamport: 4, node: :b@test, alive?: false),
        record("k", lamport: 4, node: :c@test),
        record("k", lamport: 2, node: :d@test)
      ]

      expected = Record.merge_all(records)

      for permutation <- permutations(records) do
        assert Record.merge_all(permutation) == expected
      end
    end
  end

  describe "alive?/1" do
    test "a tombstone is not a member, a missing record is not a member" do
      refute Record.alive?(record("k", alive?: false))
      refute Record.alive?(nil)
      assert Record.alive?(record("k", alive?: true))
    end
  end

  describe "same_version?/2" do
    test "compares the version, not the payload" do
      a = record("k", lamport: 2, node: :a@test, wall: 1)
      b = record("k", lamport: 2, node: :a@test, wall: 999)
      c = record("k", lamport: 3, node: :a@test)

      assert Record.same_version?(a, b)
      refute Record.same_version?(a, c)
      refute Record.same_version?(a, nil)
      assert Record.same_version?(nil, nil)
    end
  end

  defp permutations([]), do: [[]]

  defp permutations(list) do
    for head <- list, tail <- permutations(list -- [head]), do: [head | tail]
  end
end
