defmodule Kurwa.NamespaceTest do
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [unique_key: 1]

  alias Kurwa.Namespace

  test "a named set behaves like the plain API" do
    key = unique_key("ns")

    assert Namespace.member?("alpha", key) == {:ok, false}
    assert Namespace.add("alpha", key) == :ok
    assert Namespace.member?("alpha", key) == {:ok, true}
    assert Namespace.delete("alpha", key) == :ok
    assert Namespace.member?("alpha", key) == {:ok, false}
  end

  test "the same key in two sets is two independent facts" do
    key = unique_key("shared")

    :ok = Namespace.add("alpha", key)

    assert Namespace.member?("alpha", key) == {:ok, true}
    assert Namespace.member?("beta", key) == {:ok, false}
    assert Kurwa.fetch(key) == {:ok, false}
  end

  test "the default set is not reachable through a name" do
    key = unique_key("default-vs-named")

    :ok = Kurwa.add(key)

    assert Kurwa.fetch(key) == {:ok, true}
    assert Namespace.member?("alpha", key) == {:ok, false}
  end

  describe "member_any?/3 (union)" do
    test "true when any set has the key" do
      key = unique_key("union")
      :ok = Namespace.add("greylist", key)

      assert Namespace.member_any?(["blacklist", "greylist"], key) == {:ok, true}
      assert Namespace.member_any?(["greylist"], key) == {:ok, true}
    end

    test "false only when no set has it" do
      key = unique_key("union-empty")

      assert Namespace.member_any?(["blacklist", "greylist"], key) == {:ok, false}
    end

    test "a union of no sets is empty, so nothing is a member" do
      assert Namespace.member_any?([], unique_key("void")) == {:ok, false}
    end

    test "surfaces an error when no set could answer" do
      key = unique_key("union-broken")

      assert {:error, {:incomplete_union, sets}} =
               with_strict_quorum(fn -> Namespace.member_any?(["a", "b"], key, r: 3) end)

      assert map_size(sets) == 2
    end
  end

  describe "member_all?/3 (intersection)" do
    test "true only when every set has the key" do
      key = unique_key("inter")

      :ok = Namespace.add("alpha", key)
      assert Namespace.member_all?(["alpha", "beta"], key) == {:ok, false}

      :ok = Namespace.add("beta", key)
      assert Namespace.member_all?(["alpha", "beta"], key) == {:ok, true}
    end

    test "an intersection of no sets holds everything" do
      assert Namespace.member_all?([], unique_key("void")) == {:ok, true}
    end

    test "a definite false wins over an unreachable set" do
      key = unique_key("inter-broken")

      assert {:error, {:incomplete_union, _}} =
               with_strict_quorum(fn -> Namespace.member_all?(["a", "b"], key, r: 3) end)
    end
  end

  test "rejects names it cannot represent" do
    refute Namespace.valid_name?("has space")
    assert Namespace.valid_name?("ok-name")
  end

  defp with_strict_quorum(fun) do
    original = Application.get_env(:kurwadb, :strict_quorum)
    Application.put_env(:kurwadb, :strict_quorum, true)

    try do
      fun.()
    after
      Application.put_env(:kurwadb, :strict_quorum, original)
    end
  end
end
