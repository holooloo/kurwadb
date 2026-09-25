defmodule Kurwa.QuorumTest do
  use ExUnit.Case, async: true

  alias Kurwa.Quorum

  test "an empty node list is not a quorum failure, it is nothing to do" do
    assert Quorum.run([], fn _ -> {:ok, :never} end, 1, 100) == %{ok: [], failed: []}
  end

  test "collects every answer when they all arrive in time" do
    outcome = Quorum.run([:a, :b, :c], fn node -> {:ok, node} end, 3, 500)

    assert Enum.sort(outcome.ok) == [a: :a, b: :b, c: :c]
    assert outcome.failed == []
  end

  test "returns as soon as `need` replicas have answered" do
    fun = fn
      :fast -> {:ok, :fast}
      :slow -> Process.sleep(2_000) && {:ok, :slow}
    end

    {micros, outcome} = :timer.tc(fn -> Quorum.run([:fast, :slow], fun, 1, 1_500) end)

    assert outcome.ok == [fast: :fast]
    assert div(micros, 1000) < 500, "waited #{div(micros, 1000)}ms for a quorum of 1"
  end

  test "late replies from abandoned replicas do not reach the caller" do
    fun = fn
      :fast -> {:ok, :fast}
      :slow -> Process.sleep(100) && {:ok, :slow}
    end

    assert %{ok: [fast: :fast]} = Quorum.run([:fast, :slow], fun, 1, 1_000)

    Process.sleep(300)
    assert {:messages, []} = Process.info(self(), :messages)
  end

  test "separates failures from successes" do
    fun = fn
      :good -> {:ok, :yes}
      :bad -> {:error, :nope}
    end

    outcome = Quorum.run([:good, :bad], fun, 2, 500)

    assert outcome.ok == [good: :yes]
    assert outcome.failed == [bad: :nope]
  end

  test "a replica that raises counts as failed, not as a crash of the caller" do
    fun = fn
      :ok_node -> {:ok, :fine}
      :boom -> raise "replica exploded"
    end

    outcome = Quorum.run([:ok_node, :boom], fun, 2, 500)

    assert outcome.ok == [ok_node: :fine]
    assert [{:boom, {:error, %RuntimeError{}}}] = outcome.failed
  end

  test "a reply the contract does not allow is a failure" do
    outcome = Quorum.run([:weird], fn _ -> :whatever end, 1, 200)

    assert outcome.ok == []
    assert outcome.failed == [weird: {:unexpected_return, :whatever}]
  end

  test "reports the replicas that ran out of time" do
    fun = fn
      :fast -> {:ok, :fast}
      :slow -> Process.sleep(1_000) && {:ok, :slow}
    end

    outcome = Quorum.run([:fast, :slow], fun, 2, 150)

    assert outcome.ok == [fast: :fast]
    assert outcome.failed == [slow: :timeout]
  end

  test "a quorum that cannot be reached still returns what it got" do
    outcome = Quorum.run([:a, :b], fn node -> {:ok, node} end, 5, 200)

    assert length(outcome.ok) == 2
    assert outcome.failed == []
  end
end
