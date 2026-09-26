defmodule Kurwa.Extractor.FlightTest do
  use ExUnit.Case, async: false

  alias Kurwa.Extractor.Flight

  test "concurrent askers for the same key share one resolution" do
    key = "flight-#{System.unique_integer([:positive])}"
    runs = :counters.new(1, [:atomics])
    me = self()

    tasks =
      for _ <- 1..20 do
        Task.async(fn ->
          Flight.fetch(key, fn ->
            :counters.add(runs, 1, 1)
            send(me, :resolving)
            Process.sleep(80)
            :the_answer
          end)
        end)
      end

    results = Task.await_many(tasks, 5_000)

    assert :counters.get(runs, 1) == 1
    assert Enum.count(results, &match?({:lead, :the_answer}, &1)) == 1
    assert Enum.count(results, &match?({:joined, :the_answer}, &1)) == 19
  end

  test "different keys do not wait for each other" do
    a = "flight-a-#{System.unique_integer([:positive])}"
    b = "flight-b-#{System.unique_integer([:positive])}"

    task = Task.async(fn -> Flight.fetch(a, fn -> Process.sleep(200) && :slow end) end)

    {micros, {:lead, :fast}} = :timer.tc(fn -> Flight.fetch(b, fn -> :fast end) end)

    assert div(micros, 1000) < 100
    assert {:lead, :slow} = Task.await(task, 5_000)
  end

  test "a key is free again once its flight settles" do
    key = "flight-#{System.unique_integer([:positive])}"

    assert {:lead, 1} = Flight.fetch(key, fn -> 1 end)
    assert {:lead, 2} = Flight.fetch(key, fn -> 2 end)
    assert Flight.in_flight() == 0
  end

  test "a leader that dies hands its waiters an error instead of parking them" do
    key = "flight-doomed-#{System.unique_integer([:positive])}"
    me = self()

    leader =
      spawn(fn ->
        Flight.fetch(key, fn ->
          send(me, :leading)
          Process.sleep(:infinity)
        end)
      end)

    assert_receive :leading, 1_000

    waiter = Task.async(fn -> Flight.fetch(key, fn -> :should_not_run end) end)
    Process.sleep(50)
    Process.exit(leader, :kill)

    assert {:joined, {:error, {:leader_down, :killed}}} = Task.await(waiter, 5_000)
  end

  test "a raising leader settles the flight before the crash propagates" do
    key = "flight-raise-#{System.unique_integer([:positive])}"

    assert catch_error(Flight.fetch(key, fn -> raise "nope" end))
    assert Flight.in_flight() == 0
    assert {:lead, :fine} = Flight.fetch(key, fn -> :fine end)
  end
end
