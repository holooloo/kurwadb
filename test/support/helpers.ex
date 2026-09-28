defmodule Kurwa.TestHelpers do
  @moduledoc "Shared bits for the test suite."

  alias Kurwa.Record

  @doc "A key no other test uses."
  def unique_key(prefix \\ "k") do
    "#{prefix}-#{System.unique_integer([:positive])}-#{:erlang.phash2(self())}"
  end

  @doc "A scratch directory, removed when the test ends."
  def tmp_dir(context_name) do
    dir =
      Path.join([
        "tmp",
        "test-scratch",
        "#{context_name}-#{System.unique_integer([:positive])}"
      ])

    File.rm_rf!(dir)
    File.mkdir_p!(dir)
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  @doc """
  Waits for `fun` to return a truthy value.

  For the things kurwadb does deliberately off the caller's path - registering a
  set name, draining a hint queue - where asserting immediately would be
  asserting a race.
  """
  def eventually(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    poll(fun, deadline)
  end

  defp poll(fun, deadline) do
    case fun.() do
      falsy when falsy in [false, nil] ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk_or_raise()
        else
          Process.sleep(25)
          poll(fun, deadline)
        end

      truthy ->
        truthy
    end
  end

  defp flunk_or_raise, do: raise("kurwadb test: condition never became true")

  @doc "A record, with everything but the interesting field defaulted."
  def record(key, opts \\ []) do
    Record.new(
      key,
      Keyword.get(opts, :lamport, 1),
      Keyword.get(opts, :node, :a@test),
      Keyword.get(opts, :alive?, true),
      Keyword.get(opts, :wall, 1_000)
    )
  end
end
