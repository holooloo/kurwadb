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
