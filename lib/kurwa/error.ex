defmodule Kurwa.Error do
  @moduledoc "Raised by the bang-style functions in `Kurwa`."

  defexception [:reason]

  @impl true
  def message(%__MODULE__{reason: :ring_empty}) do
    "kurwadb: no nodes in the ring"
  end

  def message(%__MODULE__{reason: {:quorum_not_met, %{op: op, needed: needed, got: got} = d}}) do
    "kurwadb: #{op} quorum not met - needed #{needed} replicas, got #{got} " <>
      "(failed: #{inspect(Map.get(d, :failed))})"
  end

  def message(%__MODULE__{reason: reason}), do: "kurwadb: #{inspect(reason)}"
end
