defmodule Kurwa.Clock do
  @moduledoc """
  Node-local Lamport clock, kept in an `:atomics` cell so every request can bump
  it without going through a process.

  Ordering of writes never depends on wall-clock time, so clock skew between
  nodes cannot make a write disappear. It only decides *which* concurrent write
  wins, and that is settled by `{lamport, node}`.
  """

  @pt_key {__MODULE__, :atomics}

  @doc "Installs the counter. Called once from `Kurwa.Application`."
  def init do
    unless :persistent_term.get(@pt_key, nil) do
      :persistent_term.put(@pt_key, :atomics.new(1, signed: false))
    end

    :ok
  end

  @doc "Next local stamp, strictly greater than anything ticked or observed so far."
  @spec tick() :: pos_integer()
  def tick, do: :atomics.add_get(ref(), 1, 1)

  @doc "Current value without advancing it."
  @spec peek() :: non_neg_integer()
  def peek, do: :atomics.get(ref(), 1)

  @doc """
  Raises the clock to at least `seen`.

  Called for every record we accept from a peer and for every record replayed
  from the WAL, so a restarted node never reissues stamps it already used.
  """
  @spec observe(non_neg_integer()) :: :ok
  def observe(seen) when is_integer(seen) and seen >= 0, do: observe(ref(), seen)

  defp observe(ref, seen) do
    current = :atomics.get(ref, 1)

    if seen <= current do
      :ok
    else
      case :atomics.compare_exchange(ref, 1, current, seen) do
        :ok -> :ok
        _raced -> observe(ref, seen)
      end
    end
  end

  defp ref do
    case :persistent_term.get(@pt_key, nil) do
      nil ->
        init()
        :persistent_term.get(@pt_key)

      ref ->
        ref
    end
  end
end
