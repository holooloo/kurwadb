defmodule Kurwa.Coordinator do
  @moduledoc """
  Turns one client request into N replica requests and a quorum decision.

  Any node can coordinate any key - there is no leader. A write gets one stamp
  from the local Lamport clock and that same record is sent to every replica, so
  all replicas converge on byte-identical data instead of each inventing its own
  version.

  Reads merge whatever answered and repair the replicas that were behind, which
  is how a replica that missed a write catches up. Note what that implies: a key
  that is never read is never repaired. That is the gap hinted handoff fills, and
  it is not implemented yet (see README).
  """

  alias Kurwa.Cluster
  alias Kurwa.Config
  alias Kurwa.Clock
  alias Kurwa.Quorum
  alias Kurwa.Record
  alias Kurwa.Ring
  alias Kurwa.Store

  @type error ::
          {:error, :ring_empty}
          | {:error, {:quorum_not_met, map()}}

  @doc "Adds `key` to the set."
  @spec add(Record.key(), keyword()) :: :ok | error()
  def add(key, opts \\ []) when is_binary(key), do: write(key, true, opts)

  @doc "Removes `key` from the set (writes a tombstone)."
  @spec delete(Record.key(), keyword()) :: :ok | error()
  def delete(key, opts \\ []) when is_binary(key), do: write(key, false, opts)

  @doc "Is `key` in the set?"
  @spec member?(Record.key(), keyword()) :: {:ok, boolean()} | error()
  def member?(key, opts \\ []) when is_binary(key) do
    with {:ok, prefs} <- preflist(key, opts) do
      timeout = Keyword.get(opts, :timeout, Config.request_timeout())
      r = quorum_size(Keyword.get(opts, :r, Config.r()), prefs)

      outcome = Quorum.run(prefs, &replica_get(&1, key, timeout), r, timeout)

      if length(outcome.ok) >= r do
        winner = outcome.ok |> Enum.map(fn {_node, record} -> record end) |> Record.merge_all()
        repair(prefs, outcome.ok, winner)
        {:ok, Record.alive?(winner)}
      else
        {:error, {:quorum_not_met, details(:read, r, prefs, outcome)}}
      end
    end
  end

  @doc """
  Approximate number of live keys in the cluster.

  Each node reports what it holds locally; since every key sits on `replicas`
  nodes, the cluster total is the sum divided by that. It is approximate on
  purpose - an exact distributed count would need to freeze the cluster.
  """
  @spec count(keyword()) :: {:ok, map()} | {:error, term()}
  def count(opts \\ []) do
    timeout = Keyword.get(opts, :timeout, Config.request_timeout())
    nodes = Cluster.members()

    case nodes do
      [] ->
        {:error, :ring_empty}

      nodes ->
        outcome = Quorum.run(nodes, &replica_count(&1, timeout), length(nodes), timeout)
        per_node = Map.new(outcome.ok)
        sum = per_node |> Map.values() |> Enum.sum()
        replicas = min(Config.n(), length(nodes))

        {:ok,
         %{
           approximate: div(sum, replicas),
           replicas: replicas,
           per_node: per_node,
           unreachable: Map.new(outcome.failed)
         }}
    end
  end

  defp write(key, alive?, opts) do
    with {:ok, prefs} <- preflist(key, opts) do
      timeout = Keyword.get(opts, :timeout, Config.request_timeout())
      w = quorum_size(Keyword.get(opts, :w, Config.w()), prefs)
      record = Record.new(key, Clock.tick(), node(), alive?)

      outcome = Quorum.run(prefs, &replica_put(&1, record, timeout), w, timeout)

      if length(outcome.ok) >= w do
        :ok
      else
        {:error, {:quorum_not_met, details(:write, w, prefs, outcome)}}
      end
    end
  end

  defp preflist(key, opts) do
    n = Keyword.get(opts, :n, Config.n())

    case Ring.preflist(Cluster.ring(), key, n) do
      [] -> {:error, :ring_empty}
      prefs -> {:ok, prefs}
    end
  end

  # How many acks we insist on. The preference list can be shorter than `n` when
  # the cluster is smaller or nodes are down; `strict_quorum: true` keeps the
  # configured number and fails such requests, the default caps it at the
  # replicas that actually exist and keeps serving.
  defp quorum_size(configured, prefs) do
    if Config.get(:strict_quorum) do
      configured
    else
      max(min(configured, length(prefs)), 1)
    end
  end

  defp replica_put(node, record, timeout) do
    if node == node() do
      case Store.put(record) do
        {:ok, winner} -> {:ok, winner}
        {:stale, winner} -> {:ok, winner}
        {:error, reason} -> {:error, reason}
      end
    else
      :erpc.call(node, Kurwa.Replica, :put, [record], timeout)
    end
  end

  defp replica_get(node, key, timeout) do
    if node == node() do
      Store.get(key)
    else
      :erpc.call(node, Kurwa.Replica, :get, [key], timeout)
    end
  end

  defp replica_count(node, timeout) do
    if node == node() do
      {:ok, Store.count()}
    else
      :erpc.call(node, Kurwa.Replica, :local_count, [], timeout)
    end
  end

  # Read repair: push the winner to every replica that answered with something
  # older. Fire and forget - the read has already been answered, and a lost
  # repair just means the next read repairs it instead.
  defp repair(_prefs, _answers, nil), do: :ok

  defp repair(_prefs, answers, winner) do
    for {node, record} <- answers, not Record.same_version?(record, winner) do
      :erpc.cast(node, Kurwa.Replica, :put, [winner])
    end

    :ok
  end

  defp details(op, needed, prefs, outcome) do
    %{
      op: op,
      needed: needed,
      got: length(outcome.ok),
      replicas: prefs,
      failed: Map.new(outcome.failed)
    }
  end
end
