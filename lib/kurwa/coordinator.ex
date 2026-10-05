defmodule Kurwa.Coordinator do
  @moduledoc """
  Turns one client request into N replica requests and a quorum decision.

  Any node can coordinate any key - there is no leader. A write gets one stamp
  from the local Lamport clock and that same record is sent to every replica, so
  all replicas converge on byte-identical data instead of each inventing its own
  version.

  One thing worth being explicit about: `{:error, {:quorum_not_met, _}}` means
  the write was not acknowledged by enough replicas, **not** that nothing was
  written. The replicas that did take it keep it. A client that retries will
  converge on whichever write has the higher stamp, which is what last-writer-
  wins is for; a client that treats the error as "nothing happened" is wrong.

  Reads merge whatever answered and repair the replicas that were behind, which
  heals a replica that answered with something stale. Read repair alone is not
  enough, though: it only reaches replicas that answered inside the quorum
  window, and a replica that was down was never asked at all. So a write that
  could not reach one of its replicas also leaves a hint (`Kurwa.Handoff`), and
  that hint is replayed when the replica comes back.
  """

  alias Kurwa.Cluster
  alias Kurwa.Clock
  alias Kurwa.Config
  alias Kurwa.Handoff
  alias Kurwa.Placement
  alias Kurwa.Quorum
  alias Kurwa.Record
  alias Kurwa.Store

  @type error ::
          {:error, :ring_empty}
          | {:error, :no_replicas_reachable}
          | {:error, {:quorum_not_met, map()}}

  @doc """
  Adds `key` to the set.

  `ttl: milliseconds` makes the key expire on its own. Expiry is wall-clock, so
  unlike everything else here it is exposed to clock skew between nodes.
  """
  @spec add(Record.key(), keyword()) :: :ok | error()
  def add(key, opts \\ []) when is_binary(key), do: write(key, true, opts)

  @doc "Removes `key` from the set (writes a tombstone)."
  @spec delete(Record.key(), keyword()) :: :ok | error()
  def delete(key, opts \\ []) when is_binary(key), do: write(key, false, opts)

  @doc "Is `key` in the set?"
  @spec member?(Record.key(), keyword()) :: {:ok, boolean()} | error()
  def member?(key, opts \\ []) do
    with {:ok, record} <- lookup(key, opts), do: {:ok, Record.member?(record)}
  end

  @doc """
  The merged record for `key`, or `nil` if no replica has one.

  `member?/2` is this plus one predicate. It is separate because a caller that
  is about to cache the answer needs to know when the key expires, not just
  whether it is there now.
  """
  @spec lookup(Record.key(), keyword()) :: {:ok, Record.t() | nil} | error()
  def lookup(key, opts \\ []) when is_binary(key) do
    with {:ok, placement} <- placement(key, opts) do
      timeout = Keyword.get(opts, :timeout, Config.request_timeout())
      targets = placement.up
      r = quorum_size(Keyword.get(opts, :r, Config.r()), targets)

      outcome = Quorum.request(targets, {:get, key}, r, timeout)

      if length(outcome.ok) >= r do
        winner = outcome.ok |> Enum.map(fn {_node, record} -> record end) |> Record.merge_all()
        repair(outcome.ok, winner)
        {:ok, winner}
      else
        {:error, {:quorum_not_met, details(:read, r, placement, outcome)}}
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

  # A write stamped by a coordinator whose clock is behind what a replica
  # already holds loses the merge on every replica - and each of them still
  # answers {:ok, winner}, so it used to be acknowledged as done when it had
  # done nothing. Seen through a delete that a read on another node had just
  # repaired, with the add that followed it coordinated here: "ok", and the key
  # still absent.
  #
  # So the replies are checked. If the winner is not our record, the clock is
  # raised past it and the write goes again, once - which is what it would have
  # been stamped with had this node heard of that version first.
  defp write(key, alive?, opts), do: write(key, alive?, opts, 2)

  defp write(key, alive?, opts, attempts) do
    with {:ok, placement} <- placement(key, opts) do
      timeout = Keyword.get(opts, :timeout, Config.request_timeout())
      targets = placement.up
      w = quorum_size(Keyword.get(opts, :w, Config.w()), targets)
      record = Record.new(key, Clock.tick(), node(), alive?, nil, expiry(opts))

      outcome = Quorum.request(targets, {:put, record}, w, timeout)

      newer =
        for {_node, winner} <- outcome.ok,
            not Record.same_version?(winner, record),
            do: Record.lamport(winner)

      if newer != [] and attempts > 1 do
        Clock.observe(Enum.max(newer))
        write(key, alive?, opts, attempts - 1)
      else
        finish_write(outcome, w, placement, record)
      end
    end
  end

  defp finish_write(outcome, w, placement, record) do
    if length(outcome.ok) >= w do
      # We told the client yes, so every replica that did not take the write
      # gets a hint: the ones that were unreachable, and the ones that tried
      # and failed. A replayed hint is an ordinary idempotent put.
      failed = Enum.map(outcome.failed, fn {node, _reason} -> node end)
      Handoff.store_all(placement.down ++ failed, record)
      :ok
    else
      {:error, {:quorum_not_met, details(:write, w, placement, outcome)}}
    end
  end

  # A TTL is turned into an absolute instant by the coordinator, once, so every
  # replica stores the same deadline instead of each starting its own countdown
  # when the write happens to arrive.
  defp expiry(opts) do
    case Keyword.get(opts, :ttl) do
      nil ->
        :never

      ms when is_integer(ms) and ms > 0 ->
        System.system_time(:millisecond) + ms

      other ->
        raise ArgumentError,
              "ttl must be a positive number of milliseconds, got #{inspect(other)}"
    end
  end

  defp placement(key, opts) do
    n = Keyword.get(opts, :n, Config.n())

    case Placement.targets(key, n) do
      %{primaries: []} -> {:error, :ring_empty}
      %{up: []} -> {:error, :no_replicas_reachable}
      placement -> {:ok, placement}
    end
  end

  # How many acks we insist on. The reachable replica list can be shorter than
  # `n` when the cluster is smaller or nodes are down; `strict_quorum: true`
  # keeps the configured number and fails such requests, the default caps it at
  # the replicas that can actually answer and keeps serving.
  defp quorum_size(configured, prefs) do
    if Config.get(:strict_quorum) do
      configured
    else
      max(min(configured, length(prefs)), 1)
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
  # repair just means the next read repairs it instead. Replicas that did not
  # answer are the handoff's job, not this one's.
  defp repair(_answers, nil), do: :ok

  defp repair(answers, winner) do
    for {node, record} <- answers, not Record.same_version?(record, winner) do
      :erpc.cast(node, Kurwa.Replica, :put, [winner])
    end

    :ok
  end

  defp details(op, needed, placement, outcome) do
    %{
      op: op,
      needed: needed,
      got: length(outcome.ok),
      replicas: placement.primaries,
      unreachable: placement.down,
      failed: Map.new(outcome.failed)
    }
  end
end
