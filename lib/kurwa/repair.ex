defmodule Kurwa.Repair do
  @moduledoc """
  Active anti-entropy: finds replicas that have silently drifted apart, without
  anyone having read the keys involved.

  The other two repair mechanisms are reactive. Hinted handoff needs a
  coordinator that noticed a replica was unreachable; read repair needs somebody
  to read the key. Neither covers the case where a replica *acknowledged* a write
  and then lost it - a crash before the WAL was fsynced, a restore from an old
  snapshot, a disk that lied. Nobody knows that happened, so nobody fixes it, and
  a key nobody reads stays wrong forever.

  This process goes looking.

  ## Why there is no tree here

  Anti-entropy is usually a Merkle tree: hash the keys into leaves, hash the
  leaves into parents, compare roots, and descend only where they differ, so two
  replicas holding millions of keys exchange a few kilobytes.

  With no values the tree is not worth building. A leaf digest is `phash2` over a
  handful of fixed-size fields, and the whole vector of #{4096} bucket digests is
  32 KB - one message, one round trip, no descent. The comparison that a tree
  exists to avoid is cheaper than the tree.

  ## What a round does

  1. Fold the local store into a digest per bucket, counting only keys that this
     node *and* the peer are both supposed to hold. Comparing whole stores would
     report every key the peer legitimately does not have.
  2. Ask the peer for the same vector.
  3. For each bucket where they disagree, swap the records in that bucket both
     ways and merge. Merging is idempotent and convergent, so a full swap of a
     small bucket is simpler than working out who is behind, and correct either
     way.

  Digests are computed by folding ETS on demand, in this process, never on the
  write path: the tables are `:protected`, so reading them costs the shards
  nothing. A round is work, so it is bounded - see `repair_max_buckets`.
  """

  use GenServer

  alias Kurwa.Cluster
  alias Kurwa.Config
  alias Kurwa.Key
  alias Kurwa.Record
  alias Kurwa.Ring
  alias Kurwa.Store

  require Logger

  @name __MODULE__

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: @name)

  @doc "Runs one round against `peer` now, and reports what it found."
  @spec run(node(), timeout()) :: {:ok, map()} | {:error, term()}
  def run(peer, timeout \\ 60_000), do: GenServer.call(@name, {:run, peer}, timeout)

  @doc "Runs one round against every reachable peer."
  @spec run_all(timeout()) :: [{node(), {:ok, map()} | {:error, term()}}]
  def run_all(timeout \\ 120_000), do: GenServer.call(@name, :run_all, timeout)

  @doc """
  Digest vector for the keys this node shares with `peer`.

  Public because the peer calls it through `Kurwa.Replica`; the two sides have to
  compute it the same way for the comparison to mean anything.
  """
  @spec digest(node()) :: %{buckets: tuple(), keys: non_neg_integer()}
  def digest(peer) do
    buckets = Config.get(:repair_buckets)
    counters = :counters.new(buckets, [:atomics])
    shared = shared?(peer)

    keys =
      Store.fold(0, fn record, count ->
        key = Record.key(record)

        if shared.(key) do
          :counters.add(counters, bucket_of(key, buckets), record_digest(record))
          count + 1
        else
          count
        end
      end)

    %{buckets: Enum.map(1..buckets, &:counters.get(counters, &1)) |> List.to_tuple(), keys: keys}
  end

  @doc "Every record this node holds in `index`, restricted to what it shares with `peer`."
  @spec bucket(node(), pos_integer()) :: [Record.t()]
  def bucket(peer, index) do
    buckets = Config.get(:repair_buckets)
    shared = shared?(peer)

    Store.fold([], fn record, acc ->
      key = Record.key(record)

      if bucket_of(key, buckets) == index and shared.(key),
        do: [record | acc],
        else: acc
    end)
  end

  @impl true
  def init(_opts) do
    schedule()
    {:ok, %{rounds: 0, repaired: 0, next_peer: 0}}
  end

  @impl true
  def handle_call({:run, peer}, _from, state) do
    {result, state} = round(peer, state)
    {:reply, result, state}
  end

  @impl true
  def handle_call(:run_all, _from, state) do
    {results, state} =
      Enum.reduce(peers(), {[], state}, fn peer, {acc, state} ->
        {result, state} = round(peer, state)
        {[{peer, result} | acc], state}
      end)

    {:reply, Enum.reverse(results), state}
  end

  @impl true
  def handle_call(:stats, _from, state),
    do: {:reply, Map.take(state, [:rounds, :repaired]), state}

  @impl true
  def handle_info(:tick, state) do
    # One peer per tick, round robin: a round is a fold of the whole store, and
    # doing every peer at once would make that spike.
    state =
      case peers() do
        [] ->
          state

        peers ->
          peer = Enum.at(peers, rem(state.next_peer, length(peers)))
          {_result, state} = round(peer, state)
          %{state | next_peer: state.next_peer + 1}
      end

    schedule()
    {:noreply, state}
  end

  @impl true
  def handle_info(_other, state), do: {:noreply, state}

  @doc "Rounds run and records repaired since boot."
  def stats, do: GenServer.call(@name, :stats)

  defp round(peer, state) do
    started = System.monotonic_time(:millisecond)
    mine = digest(peer)

    case remote_digest(peer) do
      {:ok, theirs} ->
        diverged = compare(mine.buckets, theirs.buckets)
        limit = Config.get(:repair_max_buckets)
        {to_fix, deferred} = Enum.split(diverged, limit)

        repaired = Enum.reduce(to_fix, 0, fn index, acc -> acc + exchange(peer, index) end)

        if repaired > 0 or diverged != [] do
          Logger.info(
            "kurwadb: anti-entropy with #{peer}: #{length(diverged)} of " <>
              "#{tuple_size(mine.buckets)} buckets diverged, #{repaired} records exchanged" <>
              if(deferred == [],
                do: "",
                else: ", #{length(deferred)} buckets left for next round"
              )
          )
        end

        result = %{
          peer: peer,
          keys: mine.keys,
          diverged: length(diverged),
          repaired: repaired,
          deferred: length(deferred),
          took_ms: System.monotonic_time(:millisecond) - started
        }

        {{:ok, result}, %{state | rounds: state.rounds + 1, repaired: state.repaired + repaired}}

      {:error, reason} ->
        Logger.debug("kurwadb: anti-entropy with #{peer} failed: #{inspect(reason)}")
        {{:error, reason}, state}
    end
  end

  defp compare(mine, theirs) when tuple_size(mine) == tuple_size(theirs) do
    for i <- 1..tuple_size(mine), elem(mine, i - 1) != elem(theirs, i - 1), do: i
  end

  # A peer configured with a different bucket count cannot be compared at all;
  # saying so beats silently repairing everything.
  defp compare(mine, theirs) do
    Logger.warning(
      "kurwadb: anti-entropy peer has #{tuple_size(theirs)} buckets, we have " <>
        "#{tuple_size(mine)} - skipping, repair_buckets must match across the cluster"
    )

    []
  end

  # Swap the bucket both ways. Merging is idempotent, so this needs no agreement
  # about who is behind - whoever is, ends up right.
  defp exchange(peer, index) do
    ours = bucket(peer, index)

    with {:ok, theirs} <- remote_bucket(peer, index) do
      Enum.each(theirs, &Store.put/1)
      _ = remote_put(peer, ours)
      length(theirs) + length(ours)
    else
      {:error, _reason} -> 0
    end
  end

  defp remote_digest(peer), do: safe(peer, :digest, [node()])
  defp remote_bucket(peer, index), do: safe(peer, :bucket, [node(), index])
  defp remote_put(peer, records), do: safe(peer, :put_many, [records])

  defp safe(peer, fun, args) do
    {:ok, :erpc.call(peer, Kurwa.Replica, fun, args, Config.request_timeout() * 10)}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # Only keys both nodes are supposed to hold. Built once per round so the ring
  # and the replica count are read once rather than per key.
  defp shared?(peer) do
    ring = Cluster.ring()
    n = Config.n()
    me = node()

    fn key ->
      # A hint records what this node owes someone else; it is nobody else's
      # business and must never be compared or exchanged.
      if Key.local_only?(key) do
        false
      else
        prefs = Ring.preflist(ring, key, n)
        me in prefs and (peer == me or peer in prefs)
      end
    end
  end

  defp bucket_of(key, buckets), do: :erlang.phash2(key, buckets) + 1

  # `wall` is left out on purpose: it records when a write happened, not what the
  # key is, and a difference there is not divergence worth exchanging keys over.
  defp record_digest({key, lamport, origin, alive?, _wall, expires_at}) do
    :erlang.phash2({key, lamport, origin, alive?, expires_at}, 4_294_967_296)
  end

  defp peers, do: Enum.reject(MapSet.to_list(Cluster.up()), &(&1 == node()))

  defp schedule, do: Process.send_after(self(), :tick, Config.get(:repair_interval))
end
