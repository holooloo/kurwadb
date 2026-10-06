defmodule Kurwa do
  @moduledoc """
  kurwadb - a distributed set. Keys, and nothing else.

      Kurwa.add("order:1029")      #=> :ok
      Kurwa.member?("order:1029")  #=> true
      Kurwa.delete("order:1029")   #=> :ok
      Kurwa.member?("order:1029")  #=> false

  There are no values, no scans and no queries: every operation names exactly one
  key. That constraint is what lets the whole store be an `:ets` set behind a
  hash ring, with merges that need no coordination.

  These functions work on the default, unnamed set. For named sets and for set
  algebra over them, see `Kurwa.Namespace`. Everything here goes through
  `Kurwa.Extractor` (cache and single-flight, both pass-through unless the cache
  is enabled) and then `Kurwa.Coordinator`, which is the quorum across the ring;
  for the deliberately node-local variants, see `Kurwa.Store`.
  """

  alias Kurwa.Cluster
  alias Kurwa.Config
  alias Kurwa.Coordinator
  alias Kurwa.Extractor
  alias Kurwa.Key
  alias Kurwa.Record
  alias Kurwa.Ring

  @type key :: Record.key()
  @type opts :: keyword()

  @doc """
  Adds `key` to the set. Idempotent.

  Options: `ttl: milliseconds` makes the key expire on its own; `:n`, `:w` and
  `:timeout` override the configured defaults for this call.

      Kurwa.add("seen:event:88", ttl: :timer.minutes(10))

  A second `add` replaces the deadline rather than extending it, because the
  newer write simply wins the merge.
  """
  @spec add(key(), opts()) :: :ok | {:error, term()}
  def add(key, opts \\ []) when is_binary(key), do: Extractor.add(key, opts)

  @doc """
  Adds `key` only if it is not already there: `:ok` if this call added it,
  `:exists` if not. Of concurrent calls for one absent key, at most one gets
  `:ok` - see `Kurwa.Coordinator.add_new/2` for how, and for when none does.
  """
  @spec add_new(key(), opts()) :: :ok | :exists | {:error, term()}
  def add_new(key, opts \\ []) when is_binary(key), do: Extractor.add_new(key, opts)

  @doc "Adds `key`, raising `Kurwa.Error` if the quorum is not met."
  @spec add!(key(), opts()) :: :ok
  def add!(key, opts \\ []), do: unwrap(add(key, opts))

  @doc "Removes `key` from the set. Idempotent."
  @spec delete(key(), opts()) :: :ok | {:error, term()}
  def delete(key, opts \\ []) when is_binary(key), do: Extractor.delete(key, opts)

  @doc "Removes `key`, raising `Kurwa.Error` if the quorum is not met."
  @spec delete!(key(), opts()) :: :ok
  def delete!(key, opts \\ []), do: unwrap(delete(key, opts))

  @doc """
  Is `key` in the set?

  Raises `Kurwa.Error` when the read quorum cannot be reached - which is the
  point: a membership check that silently answers `false` because replicas were
  unreachable is worse than one that fails.
  """
  @spec member?(key(), opts()) :: boolean()
  def member?(key, opts \\ []), do: unwrap(fetch(key, opts))

  @doc "Like `member?/2`, but returns the error instead of raising."
  @spec fetch(key(), opts()) :: {:ok, boolean()} | {:error, term()}
  def fetch(key, opts \\ []) when is_binary(key), do: Extractor.member?(key, opts)

  @doc """
  Milliseconds until `key` expires.

  `:never` for a key with no expiry, `nil` when it is not a member. Always reads
  the cluster, never the extractor cache, because the cache remembers the answer
  and not the deadline.
  """
  @spec ttl(key(), opts()) :: {:ok, non_neg_integer() | :never | nil} | {:error, term()}
  def ttl(key, opts \\ []) when is_binary(key) do
    with {:ok, record} <- Coordinator.lookup(Key.encode(key), opts) do
      if Record.member?(record), do: {:ok, Record.ttl(record)}, else: {:ok, nil}
    end
  end

  @doc "Approximate number of live keys in the cluster. See `Kurwa.Coordinator.count/1`."
  @spec count(opts()) :: {:ok, map()} | {:error, term()}
  defdelegate count(opts \\ []), to: Coordinator

  @doc "What this node knows about itself and the cluster."
  @spec info() :: map()
  def info do
    ring = Cluster.ring()

    %{
      node: node(),
      members: Ring.nodes(ring),
      up: ring |> Ring.nodes() |> Enum.filter(&MapSet.member?(Cluster.up(), &1)),
      down: Cluster.down(),
      handoff: Kurwa.Handoff.depth(),
      vnodes: ring.vnodes,
      n: Config.n(),
      r: Config.r(),
      w: Config.w(),
      strict_quorum: Config.get(:strict_quorum),
      shards: Config.shards(),
      engine: Config.engine(),
      local_keys: Kurwa.Store.count(),
      lamport: Kurwa.Clock.peek(),
      cache: Extractor.stats()
    }
  end

  defp unwrap(:ok), do: :ok
  defp unwrap({:ok, value}), do: value
  defp unwrap({:error, reason}), do: raise(Kurwa.Error, reason: reason)
end
