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
  algebra over them, see `Kurwa.Namespace`. Every function here coordinates a
  quorum across the ring (`Kurwa.Coordinator`); for the deliberately node-local
  variants, see `Kurwa.Store`.
  """

  alias Kurwa.Cluster
  alias Kurwa.Config
  alias Kurwa.Coordinator
  alias Kurwa.Key
  alias Kurwa.Record
  alias Kurwa.Ring

  @type key :: Record.key()
  @type opts :: keyword()

  @doc """
  Adds `key` to the set. Idempotent.

  Options: `:n`, `:w`, `:timeout` override the configured defaults for this call.
  """
  @spec add(key(), opts()) :: :ok | {:error, term()}
  def add(key, opts \\ []) when is_binary(key), do: Coordinator.add(Key.encode(key), opts)

  @doc "Adds `key`, raising `Kurwa.Error` if the quorum is not met."
  @spec add!(key(), opts()) :: :ok
  def add!(key, opts \\ []), do: unwrap(add(key, opts))

  @doc "Removes `key` from the set. Idempotent."
  @spec delete(key(), opts()) :: :ok | {:error, term()}
  def delete(key, opts \\ []) when is_binary(key), do: Coordinator.delete(Key.encode(key), opts)

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
  def fetch(key, opts \\ []) when is_binary(key), do: Coordinator.member?(Key.encode(key), opts)

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
      vnodes: ring.vnodes,
      n: Config.n(),
      r: Config.r(),
      w: Config.w(),
      strict_quorum: Config.get(:strict_quorum),
      shards: Config.shards(),
      engine: Config.engine(),
      local_keys: Kurwa.Store.count(),
      lamport: Kurwa.Clock.peek()
    }
  end

  defp unwrap(:ok), do: :ok
  defp unwrap({:ok, value}), do: value
  defp unwrap({:error, reason}), do: raise(Kurwa.Error, reason: reason)
end
