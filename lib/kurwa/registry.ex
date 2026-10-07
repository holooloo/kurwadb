defmodule Kurwa.Registry do
  @moduledoc """
  Which named sets exist.

  The awkward part of the question is that answering it is enumeration, and
  enumeration is the one thing this store refuses to do. So the registry is not
  an index bolted on the side: **a set name is a key, in a set**. It replicates,
  merges, hands off and repairs itself through exactly the same path as
  everything else, with no new machinery at all.

  What makes it cheap to read is a local routing decision rather than a new data
  structure. `Kurwa.Store` sends keys in the reserved `_sets` namespace to a
  shard of their own, so listing folds a table holding one record per set
  instead of a table holding every key you have ever written.

  ## Two honest limits

  A name is registered the first time a key is added to that set, and stays
  until somebody calls `forget/1`. Removing it when the set empties would mean
  counting the set's keys, which is a scan. So this lists sets that have **ever**
  held a key, not sets that hold one now.

  `list/1` unions what every reachable node can see. With fewer replicas than
  nodes a single node holds only part of the registry, so a node that cannot be
  reached is reported rather than quietly dropped from the answer.
  """

  use GenServer

  alias Kurwa.Cluster
  alias Kurwa.Config
  alias Kurwa.Coordinator
  alias Kurwa.Key
  alias Kurwa.Quorum
  alias Kurwa.Record
  alias Kurwa.Store

  require Logger

  @name __MODULE__
  @table :kurwa_registered_sets

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: @name)

  @doc """
  Records that `name` exists, unless this node has already done so.

  Fire and forget: the caller's write has already been answered, and making it
  wait on a second quorum round would double the latency of every add to a named
  set for a fact that only needs recording once.
  """
  @spec register(binary()) :: :ok
  def register(name) when is_binary(name) do
    # insert_new is the whole deduplication: it is atomic, so a burst of
    # concurrent adds to a new set produces exactly one registry write.
    if new_here?(name) do
      Task.Supervisor.start_child(Kurwa.TaskSupervisor, fn -> write(name) end)
    end

    :ok
  end

  @doc """
  Has this node already written a registration for `name`?

  This is the deduplication cache, not the data: a name can be absent here and
  present in the store, because a registry write that failed its quorum may
  still have landed on some replicas. What it answers is whether the next add to
  that set will try again.
  """
  @spec registered_here?(binary()) :: boolean()
  def registered_here?(name) when is_binary(name) do
    :ets.member(@table, name)
  rescue
    ArgumentError -> false
  end

  @doc "Set names visible in this node's copy of the registry."
  @spec local() :: [binary()]
  def local, do: Enum.reject(local_names(), &schema_mark?/1)

  @doc false
  # The registry as stored here, schema records included: what other nodes ask
  # for when they list.
  def local_names do
    Store.fold_system([], fn record, acc ->
      with true <- Record.member?(record),
           {:ok, name} <- Key.registry_name(Record.key(record)) do
        [name | acc]
      else
        _ -> acc
      end
    end)
    |> Enum.sort()
  end

  @doc """
  Every set the cluster knows about, and the nodes that could not be asked.

  Ordinary reads go to a key's replicas; this one has to ask everybody, because
  the registry is spread across the ring like any other set.
  """
  @spec list(keyword()) :: {:ok, %{sets: [binary()], unreachable: map()}}
  def list(opts \\ []) do
    {:ok, %{sets: names} = listing} = names(opts)
    {:ok, %{listing | sets: Enum.reject(names, &schema_mark?/1)}}
  end

  # A schema created with CREATE SCHEMA and holding no sets yet is recorded as
  # a registry entry with this prefix. A slash cannot start a set name, so the
  # two never collide, and schemas replicate and repair like set names do.
  @schema_mark "/"

  defp schema_mark?(name), do: String.starts_with?(name, @schema_mark)

  @doc "Records an empty schema, so it is listed before it holds a set."
  @spec register_schema(binary()) :: :ok | {:error, term()}
  def register_schema(schema) when is_binary(schema) do
    name = @schema_mark <> schema
    :ets.insert(@table, {name})
    Coordinator.add(Key.registry_key(name))
  end

  @doc "Drops the record of a schema made by `register_schema/1`."
  @spec forget_schema(binary()) :: :ok | {:error, term()}
  def forget_schema(schema) when is_binary(schema), do: forget(@schema_mark <> schema)

  @doc """
  Every schema: those created and recorded, and the first part of every
  dotted set name - `analytics.events` is the set `events` in `analytics`.
  """
  @spec schemas(keyword()) :: {:ok, [binary()]}
  def schemas(opts \\ []) do
    {:ok, %{sets: names}} = names(opts)

    schemas =
      for name <- names, uniq: true do
        if schema_mark?(name),
          do: String.replace_prefix(name, @schema_mark, ""),
          else: name |> String.split(".", parts: 2) |> schema_of()
      end

    {:ok, schemas |> Enum.reject(&is_nil/1) |> Enum.sort()}
  end

  defp schema_of([schema, _table]), do: schema
  defp schema_of([_table]), do: nil

  defp names(opts) do
    timeout = Keyword.get(opts, :timeout, Config.request_timeout())
    nodes = Cluster.members()

    outcome = Quorum.run(nodes, &remote_local(&1, timeout), length(nodes), timeout)

    sets =
      outcome.ok
      |> Enum.flat_map(fn {_node, names} -> names end)
      |> Enum.uniq()
      |> Enum.sort()

    {:ok, %{sets: sets, unreachable: Map.new(outcome.failed)}}
  end

  @doc """
  Drops `name` from the registry.

  The set's keys are untouched - this only says "stop listing it". The next add
  to that set registers it again, which is the right behaviour: the registry
  describes what exists, and a set with keys in it does.
  """
  @spec forget(binary()) :: :ok | {:error, term()}
  def forget(name) when is_binary(name) do
    delete_here(name)
    Coordinator.delete(Key.registry_key(name))
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, %{}}
  end

  defp new_here?(name) do
    :ets.insert_new(@table, {name})
  rescue
    ArgumentError -> false
  end

  defp delete_here(name) do
    :ets.delete(@table, name)
  rescue
    ArgumentError -> true
  end

  defp write(name) do
    case Coordinator.add(Key.registry_key(name)) do
      :ok ->
        :ok

      {:error, reason} ->
        # Let the next add to this set try again rather than remembering a
        # registration that never happened.
        delete_here(name)
        Logger.debug("kurwadb: could not register set #{inspect(name)}: #{inspect(reason)}")
    end
  end

  defp remote_local(node, timeout) do
    if node == node() do
      {:ok, local_names()}
    else
      :erpc.call(node, Kurwa.Replica, :local_sets, [], timeout)
    end
  end
end
