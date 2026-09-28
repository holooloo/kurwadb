defmodule Kurwa.Store do
  @moduledoc """
  Node-local view of the data: routes a key to its shard and talks to the engine.

  Everything here is local only. Replication and quorums live in
  `Kurwa.Coordinator`.
  """

  alias Kurwa.Config
  alias Kurwa.Key
  alias Kurwa.Record
  alias Kurwa.Store.Shard

  @doc """
  Which local shard holds `key`.

  System keys get a shard of their own. They replicate exactly like any other
  key - this is purely local routing - but keeping them apart means enumerating
  them costs the number of system keys instead of a fold over everything.
  """
  @spec shard_for(Record.key()) :: non_neg_integer()
  def shard_for(key) when is_binary(key) do
    if Key.system?(key), do: system_shard(), else: :erlang.phash2(key, Config.shards())
  end

  @doc "The shard reserved for kurwadb's own keys. Always the last one."
  @spec system_shard() :: non_neg_integer()
  def system_shard, do: Config.shards()

  @doc "Folds over the system shard alone, tombstones included."
  @spec fold_system(acc, (Record.t(), acc -> acc)) :: acc when acc: term()
  def fold_system(acc, fun) when is_function(fun, 2) do
    engine().fold(handle(system_shard()), acc, fun)
  rescue
    ArgumentError -> acc
  end

  @doc "Directory holding shard `index`. Node-scoped, so several nodes can share a data dir."
  @spec dir(non_neg_integer()) :: Path.t()
  def dir(index) do
    node_dir = node() |> Atom.to_string() |> String.replace(~r/[^A-Za-z0-9_-]/, "_")
    Path.join([Config.data_dir(), node_dir, "shard_#{index}"])
  end

  @doc "Merges `record` into the local copy."
  @spec put(Record.t()) :: {:ok, Record.t()} | {:stale, Record.t()} | {:error, term()}
  def put(record) do
    record |> Record.key() |> shard_for() |> Shard.put(record)
  end

  @doc """
  Local version of `key`, tombstones included.

  `{:error, :unavailable}` means this replica cannot answer right now (shard
  restarting, engine not open yet) - the caller must count that as a failed
  replica rather than as "key absent".
  """
  @spec get(Record.key()) :: {:ok, Record.t() | nil} | {:error, :unavailable}
  def get(key) when is_binary(key) do
    index = shard_for(key)
    {:ok, engine().get(handle(index), key)}
  rescue
    ArgumentError -> {:error, :unavailable}
  end

  @doc "Local live keys across all shards."
  @spec count() :: non_neg_integer()
  def count do
    Enum.reduce(shards(), 0, fn index, acc -> acc + engine().count(handle(index)) end)
  rescue
    ArgumentError -> 0
  end

  @doc "Folds over every local record, tombstones included."
  @spec fold(acc, (Record.t(), acc -> acc)) :: acc when acc: term()
  def fold(acc, fun) when is_function(fun, 2) do
    Enum.reduce(shards(), acc, fn index, acc -> engine().fold(handle(index), acc, fun) end)
  end

  @doc "Forces an fsync on every shard."
  def sync, do: Enum.each(shards(), &Shard.sync/1)

  @doc "Forces a compaction on every shard."
  def compact, do: Enum.each(shards(), &Shard.compact/1)

  @doc "Forces a tombstone sweep on every shard. Returns the total dropped."
  def gc do
    Enum.reduce(shards(), 0, fn index, acc ->
      {:ok, dropped} = Shard.gc(index)
      acc + dropped
    end)
  end

  defp shards, do: 0..Config.shards()
  defp engine, do: Config.engine()
  defp handle(index), do: engine().handle(Shard.name(index))
end
