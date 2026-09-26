defmodule Kurwa.Store.Engine do
  @moduledoc """
  What a storage engine has to provide.

  Two kinds of operations, on purpose:

  * write-side callbacks take the engine `state` and are only ever called from
    the owning `Kurwa.Store.Shard` process;
  * read-side callbacks take a cheap `handle/1`, so a membership check runs in
    the caller process with no message passing at all.

  `Kurwa.Store.Ets` is the engine shipped today (ETS + write-ahead log). An
  on-disk LSM engine can be added later without touching the cluster layer, as
  long as it answers these callbacks.
  """

  alias Kurwa.Record

  @type name :: atom()
  @type state :: term()
  @type handle :: term()

  @doc "Opens (and recovers) the engine instance called `name`."
  @callback open(name(), keyword()) :: {:ok, state()} | {:error, term()}

  @doc "Flushes and releases everything the instance holds."
  @callback close(state()) :: :ok

  @doc "Process-independent read handle for an already-open instance."
  @callback handle(name()) :: handle()

  @doc "Merges `record` in. Returns the winner, which may be the record we already had."
  @callback put(state(), Record.t()) :: {:ok | :stale, Record.t(), state()}

  @doc "Current version of `key`, tombstones included, or `nil`."
  @callback get(handle(), Record.key()) :: Record.t() | nil

  @doc "Number of live keys (tombstones excluded)."
  @callback count(handle()) :: non_neg_integer()

  @doc "Folds over every record, tombstones included."
  @callback fold(handle(), acc, (Record.t(), acc -> acc)) :: acc when acc: term()

  @doc "Makes everything written so far durable."
  @callback sync(state()) :: :ok

  @doc "Drops tombstones older than `cutoff` (wall-clock ms). Returns how many went."
  @callback gc(state(), integer()) :: {non_neg_integer(), state()}

  @doc "Compacts if the engine thinks it is time (snapshot + log truncation for ETS)."
  @callback maybe_compact(state()) :: state()

  @doc "Compacts right now."
  @callback compact(state()) :: state()
end
