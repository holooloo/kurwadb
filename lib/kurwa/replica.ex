defmodule Kurwa.Replica do
  @moduledoc """
  The surface one node exposes to its peers.

  Every cross-node call in kurwadb goes through this module, so the wire
  protocol between nodes is exactly these five functions. Records arriving here
  came off the network, so they are shape-checked before they reach the engine.
  """

  alias Kurwa.Record
  alias Kurwa.Store

  @doc "Merges a replicated record into the local copy and returns the winner."
  @spec put(Record.t()) :: {:ok, Record.t()} | {:error, term()}
  def put(record) do
    if valid_record?(record) do
      case Store.put(record) do
        # A stale write is still a successful replication: this replica already
        # holds something at least as new.
        {:ok, winner} -> {:ok, winner}
        {:stale, winner} -> {:ok, winner}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, {:bad_record, record}}
    end
  end

  @doc """
  Merges a batch of replicated records. Used by `Kurwa.Handoff` when a replica
  comes back and has to catch up.
  """
  @spec put_many([Record.t()]) :: :ok | {:error, term()}
  def put_many(records) when is_list(records) do
    Enum.reduce_while(records, :ok, fn record, :ok ->
      case put(record) do
        {:ok, _winner} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc "Local version of `key`, tombstones included."
  @spec get(Record.key()) :: {:ok, Record.t() | nil} | {:error, term()}
  def get(key) when is_binary(key), do: Store.get(key)
  def get(other), do: {:error, {:bad_key, other}}

  @doc "Live keys held by this node (its share of the ring, replicas included)."
  @spec local_count() :: {:ok, non_neg_integer()}
  def local_count, do: {:ok, Store.count()}

  @doc "Cheap liveness probe, also used to tell kurwadb nodes from other BEAM nodes."
  @spec ping() :: :pong
  def ping, do: :pong

  defp valid_record?({key, lamport, origin, alive?, wall, expires_at})
       when is_binary(key) and is_integer(lamport) and lamport >= 0 and is_atom(origin) and
              is_boolean(alive?) and is_integer(wall) and
              (expires_at == :never or is_integer(expires_at)),
       do: true

  defp valid_record?(_), do: false
end
