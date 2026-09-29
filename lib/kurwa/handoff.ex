defmodule Kurwa.Handoff do
  @moduledoc """
  Writes that a replica was not around to receive, kept until it is.

  Read repair alone cannot close this gap: it only fixes replicas that answered
  inside the quorum window, so a key nobody reads never heals, and a replica that
  was down when the write happened was never even asked. This process remembers
  those writes per target node and replays them when the node is back.

  Queues are keyed by storage key and merged with the same last-writer-wins rule
  as the store, so a key written a hundred times while a replica was away replays
  once, with the newest version.

  Hints are durable. Each one is written to the local store as an ordinary
  record whose key says who owes it and for what, so it goes through the same
  write-ahead log as everything else and survives a restart of this node. The
  in-memory queues are a working set rebuilt from the store at boot, not the
  only copy.

  The durable write happens in the caller's process, not in this one: a node
  that is away during heavy writing would otherwise turn this process into the
  bottleneck for every write that misses it.

  One limit, stated rather than implied: a queue is bounded
  (`handoff_max_hints`). Once it is full, new hints for that node are refused
  and logged - a replica away long enough to overflow needs anti-entropy, not an
  ever-growing queue.
  """

  use GenServer

  alias Kurwa.Cluster
  alias Kurwa.Clock
  alias Kurwa.Config
  alias Kurwa.Key
  alias Kurwa.Record
  alias Kurwa.Store

  require Logger

  @name __MODULE__

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: @name)

  @doc """
  Remembers `record` for `node`, to be replayed when it is reachable.

  The durable part happens here, in the calling process, so that a burst of
  writes missing one replica does not queue up behind a single process.
  """
  @spec store(node(), Record.t()) :: :ok
  def store(node, record) do
    _ = Store.put(persistent_form(node, record))
    GenServer.cast(@name, {:store, node, record})
  end

  @doc "Remembers `record` for several nodes at once."
  @spec store_all([node()], Record.t()) :: :ok
  def store_all([], _record), do: :ok
  def store_all(nodes, record), do: Enum.each(nodes, &store(&1, record))

  @doc "Pending hints per node."
  @spec depth() :: %{node() => non_neg_integer()}
  def depth, do: GenServer.call(@name, :depth)

  @doc "Asks for a replay without waiting for it, e.g. when a node comes back."
  @spec kick() :: :ok
  def kick, do: GenServer.cast(@name, :replay_now)

  @doc "Tries to drain every queue right now. Returns what is left."
  @spec drain() :: %{node() => non_neg_integer()}
  def drain, do: GenServer.call(@name, :drain, 30_000)

  @impl true
  def init(_opts) do
    schedule()
    {:ok, %{queues: recover(), refused: %{}}}
  end

  # Hints written before the last restart are still in the store; the queues are
  # a working set, not the record of what is owed.
  defp recover do
    queues =
      Store.fold_system(%{}, fn record, queues ->
        with true <- Record.alive?(record),
             {:ok, target, key, original_alive?} <- Key.hint_parts(Record.key(record)) do
          original = original_form(record, key, original_alive?)
          Map.update(queues, target, %{key => original}, &Map.put(&1, key, original))
        else
          _ -> queues
        end
      end)

    pending = queues |> Map.values() |> Enum.map(&map_size/1) |> Enum.sum()

    if pending > 0 do
      Logger.info("kurwadb: recovered #{pending} hints for #{map_size(queues)} nodes")
    end

    queues
  end

  @impl true
  def handle_cast({:store, node, record}, state) do
    queue = Map.get(state.queues, node, %{})
    key = Record.key(record)
    max = Config.get(:handoff_max_hints)

    cond do
      Map.has_key?(queue, key) ->
        merged = Record.merge(Map.fetch!(queue, key), record)
        {:noreply, put_queue(state, node, Map.put(queue, key, merged))}

      map_size(queue) >= max ->
        {:noreply, refuse(state, node, max)}

      true ->
        {:noreply, put_queue(state, node, Map.put(queue, key, record))}
    end
  end

  @impl true
  def handle_cast(:replay_now, state), do: {:noreply, replay(state)}

  @impl true
  def handle_call(:depth, _from, state) do
    {:reply, Map.new(state.queues, fn {node, queue} -> {node, map_size(queue)} end), state}
  end

  @impl true
  def handle_call(:drain, _from, state) do
    state = replay(state)
    {:reply, Map.new(state.queues, fn {node, queue} -> {node, map_size(queue)} end), state}
  end

  @impl true
  def handle_info(:replay, state) do
    state = replay(state)
    schedule()
    {:noreply, state}
  end

  @impl true
  def handle_info(_other, state), do: {:noreply, state}

  defp replay(state) do
    reachable = Cluster.up()
    batch = Config.get(:handoff_batch)
    timeout = Config.request_timeout() * 2

    queues =
      Map.new(state.queues, fn {node, queue} ->
        if map_size(queue) > 0 and MapSet.member?(reachable, node) do
          {node, send_batch(node, queue, batch, timeout)}
        else
          {node, queue}
        end
      end)

    %{state | queues: Enum.reject(queues, fn {_node, queue} -> queue == %{} end) |> Map.new()}
  end

  defp send_batch(node, queue, batch, timeout) do
    {keys, records} =
      queue
      |> Enum.take(batch)
      |> Enum.reduce({[], []}, fn {key, record}, {keys, records} ->
        {[key | keys], [record | records]}
      end)

    case deliver(node, records, timeout) do
      :ok ->
        # Mark them delivered in the store, or a restart would replay them.
        Enum.each(records, fn record -> mark_delivered(node, record) end)

        Logger.info("kurwadb: handed off #{length(keys)} keys to #{node}")
        Map.drop(queue, keys)

      {:error, reason} ->
        Logger.debug("kurwadb: handoff to #{node} failed: #{inspect(reason)}")
        queue
    end
  end

  defp deliver(node, records, timeout) do
    if node == node() do
      Kurwa.Replica.put_many(records)
    else
      :erpc.call(node, Kurwa.Replica, :put_many, [records], timeout)
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # One line per node per overflow window, not one per refused hint.
  defp refuse(state, node, max) do
    now = System.monotonic_time(:millisecond)
    last = Map.get(state.refused, node, 0)

    if now - last > 60_000 do
      Logger.warning(
        "kurwadb: handoff queue for #{node} is full (#{max} hints), refusing new ones - " <>
          "that replica needs a repair pass"
      )

      %{state | refused: Map.put(state.refused, node, now)}
    else
      state
    end
  end

  defp put_queue(state, node, queue), do: %{state | queues: Map.put(state.queues, node, queue)}

  # A hint is a record whose key says who owes it, carrying the original's
  # stamp. `alive?` on the record means "still owed"; the original's own
  # liveness rides in the key.
  defp persistent_form(target, {key, lamport, origin, alive?, wall, expires_at}) do
    {Key.hint_key(target, key, alive?), lamport, origin, true, wall, expires_at}
  end

  defp original_form({_hint_key, lamport, origin, _pending, wall, expires_at}, key, alive?) do
    {key, lamport, origin, alive?, wall, expires_at}
  end

  # Both variants, not just the one we are holding. The original's liveness is
  # part of the hint key, so adding a key and then deleting it leaves two
  # durable hints while the in-memory queue - which dedupes by the original key
  # - only ever hands over the newer one. Clearing both is free here and leaves
  # nothing behind for a restart to replay.
  defp mark_delivered(target, {key, _lamport, _origin, _alive?, _wall, _expires}) do
    now = System.system_time(:millisecond)

    for original_alive? <- [true, false] do
      Store.put(
        {Key.hint_key(target, key, original_alive?), Clock.tick(), node(), false, now, :never}
      )
    end
  end

  defp schedule, do: Process.send_after(self(), :replay, Config.get(:handoff_interval))
end
