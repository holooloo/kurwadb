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

  Two limits, stated rather than implied:

  * hints live in memory on the coordinating node. If that node restarts before
    it drains them, they are gone and read repair is the only remaining path.
  * a queue is bounded (`handoff_max_hints`). Once it is full, new hints for that
    node are refused and logged - a replica that has been away long enough to
    overflow needs a real repair pass, not an ever-growing queue.
  """

  use GenServer

  alias Kurwa.Cluster
  alias Kurwa.Config
  alias Kurwa.Record

  require Logger

  @name __MODULE__

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: @name)

  @doc "Remembers `record` for `node`, to be replayed when it is reachable."
  @spec store(node(), Record.t()) :: :ok
  def store(node, record), do: GenServer.cast(@name, {:store, node, record})

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
    {:ok, %{queues: %{}, refused: %{}}}
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

  defp schedule, do: Process.send_after(self(), :replay, Config.get(:handoff_interval))
end
