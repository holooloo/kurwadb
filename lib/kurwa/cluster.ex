defmodule Kurwa.Cluster do
  @moduledoc """
  Membership and the ring.

  Erlang distribution already gives us a full mesh and node up/down events, so
  this process does not implement gossip - it listens to `:net_kernel`, keeps the
  set of *verified* kurwadb nodes (a peer must answer `Kurwa.Replica.ping/0`,
  which keeps unrelated BEAM nodes out of the ring), and republishes the ring on
  every change.

  The ring itself lives in `:persistent_term` so that the request path reads it
  with no copy and no process hop. Membership changes are rare, which is exactly
  the access pattern `:persistent_term` is for.
  """

  use GenServer

  alias Kurwa.Config
  alias Kurwa.Ring

  require Logger

  @pt_key {__MODULE__, :ring}

  @doc "Current ring. Safe to call from anywhere, including before the process starts."
  @spec ring() :: Ring.t()
  def ring, do: :persistent_term.get(@pt_key, empty_ring())

  @doc "Verified kurwadb nodes, this one included."
  @spec members() :: [node()]
  def members, do: Ring.nodes(ring())

  @doc "Asks the cluster process to re-check `node` right now."
  def check(node), do: GenServer.cast(__MODULE__, {:check, node})

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :ok = :net_kernel.monitor_nodes(true, [{:node_type, :visible}])

    state = %{members: MapSet.new([node()])}
    publish(state.members)

    connect_seeds()
    Enum.each(Node.list(), &check/1)
    schedule_seeds()

    {:ok, state}
  end

  @impl true
  def handle_cast({:check, node}, state) do
    if MapSet.member?(state.members, node) do
      {:noreply, state}
    else
      verify(node)
      {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:join, node}, state) do
    if MapSet.member?(state.members, node) do
      {:noreply, state}
    else
      members = MapSet.put(state.members, node)
      Logger.info("kurwadb: node joined: #{node} (members: #{MapSet.size(members)})")
      publish(members)
      {:noreply, %{state | members: members}}
    end
  end

  @impl true
  def handle_info({:nodeup, node, _info}, state) do
    verify(node)
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node, _info}, state) do
    if MapSet.member?(state.members, node) do
      members = MapSet.delete(state.members, node)
      Logger.warning("kurwadb: node left: #{node} (members: #{MapSet.size(members)})")
      publish(members)
      {:noreply, %{state | members: members}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(:connect_seeds, state) do
    connect_seeds()
    schedule_seeds()
    {:noreply, state}
  end

  @impl true
  def handle_info(_other, state), do: {:noreply, state}

  # A peer is only allowed into the ring once it answers as a kurwadb node.
  # Done off-process: an unreachable peer must not stall membership handling.
  defp verify(node) do
    parent = self()

    spawn(fn ->
      case safe_ping(node) do
        :pong ->
          GenServer.cast(parent, {:join, node})

        other ->
          Logger.debug("kurwadb: ignoring non-kurwadb node #{node}: #{inspect(other)}")
      end
    end)
  end

  defp safe_ping(node) do
    :erpc.call(node, Kurwa.Replica, :ping, [], 2_000)
  catch
    kind, reason -> {kind, reason}
  end

  defp connect_seeds do
    connected = Node.list()

    for seed <- Config.seeds(), seed != node(), seed not in connected do
      case Node.connect(seed) do
        true -> Logger.info("kurwadb: connected to seed #{seed}")
        _ -> Logger.debug("kurwadb: seed #{seed} unreachable")
      end
    end
  end

  defp schedule_seeds do
    if Config.seeds() != [] do
      Process.send_after(self(), :connect_seeds, Config.get(:seed_retry_interval))
    end
  end

  defp publish(members) do
    :persistent_term.put(@pt_key, Ring.new(MapSet.to_list(members), Config.vnodes()))
  end

  defp empty_ring, do: Ring.new([node()], Config.vnodes())
end
