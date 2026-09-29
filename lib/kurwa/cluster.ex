defmodule Kurwa.Cluster do
  @moduledoc """
  Membership and the ring.

  Erlang distribution already gives us a full mesh and node up/down events, so
  this process does not implement gossip - it listens to `:net_kernel`, keeps the
  set of *verified* kurwadb nodes (a peer must answer `Kurwa.Replica.ping/0`,
  which keeps unrelated BEAM nodes out of the ring), and republishes the ring on
  every change.

  Two sets, not one, and the difference is load-bearing:

  * `members` - every node we have ever verified. The ring is built from these,
    so a key's replicas do not move because a node blinked.
  * `up` - the members we can reach right now. Writes go to these; the rest
    become hints for `Kurwa.Handoff`.

  A node leaves `members` only when an operator says so (`forget/1`).

  Membership is also *learned*, not only verified. On reaching a peer, this node
  takes the peer's member list and adds anything new to its own - as known, not
  as reachable. Without that a restarted node would remember only the nodes it
  could reach at that moment, compute its ring over a smaller cluster than its
  peers, and place the same key on different replicas than they do. It also
  means one seed is enough to join: the rest of the cluster arrives with the
  first answer.

  The ring itself lives in `:persistent_term` so that the request path reads it
  with no copy and no process hop. Membership changes are rare, which is exactly
  the access pattern `:persistent_term` is for.
  """

  use GenServer

  alias Kurwa.Config
  alias Kurwa.Ring

  require Logger

  @pt_ring {__MODULE__, :ring}
  @pt_up {__MODULE__, :up}

  @doc "Current ring, built from every known member. Safe to call before the process starts."
  @spec ring() :: Ring.t()
  def ring do
    # Not `:persistent_term.get(key, empty_ring())`: arguments are evaluated
    # eagerly, so that builds a throwaway 128-point ring on every single request.
    case :persistent_term.get(@pt_ring, nil) do
      nil -> empty_ring()
      ring -> ring
    end
  end

  @doc "Every kurwadb node we have verified, reachable or not."
  @spec members() :: [node()]
  def members, do: Ring.nodes(ring())

  @doc "The members we can reach right now."
  @spec up() :: MapSet.t(node())
  def up do
    case :persistent_term.get(@pt_up, nil) do
      nil -> MapSet.new([node()])
      up -> up
    end
  end

  @doc "Known members we cannot reach."
  @spec down() :: [node()]
  def down do
    reachable = up()
    Enum.reject(members(), &MapSet.member?(reachable, &1))
  end

  @doc """
  Drops `node` from the ring for good.

  Until an operator does this, a node that is down keeps its share of the
  keyspace and its pending hints, because its data is still its responsibility.
  """
  @spec forget(node()) :: :ok
  def forget(node), do: GenServer.call(__MODULE__, {:forget, node})

  @doc "Asks the cluster process to re-check `node` right now."
  def check(node), do: GenServer.cast(__MODULE__, {:check, node})

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :ok = :net_kernel.monitor_nodes(true, [{:node_type, :visible}])

    state = %{members: MapSet.new([node()]), up: MapSet.new([node()])}
    publish(state)

    connect_seeds()
    Enum.each(Node.list(), &check/1)
    schedule_seeds()

    {:ok, state}
  end

  @impl true
  def handle_call({:forget, node}, _from, state) do
    if node == node() do
      {:reply, {:error, :cannot_forget_self}, state}
    else
      state = %{
        state
        | members: MapSet.delete(state.members, node),
          up: MapSet.delete(state.up, node)
      }

      Logger.warning("kurwadb: forgetting #{node}: its keyspace is being reassigned")
      publish(state)
      {:reply, :ok, state}
    end
  end

  @impl true
  def handle_cast({:check, node}, state) do
    if MapSet.member?(state.up, node) do
      {:noreply, state}
    else
      verify(node)
      {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:join, node}, state) do
    if MapSet.member?(state.up, node) do
      {:noreply, state}
    else
      known? = MapSet.member?(state.members, node)
      state = %{state | members: MapSet.put(state.members, node), up: MapSet.put(state.up, node)}

      Logger.info(
        "kurwadb: node #{if known?, do: "is back", else: "joined"}: #{node} " <>
          "(up: #{MapSet.size(state.up)}/#{MapSet.size(state.members)})"
      )

      publish(state)
      # Whatever it missed while it was away can go out now.
      Kurwa.Handoff.kick()
      {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:learn, nodes}, state) do
    fresh = Enum.reject(nodes, &MapSet.member?(state.members, &1))

    if fresh == [] do
      {:noreply, state}
    else
      members = Enum.reduce(fresh, state.members, &MapSet.put(&2, &1))

      Logger.info(
        "kurwadb: learned about #{Enum.join(fresh, ", ")} from a peer " <>
          "(members: #{MapSet.size(members)})"
      )

      state = %{state | members: members}
      publish(state)

      # They are known now, but not yet reachable: check the ones that answer.
      Enum.each(fresh, &check/1)
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({:nodeup, node, _info}, state) do
    verify(node)
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node, _info}, state) do
    if MapSet.member?(state.up, node) do
      state = %{state | up: MapSet.delete(state.up, node)}

      Logger.warning(
        "kurwadb: node unreachable: #{node} (up: #{MapSet.size(state.up)}/" <>
          "#{MapSet.size(state.members)}) - its writes are being kept as hints"
      )

      publish(state)
      {:noreply, state}
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
          # Whoever this node knows about, we should know about too.
          case safe_members(node) do
            {:ok, members} -> GenServer.cast(parent, {:learn, members})
            _ -> :ok
          end

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

  defp safe_members(node) do
    :erpc.call(node, Kurwa.Replica, :members, [], 2_000)
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

  defp publish(state) do
    :persistent_term.put(@pt_ring, Ring.new(MapSet.to_list(state.members), Config.vnodes()))
    :persistent_term.put(@pt_up, state.up)
  end

  defp empty_ring, do: Ring.new([node()], Config.vnodes())
end
