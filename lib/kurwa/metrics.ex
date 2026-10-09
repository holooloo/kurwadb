defmodule Kurwa.Metrics do
  @moduledoc """
  What this node is doing, cheaply enough to leave on: counters bumped on the
  request path, and a sampler that turns them into per-second rates once a
  second. The dashboard (`/dashboard`) reads `cluster/0`.

  The counters are one `:counters` array, written with atomics and no process
  in the way, so counting a request costs a few nanoseconds. What is counted
  follows a request through the node:

    * **frontends** - requests, errors and time spent, per protocol;
    * **coordinator** - reads and writes it ran a quorum for;
    * **quorum** - replica requests answered on this node, and sent to and
      received from each peer (an ETS counter per peer, since peers come and go);
    * **shards** - reads and writes each local shard served.

  Connections are not counted here: each listener knows its own.
  """

  use GenServer

  alias Kurwa.Config

  @frontends [:http, :pg, :resp, :mysql, :mongo, :mssql, :ninep]
  @per_frontend 3
  @coordinator [:reads, :writes]
  @clients :kurwa_metrics_clients
  @history 120

  # Counter positions, worked out at compile time: the request path pays for
  # an atomic add, not for finding where to add. Layout: three per frontend
  # (requests, errors, time), then coordinator reads and writes, local replica
  # requests, then two per shard (gets, puts).
  @frontend_base for {f, i} <- Enum.with_index(@frontends), into: %{}, do: {f, i * @per_frontend}
  @coordinator_base length(@frontends) * @per_frontend
  @reads @coordinator_base + 1
  @writes @coordinator_base + 2
  @local @coordinator_base + 3
  @shard_base @coordinator_base + 3
  @ref_key {__MODULE__, :ref}

  @compile {:inline, bump: 1, ref: 0}

  # ----------------------------------------------------------------- clients

  @doc """
  Records the calling process as a client connection on `frontend`, until it
  exits. Call from the connection's own process.
  """
  def connect(frontend, socket) do
    peer =
      case ThousandIsland.Socket.peername(socket) do
        {:ok, {ip, port}} -> "#{:inet.ntoa(ip)}:#{port}"
        _ -> "?"
      end

    :ets.insert(@clients, {self(), frontend, peer, System.system_time(:second), nil, nil, 0})
    Process.put(@clients, true)
    GenServer.cast(__MODULE__, {:monitor, self()})
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  @doc "Names the calling connection: `user:` and `app:` (the client application)."
  def identify(fields) do
    updates =
      for {key, pos} <- [user: 5, app: 6],
          value = Keyword.get(fields, key),
          value not in [nil, ""],
          do: {pos, to_string(value)}

    if updates != [], do: :ets.update_element(@clients, self(), updates)
    :ok
  rescue
    _ -> :ok
  end

  # Only a process that connect/2 registered has a row: asking the process
  # dictionary first keeps everyone else (HTTP's request processes) from
  # paying for a failed update and the exception it raises.
  defp count_client do
    if Process.get(@clients) do
      :ets.update_counter(@clients, self(), {7, 1})
    end
  rescue
    _ -> :ok
  end

  # ---------------------------------------------------------------- counting

  @doc "Counts one request on `frontend` that took from `started` (native time) until now."
  def request(frontend, started) do
    case ref() do
      nil ->
        :ok

      ref ->
        base = frontend_base(frontend)
        count_client()
        :counters.add(ref, base + 1, 1)
        :counters.add(ref, base + 3, System.monotonic_time() - started)
    end
  end

  @doc "Runs `fun` as one request on `frontend`."
  def measure(frontend, fun) do
    started = System.monotonic_time()

    try do
      fun.()
    after
      request(frontend, started)
    end
  end

  @doc "Counts a request on `frontend` that ended in an error."
  def error(frontend), do: bump(frontend_base(frontend) + 2)

  @doc "Counts a coordinator operation: :reads or :writes."
  def coordinator(:reads), do: bump(@reads)
  def coordinator(:writes), do: bump(@writes)

  @doc "Counts a replica request answered on this node for its own coordinator."
  def local_replica, do: bump(@local)

  @doc "Counts replica requests sent to `peer`."
  def sent(peer, n \\ 1), do: peer_bump({:sent, peer}, n)

  @doc "Counts a replica request received from `peer`."
  def received(peer), do: peer_bump({:received, peer}, 1)

  @doc "Counts a read (:get) or write (:put) on local shard `index`."
  def shard(index, :get), do: bump(@shard_base + index * 2 + 1)
  def shard(index, :put), do: bump(@shard_base + index * 2 + 2)

  defp bump(i) do
    case ref() do
      nil -> :ok
      ref -> :counters.add(ref, i, 1)
    end
  end

  # Per-peer counts live in a :counters array too - an ETS counter per peer
  # was one row that every concurrent request wrote, and under load the
  # requests queued on it. A peer gets a slot the first time it is seen
  # (`@peers` maps it), which is the only time this goes through the server.
  @peer_slots 256
  @peers_ref {__MODULE__, :peers}

  defp peer_bump({kind, peer}, n) do
    with %{} = slots <- :persistent_term.get(@peers_ref, nil),
         {:ok, slot} <- peer_slot(slots, peer) do
      ref = :persistent_term.get({__MODULE__, :peer_counters})
      :counters.add(ref, slot * 2 + if(kind == :sent, do: 1, else: 2), n)
    end

    :ok
  end

  defp peer_slot(slots, peer) do
    case slots do
      %{^peer => slot} -> {:ok, slot}
      _ -> GenServer.call(__MODULE__, {:peer_slot, peer})
    end
  catch
    :exit, _ -> :error
  end

  defp ref, do: :persistent_term.get(@ref_key, nil)

  defp frontend_base(frontend), do: Map.get(@frontend_base, frontend, 0)
  defp coordinator_base, do: @coordinator_base
  defp shard_base, do: @shard_base

  defp size, do: shard_base() + (Config.shards() + 1) * 2

  # ------------------------------------------------------------- reading

  @doc "This node's current picture: rates over the last second, plus history."
  def snapshot do
    GenServer.call(__MODULE__, :snapshot, 2_000)
  end

  @doc """
  Every member's snapshot, asked in parallel. A member that does not answer
  is in the list as down, with no numbers.
  """
  def cluster do
    members = Kurwa.Cluster.members()

    members
    |> Task.async_stream(
      fn node ->
        if node == node(),
          do: snapshot(),
          else: :erpc.call(node, __MODULE__, :snapshot, [], 2_000)
      end,
      timeout: 3_000,
      on_timeout: :kill_task
    )
    |> Enum.zip(members)
    |> Enum.map(fn
      {{:ok, snapshot}, _node} -> snapshot
      {_, node} -> %{node: to_string(node), up: false}
    end)
  catch
    _, _ -> [%{node: to_string(node()), up: false}]
  end

  # --------------------------------------------------------------- sampler

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    ref = :counters.new(size(), [:write_concurrency])
    :persistent_term.put(@ref_key, ref)

    :persistent_term.put(
      {__MODULE__, :peer_counters},
      :counters.new(@peer_slots * 2, [:write_concurrency])
    )

    :persistent_term.put(@peers_ref, %{})
    :ets.new(@clients, [:set, :public, :named_table, write_concurrency: true])
    :erlang.system_flag(:scheduler_wall_time, true)
    Process.send_after(self(), :sample, 1_000)

    {:ok,
     %{
       ref: ref,
       previous: read_all(ref),
       previous_at: System.monotonic_time(),
       wall: :erlang.statistics(:scheduler_wall_time),
       current: %{},
       history: %{rps: [], util: [], memory: [], replica: []},
       started: System.system_time(:second)
     }}
  end

  @impl true
  def handle_info(:sample, state) do
    Process.send_after(self(), :sample, 1_000)
    now = System.monotonic_time()
    counts = read_all(state.ref)

    seconds =
      max(System.convert_time_unit(now - state.previous_at, :native, :microsecond), 1) / 1.0e6

    wall = :erlang.statistics(:scheduler_wall_time)

    current =
      rates(state.previous, counts, seconds) |> Map.put(:util, utilization(state.wall, wall))

    total_rps = current.frontends |> Map.values() |> Enum.map(& &1.rps) |> Enum.sum()
    replica = current.local_replica + (current.received |> Map.values() |> Enum.sum())

    history = %{
      rps: push(state.history.rps, round1(total_rps)),
      util: push(state.history.util, current.util),
      memory: push(state.history.memory, :erlang.memory(:total)),
      replica: push(state.history.replica, round1(replica))
    }

    {:noreply,
     %{state | previous: counts, previous_at: now, wall: wall, current: current, history: history}}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    :ets.delete(@clients, pid)
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def handle_call(:snapshot, _from, state), do: {:reply, build(state), state}

  def handle_call({:peer_slot, peer}, _from, state) do
    slots = :persistent_term.get(@peers_ref)

    case slots do
      %{^peer => slot} ->
        {:reply, {:ok, slot}, state}

      _ when map_size(slots) >= @peer_slots ->
        {:reply, :error, state}

      _ ->
        slot = map_size(slots)
        :persistent_term.put(@peers_ref, Map.put(slots, peer, slot))
        {:reply, {:ok, slot}, state}
    end
  end

  @impl true
  def handle_cast({:monitor, pid}, state) do
    Process.monitor(pid)
    {:noreply, state}
  end

  defp push(list, value), do: Enum.take(list ++ [value], -@history)

  defp read_all(ref) do
    counters = for i <- 1..size(), into: %{}, do: {i, :counters.get(ref, i)}
    ref = :persistent_term.get({__MODULE__, :peer_counters})

    peers =
      for {peer, slot} <- :persistent_term.get(@peers_ref, %{}),
          {kind, offset} <- [sent: 1, received: 2],
          into: %{},
          do: {{kind, peer}, :counters.get(ref, slot * 2 + offset)}

    %{counters: counters, peers: peers}
  end

  defp rates(previous, now, seconds) do
    delta = fn i -> Map.get(now.counters, i, 0) - Map.get(previous.counters, i, 0) end
    per_second = fn i -> round1(delta.(i) / seconds) end

    frontends =
      for {frontend, n} <- Enum.with_index(@frontends), into: %{} do
        base = n * @per_frontend
        requests = delta.(base + 1)
        time = System.convert_time_unit(delta.(base + 3), :native, :microsecond)

        {frontend,
         %{
           rps: per_second.(base + 1),
           errors: per_second.(base + 2),
           latency_ms: if(requests > 0, do: Float.round(time / requests / 1000, 3), else: 0.0),
           total: Map.get(now.counters, base + 1, 0)
         }}
      end

    peer_rate = fn kind ->
      for {{^kind, peer}, count} <- now.peers, into: %{} do
        {to_string(peer), round1((count - Map.get(previous.peers, {kind, peer}, 0)) / seconds)}
      end
    end

    shards =
      for index <- 0..Config.shards() do
        base = shard_base() + index * 2
        %{index: index, gets: per_second.(base + 1), puts: per_second.(base + 2)}
      end

    %{
      frontends: frontends,
      reads: per_second.(coordinator_base() + 1),
      writes: per_second.(coordinator_base() + 2),
      local_replica: per_second.(coordinator_base() + length(@coordinator) + 1),
      sent: peer_rate.(:sent),
      received: peer_rate.(:received),
      shards: shards
    }
  end

  # Busy share of scheduler time over the last second, 0..100.
  defp utilization(before, now) when is_list(before) and is_list(now) do
    {active, total} =
      Enum.zip(Enum.sort(before), Enum.sort(now))
      |> Enum.reduce({0, 0}, fn {{_, a0, t0}, {_, a1, t1}}, {a, t} ->
        {a + a1 - a0, t + t1 - t0}
      end)

    if total > 0, do: Float.round(active * 100 / total, 1), else: 0.0
  end

  defp utilization(_, _), do: 0.0

  defp round1(x), do: Float.round(x * 1.0, 1)

  defp build(state) do
    current = state.current
    memory = :erlang.memory()

    %{
      node: to_string(node()),
      up: true,
      uptime: System.system_time(:second) - state.started,
      engine: Config.engine() |> inspect() |> String.replace_prefix("Kurwa.Store.", ""),
      n: Config.n(),
      r: Config.r(),
      w: Config.w(),
      lamport: Kurwa.Clock.peek(),
      keys: Kurwa.Store.count(),
      sets: length(Kurwa.Registry.local()),
      frontends: frontends(Map.get(current, :frontends, %{})),
      reads: Map.get(current, :reads, 0.0),
      writes: Map.get(current, :writes, 0.0),
      local_replica: Map.get(current, :local_replica, 0.0),
      sent: Map.get(current, :sent, %{}),
      received: Map.get(current, :received, %{}),
      shards: shards(Map.get(current, :shards, [])),
      cache: Kurwa.Extractor.Cache.stats() |> Map.take([:enabled, :size, :hits, :misses]),
      handoff: handoff(),
      system: %{
        util: Map.get(current, :util, 0.0),
        schedulers: :erlang.system_info(:schedulers_online),
        run_queue: :erlang.statistics(:run_queue),
        processes: :erlang.system_info(:process_count),
        memory: memory[:total],
        memory_processes: memory[:processes],
        memory_ets: memory[:ets],
        memory_binary: memory[:binary]
      },
      clients: clients(),
      history: state.history
    }
  end

  defp clients do
    @clients
    |> :ets.tab2list()
    |> Enum.sort_by(fn {_, frontend, _, since, _, _, _} -> {frontend, since} end)
    |> Enum.take(200)
    |> Enum.map(fn {_pid, frontend, peer, since, user, app, requests} ->
      %{frontend: frontend, peer: peer, since: since, user: user, app: app, requests: requests}
    end)
  end

  defp shards(rates) do
    for %{index: index} = shard <- rates do
      keys =
        if index < Config.shards() do
          try do
            Kurwa.Store.count(index)
          rescue
            _ -> 0
          end
        else
          length(Kurwa.Registry.local_names())
        end

      Map.merge(shard, %{keys: keys, system: index == Config.shards()})
    end
  end

  # Configured frontends, with their port and open connections.
  defp frontends(rates) do
    listeners = listeners()

    for frontend <- @frontends, port = port(frontend), port != nil do
      connections =
        case Map.get(listeners, frontend) do
          nil -> 0
          pid -> count_connections(pid)
        end

      Map.merge(
        %{
          name: frontend,
          port: port,
          public: public(frontend, port),
          connections: connections,
          rps: 0.0,
          errors: 0.0,
          latency_ms: 0.0,
          total: 0
        },
        Map.get(rates, frontend, %{})
      )
    end
  end

  # host:port a client outside uses to reach this frontend on this node.
  defp public(frontend, _port) do
    ports = Config.get(:public_ports) || %{}
    host = Config.get(:public_host)

    case Map.get(ports, to_string(frontend)) do
      nil -> nil
      public_port when is_binary(host) -> "#{host}:#{public_port}"
      public_port -> ":#{public_port}"
    end
  end

  defp count_connections(pid) do
    case ThousandIsland.connection_pids(pid) do
      {:ok, pids} -> length(pids)
      _ -> 0
    end
  catch
    _, _ -> 0
  end

  defp port(:http), do: if(Config.get(:start_gateway), do: Config.get(:http_port))
  defp port(:pg), do: if(Config.get(:start_pg), do: Config.get(:pg_port))
  defp port(:resp), do: if(Config.get(:start_resp), do: Config.get(:resp_port))
  defp port(:mysql), do: if(Config.get(:start_mysql), do: Config.get(:mysql_port))
  defp port(:mongo), do: if(Config.get(:start_mongo), do: Config.get(:mongo_port))
  defp port(:mssql), do: if(Config.get(:start_mssql), do: Config.get(:mssql_port))
  defp port(:ninep), do: if(Config.get(:start_9p), do: Config.get(:ninep_port))

  @ids %{
    kurwa_pg: :pg,
    kurwa_resp: :resp,
    kurwa_mysql: :mysql,
    kurwa_mongo: :mongo,
    kurwa_mssql: :mssql,
    kurwa_9p: :ninep
  }

  defp listeners do
    Kurwa.Supervisor
    |> Supervisor.which_children()
    |> Enum.flat_map(fn
      {id, pid, _, _} when is_pid(pid) and is_map_key(@ids, id) -> [{@ids[id], pid}]
      {{ThousandIsland, _}, pid, _, _} when is_pid(pid) -> [{:http, pid}]
      {{Bandit, _}, pid, _, _} when is_pid(pid) -> [{:http, pid}]
      {Bandit, pid, _, _} when is_pid(pid) -> [{:http, pid}]
      {ThousandIsland, pid, _, _} when is_pid(pid) -> [{:http, pid}]
      _ -> []
    end)
    |> Map.new()
  catch
    _, _ -> %{}
  end

  defp handoff do
    case Kurwa.Handoff.depth() do
      depth when is_map(depth) -> depth |> Map.values() |> Enum.sum()
      depth when is_integer(depth) -> depth
      _ -> 0
    end
  catch
    _, _ -> 0
  end
end
