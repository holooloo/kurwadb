defmodule Kurwa.TestCluster do
  @moduledoc """
  Spins up real kurwadb nodes for the cluster tests.

  The orchestrating VM deliberately stays undistributed: peers talk to the test
  process over a `standard_io` control channel, and only to each other over
  Erlang distribution. Otherwise the suite's own kurwadb app would answer the
  peers' `ping` and join the cluster under test as an extra member, with a
  different quorum config.
  """

  @cookie ~c"kurwa_cluster_test"
  @host ~c"127.0.0.1"

  @type peer :: %{pid: pid(), node: node()}

  @doc """
  Starts `count` nodes, seeded at each other, and waits for the ring to converge.

  `overrides` are merged into each node's application environment before the app
  starts, so a test can dial down timers it is going to wait on.
  """
  @spec start(pos_integer(), Path.t(), keyword()) :: [peer()]
  def start(count, data_dir, overrides \\ []) do
    names = for i <- 1..count, do: :"kurwa_node#{i}"
    nodes = Enum.map(names, &:"#{&1}@#{@host}")

    peers = Enum.map(names, &boot(&1, nodes, data_dir, overrides))

    :ok = await_ring(peers, count)
    peers
  end

  @doc "Starts one node by name, e.g. to bring a stopped one back with its data intact."
  @spec boot(atom(), [node()], Path.t(), keyword()) :: peer()
  def boot(name, seeds, data_dir, overrides \\ []) do
    {:ok, pid, node} =
      :peer.start_link(%{
        name: name,
        host: @host,
        longnames: true,
        connection: :standard_io,
        args: [~c"-setcookie", @cookie]
      })

    :peer.call(pid, :code, :add_paths, [:code.get_path()])
    # Before the app starts, or the boot lines come back over the control channel
    # and bury the test output.
    :peer.call(pid, Application, :put_env, [:logger, :level, :error])

    env =
      Keyword.merge(
        [
          n: 3,
          r: 2,
          w: 2,
          shards: 2,
          data_dir: data_dir,
          start_gateway: false,
          start_9p: false,
          cache: false,
          seeds: seeds,
          seed_retry_interval: 300,
          handoff_interval: 300,
          wal_sync_interval: 50,
          request_timeout: 2_000
        ],
        overrides
      )

    for {key, value} <- env do
      :peer.call(pid, Application, :put_env, [:kurwadb, key, value])
    end

    {:ok, _} = :peer.call(pid, Application, :ensure_all_started, [:kurwadb])

    %{pid: pid, node: node}
  end

  @doc """
  Stops a node, leaving its data directory alone so it can be booted again.

  `:graceful` shuts the application down first, which flushes the WAL the way an
  orderly restart would. `:kill` goes straight for the node, which is what a
  crash looks like: anything the WAL had not fsynced yet is gone.
  """
  @spec stop(peer(), :graceful | :kill) :: :ok
  def stop(peer, mode \\ :graceful)

  def stop(%{pid: pid} = peer, :graceful) do
    _ = call(peer, Application, :stop, [:kurwadb])
    :peer.stop(pid)
    :ok
  catch
    _kind, _reason -> :ok
  end

  def stop(%{pid: pid}, :kill) do
    :peer.stop(pid)
    :ok
  catch
    _kind, _reason -> :ok
  end

  @doc "Runs `{module, function, args}` on a node."
  def call(%{pid: pid}, module, function, args), do: :peer.call(pid, module, function, args)

  @doc "`Kurwa.info/0` as that node sees it."
  def info(peer), do: call(peer, Kurwa, :info, [])

  @doc "Live keys held locally by that node."
  def local_keys(peer), do: call(peer, Kurwa.Store, :count, [])

  @doc "Waits for `fun` to return a truthy value, polling every 100ms."
  @spec await((-> any()), timeout()) :: any()
  def await(fun, timeout \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    poll(fun, deadline)
  end

  defp poll(fun, deadline) do
    case fun.() do
      falsy when falsy in [false, nil] ->
        if System.monotonic_time(:millisecond) >= deadline do
          raise "kurwadb test cluster: condition never became true"
        else
          Process.sleep(100)
          poll(fun, deadline)
        end

      truthy ->
        truthy
    end
  end

  @doc "Waits until every node sees `count` members, all of them reachable."
  def await_ring(peers, count, timeout \\ 15_000) do
    await(
      fn ->
        Enum.all?(peers, fn peer ->
          info = info(peer)
          length(info.members) == count and length(info.up) == count
        end)
      end,
      timeout
    )

    :ok
  end

  @doc "Waits until `peer` sees exactly `up` reachable members out of `members` known."
  def await_reachability(peer, members, up, timeout \\ 15_000) do
    await(
      fn ->
        info = info(peer)
        length(info.members) == members and length(info.up) == up
      end,
      timeout
    )

    :ok
  end
end
