defmodule Kurwa.Quorum do
  @moduledoc """
  Fan out a call to N replicas, return as soon as `need` of them answer.

  The fan-out runs inside a short-lived collector process, not in the caller.
  That matters: once the quorum is reached we walk away from the slow replicas,
  and their late replies must not pile up in the mailbox of a long-lived process
  such as a gateway connection. They land in the collector's mailbox and die
  with it.

  `fun` is called with a node and must return `{:ok, value}` or `{:error, reason}`.

  `request/4` is the hot-path version for reads and writes of one key. It sends
  the request straight to `Kurwa.Replica.Endpoint` on each remote node and runs
  the local one inline, so the only process it starts is the collector:
  sending is asynchronous, and a worker per replica existed only to wait.
  """

  alias Kurwa.Replica
  alias Kurwa.Replica.Endpoint

  @type outcome :: %{ok: [{node(), term()}], failed: [{node(), term()}]}

  @doc "Runs `fun` on every node, waiting for `need` successes or `timeout`."
  @spec run([node()], (node() -> {:ok, term()} | {:error, term()}), pos_integer(), timeout()) ::
          outcome()
  def run(nodes, fun, need, timeout)

  def run([], _fun, _need, _timeout), do: %{ok: [], failed: []}

  def run(nodes, fun, need, timeout)
      when is_list(nodes) and is_function(fun, 1) and is_integer(need) and need > 0 do
    caller = self()
    tag = make_ref()

    {collector, monitor} =
      spawn_monitor(fn -> collect(caller, tag, nodes, fun, need, timeout) end)

    await(tag, collector, monitor, nodes, timeout)
  end

  @doc """
  Sends `request` to every node, waiting for `need` successes or `timeout`.

  Same outcome shape and the same guarantee as `run/4`: late replies land in
  the collector and die with it. A node that drops off while we wait answers
  at once through its monitor, rather than at the deadline.
  """
  @spec request([node()], Replica.request(), pos_integer() | (outcome() -> boolean()), timeout()) ::
          outcome()
  def request(nodes, request, need, timeout)

  def request([], _request, _need, _timeout), do: %{ok: [], failed: []}

  # The only replica is this node: there is nobody to wait for and no late reply
  # that could land in the caller's mailbox, so the collector would be a process
  # started to do nothing. A single node, and n=1, answer inline.
  def request([only], request, need, _timeout) when only == node() and is_integer(need) do
    case invoke(fn _ -> Replica.handle(request) end, only) do
      {:ok, value} -> %{ok: [{only, value}], failed: []}
      {:error, reason} -> %{ok: [], failed: [{only, reason}]}
    end
  end

  # `need` is a count of successes, or a function of the outcome so far that
  # says when the answer is settled - a conditional write is settled by a
  # majority either way, not by the first `w` replies.
  def request(nodes, request, need, timeout)
      when is_list(nodes) and ((is_integer(need) and need > 0) or is_function(need, 1)) do
    caller = self()
    tag = make_ref()

    {collector, monitor} =
      spawn_monitor(fn -> send(caller, {tag, gather(nodes, request, need, timeout)}) end)

    await(tag, collector, monitor, nodes, timeout)
  end

  defp await(tag, collector, monitor, nodes, timeout) do
    receive do
      {^tag, outcome} ->
        Process.demonitor(monitor, [:flush])
        outcome

      {:DOWN, ^monitor, :process, ^collector, reason} ->
        %{ok: [], failed: Enum.map(nodes, &{&1, {:collector_down, reason}})}
    after
      timeout + 250 ->
        Process.exit(collector, :kill)
        Process.demonitor(monitor, [:flush])
        drain(tag)
        %{ok: [], failed: Enum.map(nodes, &{&1, :timeout})}
    end
  end

  defp gather(nodes, request, need, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    name = Endpoint.name_for(request)
    me = node()

    # Remote first, so they are on the wire while the local replica answers.
    #
    # No process monitor on the endpoint: monitoring a remote name is a signal
    # over the wire to set it up and another to take it down, and measured that
    # made the quorum slower than :erpc. monitor_node/2 is bookkeeping in the
    # local distribution layer and sends nothing, and it is what turns a node
    # dropping mid-request into an answer now rather than at the deadline.
    remote = Enum.reject(nodes, &(&1 == me))

    # A node that is not distributed cannot reach anyone, and monitor_node/2
    # raises rather than say so.
    {pending, unreachable} =
      if Node.alive?() do
        pending =
          Map.new(remote, fn node ->
            true = :erlang.monitor_node(node, true)
            ref = make_ref()
            send({name, node}, {:kurwa_replica, {self(), ref}, request})
            {ref, node}
          end)

        {pending, []}
      else
        {%{}, Enum.map(remote, &{&1, {:no_reply, :noconnection}})}
      end

    {ok, failed} =
      if me in nodes do
        case invoke(fn _ -> Replica.handle(request) end, me) do
          {:ok, value} -> {[{me, value}], []}
          {:error, reason} -> {[], [{me, reason}]}
        end
      else
        {[], []}
      end

    replies(pending, need, deadline, ok, unreachable ++ failed)
  end

  defp replies(pending, need, deadline, ok, failed) do
    if settled?(need, ok, failed) or map_size(pending) == 0 do
      %{ok: ok, failed: failed}
    else
      wait = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {ref, node, {:ok, value}} when is_map_key(pending, ref) ->
          replies(Map.delete(pending, ref), need, deadline, [{node, value} | ok], failed)

        {ref, node, {:error, reason}} when is_map_key(pending, ref) ->
          replies(Map.delete(pending, ref), need, deadline, ok, [{node, reason} | failed])

        {:nodedown, down} ->
          {gone, rest} = Map.split_with(pending, fn {_ref, node} -> node == down end)
          lost = Enum.map(gone, fn {_ref, node} -> {node, {:no_reply, :noconnection}} end)
          replies(rest, need, deadline, ok, lost ++ failed)
      after
        wait ->
          %{ok: ok, failed: failed ++ Enum.map(Map.values(pending), &{&1, :timeout})}
      end
    end
  end

  defp settled?(need, ok, _failed) when is_integer(need), do: length(ok) >= need
  defp settled?(done?, ok, failed), do: done?.(%{ok: ok, failed: failed})

  defp drain(tag) do
    receive do
      {^tag, _} -> :ok
    after
      0 -> :ok
    end
  end

  defp collect(caller, tag, nodes, fun, need, timeout) do
    me = self()

    workers =
      Map.new(nodes, fn node ->
        {pid, _ref} =
          spawn_monitor(fn -> send(me, {:result, self(), node, invoke(fun, node)}) end)

        {pid, node}
      end)

    deadline = System.monotonic_time(:millisecond) + timeout
    send(caller, {tag, loop(workers, need, deadline, [], [])})
  end

  defp loop(workers, need, deadline, ok, failed) do
    if length(ok) >= need or map_size(workers) == 0 do
      %{ok: ok, failed: failed}
    else
      wait = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:result, pid, node, {:ok, value}} ->
          loop(Map.delete(workers, pid), need, deadline, [{node, value} | ok], failed)

        {:result, pid, node, {:error, reason}} ->
          loop(Map.delete(workers, pid), need, deadline, ok, [{node, reason} | failed])

        {:DOWN, _ref, :process, pid, reason} ->
          case Map.pop(workers, pid) do
            # already accounted for by its :result message
            {nil, _} ->
              loop(workers, need, deadline, ok, failed)

            {node, rest} ->
              loop(rest, need, deadline, ok, [{node, {:no_reply, reason}} | failed])
          end
      after
        wait ->
          %{ok: ok, failed: failed ++ Enum.map(Map.values(workers), &{&1, :timeout})}
      end
    end
  end

  defp invoke(fun, node) do
    case fun.(node) do
      {:ok, value} -> {:ok, value}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_return, other}}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end
end
