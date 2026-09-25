defmodule Kurwa.Quorum do
  @moduledoc """
  Fan out a call to N replicas, return as soon as `need` of them answer.

  The fan-out runs inside a short-lived collector process, not in the caller.
  That matters: once the quorum is reached we walk away from the slow replicas,
  and their late replies must not pile up in the mailbox of a long-lived process
  such as a gateway connection. They land in the collector's mailbox and die
  with it.

  `fun` is called with a node and must return `{:ok, value}` or `{:error, reason}`.
  """

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
