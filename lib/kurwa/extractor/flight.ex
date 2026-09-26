defmodule Kurwa.Extractor.Flight do
  @moduledoc """
  Single-flight: while one process is resolving a key, everyone else asking for
  the same key waits for that answer instead of starting their own.

  This is the part of the read path that protects the cluster from a hot key. A
  thousand concurrent checks of the same cold key become one quorum read and 999
  processes parked on a deferred `GenServer.reply/2`, not a thousand fan-outs.

  The leader is monitored, so a leader that dies mid-flight hands its waiters an
  error instead of leaving them parked until their call times out.
  """

  use GenServer

  @name __MODULE__

  @type role :: :lead | :joined

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: @name)

  @doc """
  Resolves `key` through `fun`, or waits for whoever is already resolving it.

  Returns `{:lead, result}` for the process that actually ran `fun` and
  `{:joined, result}` for everyone who waited, so the caller can tell a real
  backend round trip from a coalesced one.
  """
  @spec fetch(term(), (-> result), timeout()) :: {role(), result} when result: term()
  def fetch(key, fun, timeout \\ 5_000) when is_function(fun, 0) do
    case GenServer.call(@name, {:join, key}, timeout) do
      :lead ->
        try do
          result = fun.()
          GenServer.cast(@name, {:settle, key, result})
          {:lead, result}
        catch
          kind, reason ->
            GenServer.cast(@name, {:settle, key, {:error, {kind, reason}}})
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      {:joined, result} ->
        {:joined, result}
    end
  end

  @doc "How many keys are in flight right now."
  def in_flight, do: GenServer.call(@name, :in_flight)

  @impl true
  def init(_opts), do: {:ok, %{flights: %{}, monitors: %{}}}

  @impl true
  def handle_call({:join, key}, {pid, _tag} = from, state) do
    case Map.fetch(state.flights, key) do
      :error ->
        monitor = Process.monitor(pid)

        state = %{
          state
          | flights: Map.put(state.flights, key, %{leader: monitor, waiters: []}),
            monitors: Map.put(state.monitors, monitor, key)
        }

        {:reply, :lead, state}

      {:ok, flight} ->
        flight = %{flight | waiters: [from | flight.waiters]}
        {:noreply, %{state | flights: Map.put(state.flights, key, flight)}}
    end
  end

  @impl true
  def handle_call(:in_flight, _from, state), do: {:reply, map_size(state.flights), state}

  @impl true
  def handle_cast({:settle, key, result}, state) do
    {:noreply, finish(state, key, {:joined, result})}
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Map.fetch(state.monitors, monitor) do
      {:ok, key} -> {:noreply, finish(state, key, {:joined, {:error, {:leader_down, reason}}})}
      :error -> {:noreply, state}
    end
  end

  defp finish(state, key, reply) do
    case Map.pop(state.flights, key) do
      {nil, _flights} ->
        state

      {flight, flights} ->
        Process.demonitor(flight.leader, [:flush])
        Enum.each(flight.waiters, &GenServer.reply(&1, reply))

        %{state | flights: flights, monitors: Map.delete(state.monitors, flight.leader)}
    end
  end
end
