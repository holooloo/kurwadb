defmodule Kurwa.Replica.Endpoint do
  @moduledoc """
  Long-lived processes that answer replica reads and writes from other nodes.

  The hot path used to reach a peer through `:erpc.call`, which spawns a process
  on the remote node for every call. On three nodes on one machine that was
  10-13 µs of a 52 µs quorum read, on top of a 30 µs distribution round trip
  that no code here can shorten. A message to a process that already exists
  costs the round trip and nothing else.

  There are #{16} endpoints per node, registered by name, and a request picks
  one by hashing its key - so a fixed count on every node, not one per
  scheduler, because the sender has to name a process on a machine it cannot
  see. Each endpoint runs the request through `Kurwa.Replica`, which is still
  the whole surface one node exposes to another, and sends the answer to the
  alias the request carried.
  """

  alias Kurwa.Record
  alias Kurwa.Replica

  @count 16

  @doc false
  def child_spec(_opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, []}, type: :supervisor}
  end

  @doc "Starts the endpoints under a supervisor."
  def start_link do
    children =
      for i <- 0..(@count - 1) do
        %{id: {:endpoint, i}, start: {__MODULE__, :start_endpoint, [i]}}
      end

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  @doc false
  def start_endpoint(i) do
    pid = spawn_link(fn -> loop() end)
    Process.register(pid, name(i))
    {:ok, pid}
  end

  @doc "The registered name, on any node, that serves `request`."
  @spec name_for(Replica.request() | term()) :: atom()
  def name_for({:get, key}), do: name(:erlang.phash2(key, @count))
  def name_for({:put, record}), do: name(:erlang.phash2(Record.key(record), @count))

  def name_for({:put_new, record, _previous}),
    do: name(:erlang.phash2(Record.key(record), @count))

  # Anything else is answered with an error by Kurwa.Replica.handle/1 - it still
  # needs somewhere to go, so a malformed request is a reply and not a crash.
  def name_for(_other), do: name(0)

  defp loop do
    receive do
      {:kurwa_replica, {pid, ref}, request} ->
        send(pid, {ref, node(), answer(request)})
    end

    loop()
  end

  defp answer(request) do
    Replica.handle(request)
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # Precomputed: a request names its endpoint on every call, and building the
  # atom each time would cost more than the hash.
  for i <- 0..(@count - 1) do
    defp name(unquote(i)), do: unquote(:"kurwa_replica_endpoint_#{i}")
  end
end
