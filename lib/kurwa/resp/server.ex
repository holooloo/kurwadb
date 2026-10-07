defmodule Kurwa.Resp.Server do
  @moduledoc """
  A Redis protocol server, one process per connection, so `redis-cli`,
  `redis-benchmark` and Redis client libraries talk to kurwadb as they are.

  Requests are answered strictly in order, as Redis does, and a pipeline costs
  one write: replies are collected while the input in hand is handled and sent
  together, the same thing that doubled the PostgreSQL frontend's prepared
  statement rate. What the commands mean is `Kurwa.Resp.Commands`.

  A client frontend only; nodes replicate over Erlang distribution.
  """

  use ThousandIsland.Handler

  alias Kurwa.Resp.{Commands, Proto}

  require Logger

  @impl ThousandIsland.Handler
  def handle_connection(_socket, _state),
    do: {:continue, %{buffer: <<>>, session: Commands.session()}}

  @impl ThousandIsland.Handler
  def handle_data(data, socket, state) do
    {result, out} = loop(state.buffer <> data, %{state | buffer: <<>>}, [])
    if out != [], do: ThousandIsland.Socket.send(socket, out)
    result
  end

  defp measure_resp(request, session) do
    Kurwa.Metrics.measure(:resp, fn ->
      result = Commands.run(request, session)
      if match?({{:error, _}, _}, result), do: Kurwa.Metrics.error(:resp)
      result
    end)
  end

  defp loop(buffer, state, out) do
    case Proto.decode(buffer) do
      {:ok, [], rest} ->
        loop(rest, state, out)

      {:ok, request, rest} ->
        case measure_resp(request, state.session) do
          {:close, reply, _session} ->
            {{:close, state}, [out, Proto.encode(reply, state.session.proto)]}

          {reply, session} ->
            # Encoded in the version that was in force when the command ran:
            # the reply to HELLO 3 is itself RESP3.
            loop(rest, %{state | session: session}, [out, Proto.encode(reply, session.proto)])
        end

      :more ->
        {{:continue, %{state | buffer: buffer}}, out}

      {:error, reason} ->
        Logger.warning("kurwadb resp: dropping connection: #{reason}")
        {{:close, state}, [out, Proto.encode({:error, "ERR Protocol error: #{reason}"}, 2)]}
    end
  end
end
