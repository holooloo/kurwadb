defmodule Kurwa.Mongo.Server do
  @moduledoc """
  A MongoDB server, one process per connection, so `mongosh` and the MongoDB
  drivers talk to kurwadb as they are. Framing is `Kurwa.Mongo.Wire`, BSON is
  `Kurwa.Mongo.Bson`, and what the commands mean is `Kurwa.Mongo.Commands`.

  Replies are collected and written once the input in hand is handled, as in
  the other frontends. A message sent with `moreToCome` - an unacknowledged
  write - gets no reply, which is what the client asked for.
  """

  use ThousandIsland.Handler

  alias Kurwa.Mongo.{Commands, Wire}

  require Logger

  @impl ThousandIsland.Handler
  def handle_connection(_socket, _state) do
    id = System.unique_integer([:positive]) |> rem(2_000_000_000)
    {:continue, %{buffer: <<>>, session: Commands.session(id)}}
  end

  @impl ThousandIsland.Handler
  def handle_data(data, socket, state) do
    {result, out} = loop(state.buffer <> data, %{state | buffer: <<>>}, [])
    if out != [], do: ThousandIsland.Socket.send(socket, out)
    result
  end

  defp run(command, session) do
    Kurwa.Metrics.measure(:mongo, fn ->
      {reply, _} = result = Commands.run(command, session)

      if match?({:doc, [{"ok", 0} | _]}, reply) or match?({:doc, [{"ok", +0.0} | _]}, reply),
        do: Kurwa.Metrics.error(:mongo)

      result
    end)
  end

  defp loop(buffer, state, out) do
    case Wire.decode(buffer) do
      {:ok, {:msg, request_id, command, more_to_come?}, rest} ->
        {reply, session} = run(command, state.session)
        out = if more_to_come?, do: out, else: [out, Wire.reply_msg(request_id, reply)]
        loop(rest, %{state | session: session}, out)

      # Legacy OP_QUERY: only commands against <db>.$cmd, which is how drivers
      # still send their first hello. The database is in the namespace.
      {:ok, {:query, request_id, collection, {:doc, pairs}}, rest} ->
        db = collection |> String.split(".", parts: 2) |> hd()
        {reply, session} = run({:doc, pairs ++ [{"$db", db}]}, state.session)
        loop(rest, %{state | session: session}, [out, Wire.reply_query(request_id, reply)])

      {:ok, {:unsupported, request_id, opcode}, rest} ->
        Logger.warning("kurwadb mongo: unsupported opcode #{opcode}")

        error =
          {:doc,
           [
             {"ok", 0.0},
             {"errmsg", "opcode #{opcode} is not supported"},
             {"code", 352},
             {"codeName", "UnsupportedOpQueryCommand"}
           ]}

        loop(rest, state, [out, Wire.reply_msg(request_id, error)])

      :more ->
        {{:continue, %{state | buffer: buffer}}, out}

      {:error, reason} ->
        Logger.warning("kurwadb mongo: dropping connection: #{inspect(reason)}")
        {{:close, state}, out}
    end
  end
end
