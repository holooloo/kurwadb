# What the MongoDB frontend costs on the server, per findOne: decoding the
# OP_MSG, running the command, encoding the reply - with no client in the way.
#
#     KURWA_N=1 KURWA_R=1 KURWA_W=1 mix run bench/mongo_server_cost.exs

alias Kurwa.Mongo.{Bson, Commands, Wire}
Kurwa.add("1")

cmd =
  {:doc,
   [
     {"find", "kurwa"},
     {"filter", {:doc, [{"_id", "1"}]}},
     {"limit", 1},
     {"singleBatch", true},
     {"lsid", {:doc, [{"id", {:binary, 4, :crypto.strong_rand_bytes(16)}}]}},
     {"$db", "kurwadb"}
   ]}

body = Bson.encode(cmd)
msg = <<0::32-little, 0, body::binary>>
packet = <<byte_size(msg) + 16::32-little, 7::32-little, 0::32, 2013::32-little, msg::binary>>
s = Commands.session(1)
n = 50_000

run = fn ->
  {:ok, {:msg, id, c, _}, _} = Wire.decode(packet)
  {reply, _} = Commands.run(c, s)
  Wire.reply_msg(id, reply)
end

for _ <- 1..5000, do: run.()
{us, _} = :timer.tc(fn -> for _ <- 1..n, do: run.() end)
IO.puts("decode + find + encode: #{Float.round(us / n, 2)} us")
{us2, _} = :timer.tc(fn -> for _ <- 1..n, do: Kurwa.fetch("1") end)
IO.puts("  of which Kurwa.fetch: #{Float.round(us2 / n, 2)} us")
