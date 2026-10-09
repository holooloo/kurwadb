# Writes clients/ring_fixture.json: preference lists computed by the server's
# own Kurwa.Ring and Kurwa.Key, for the clients' tests to reproduce bit for
# bit. Regenerate after any change to the ring or the key encoding:
#
#     mix run --no-start clients/ring_fixture.exs

alias Kurwa.{Key, Ring}

nodes = [:"kurwadb@kurwadb", :"kurwadb@kurwadb2", :"kurwadb@kurwadb3", :"kurwa4@10.0.0.4", :"x@y"]

keys =
  for(i <- 0..149, do: "key:#{i}") ++
    ["", "a", "order:1029", "дом", "ключ:😀", "user:42", "with space", "a/b", "seen:orders:1"]

cases =
  for size <- [1, 3, 5],
      members = Enum.take(nodes, size),
      ring = Ring.new(members, 128),
      n <- [1, 3],
      set <- ["", "seen", "analytics.events"],
      key <- (if size == 3 and n == 3, do: keys, else: Enum.take(keys, 30)) do
    storage = if set == "", do: Key.encode(nil, key), else: Key.encode(set, key)

    %{
      members: Enum.map(members, &to_string/1),
      vnodes: 128,
      n: n,
      set: set,
      key: key,
      preflist: ring |> Ring.preflist(storage, n) |> Enum.map(&to_string/1)
    }
  end

path = Path.join(__DIR__, "ring_fixture.json")
File.write!(path, Jason.encode!(%{cases: cases}))
IO.puts("#{length(cases)} cases -> #{path}")
