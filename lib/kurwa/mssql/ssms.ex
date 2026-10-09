defmodule Kurwa.Mssql.Ssms do
  @moduledoc """
  The catalog queries SQL Server Management Studio sends, answered the way a
  real SQL Server answers them.

  SSMS does not ask a server what it is in a few well-known queries: it runs
  SMO, which sends dozens of generated batches - joins across `sys.*` views,
  temporary tables, registry reads, dynamic SQL - and reads every column of
  every answer by type. An interpreter for that is most of SQL Server. What
  is here instead is the answers: `priv/mssql/ssms.json`, built by
  `deploy/ssms_fixture.py` from a session recorded against a real server
  (`deploy/tds_record.py`), keyed by each request's normalised text and kept
  with the exact column types the server sent.

  Most answers are about the server and are replayed as recorded, with the
  server's name replaced by this node's. The ones about what is in it are
  made from kurwadb's own state, by the template's `role`:

    * `databases` - the system databases, plus `kurwadb`;
    * `database_by_name` - one database, by the name in `@_msparam_0`;
    * `tables` - in `kurwadb`, every set (`dbo` unless its name has a schema);
    * `schemas` - the built-in schemas plus kurwadb's.

  kurwadb answers as the recorded server's engine and version, because SMO
  chooses its queries by version: claiming another one would bring queries
  no recording has.
  """

  alias Kurwa.Mssql.Tds

  @path Path.expand("../../../priv/mssql/ssms.json", __DIR__)
  @external_resource @path
  @templates (case File.read(@path) do
                {:ok, json} -> json |> Jason.decode!() |> Map.new(&{&1["fp"], &1})
                {:error, _} -> %{}
              end)

  # Every request reaches answer/3, and normalising it costs microseconds; the
  # first few normalised characters rule nearly every request out first.
  @prefix 10
  @prefixes @templates
            |> Map.keys()
            |> MapSet.new(&binary_part(&1, 0, min(@prefix, byte_size(&1))))

  # The recorded server's own name, replaced by this node's in every answer.
  @recorded_server "5a93a07099c2"
  @system_databases ~w(master tempdb model msdb)
  @database "kurwadb"

  @doc "The databases a client can USE."
  def databases, do: @system_databases ++ [@database]

  @doc "Lowercase, whitespace collapsed, no trailing semicolon: how requests are keyed."
  def normalise(sql), do: sql |> squeeze(<<>>, true) |> drop_tail()

  # One pass: ASCII lowercased, every run of whitespace one space, none at
  # the start. Regexes would do it in three, and in OTP 28+ a regex literal is
  # rebuilt on every call - microseconds on every request.
  defp squeeze(<<c, rest::binary>>, acc, _space?) when c in [?\s, ?\t, ?\r, ?\n, ?\f, ?\v],
    do: squeeze(rest, acc, true)

  defp squeeze(<<c, rest::binary>>, acc, space?) do
    c = if c in ?A..?Z, do: c + 32, else: c
    acc = if space? and acc != <<>>, do: <<acc::binary, ?\s, c>>, else: <<acc::binary, c>>
    squeeze(rest, acc, false)
  end

  defp squeeze(<<>>, acc, _space?), do: acc

  # No trailing semicolons (nor the spaces between them).
  defp drop_tail(s) do
    case :binary.last(s) do
      c when c in [?;, ?\s] -> drop_tail(binary_part(s, 0, byte_size(s) - 1))
      _ -> s
    end
  rescue
    ArgumentError -> s
  end

  @doc """
  The answer to `sql` - a batch, or with `params` an sp_executesql statement -
  as TDS tokens, or `:nomatch`. `database` is the session's current one.
  """
  def answer(sql, params \\ %{}, database) do
    if MapSet.member?(@prefixes, prefix(sql)), do: lookup(sql, params, database), else: :nomatch
  end

  # The normalised start of `sql`, from no more of it than that takes.
  defp prefix(sql) do
    head = binary_part(sql, 0, min(byte_size(sql), 64))
    normalised = normalise(head)
    binary_part(normalised, 0, min(@prefix, byte_size(normalised)))
  end

  defp lookup(sql, params, database) do
    case Map.fetch(@templates, normalise(sql)) do
      {:ok, template} -> {:ok, encode(template, params, database)}
      :error -> :nomatch
    end
  end

  @doc "How many recorded answers there are."
  def count, do: map_size(@templates)

  # ---------------------------------------------------------------- answering

  defp encode(template, params, database) do
    ctx = %{params: params, database: database, server: server_name(), role: template["role"]}
    replay(template["reply"], ctx, nil, 0, [])
  end

  # Walks the recorded tokens, re-making each result set's rows, and fixing
  # the DONE count that follows it.
  defp replay([], _ctx, _columns, _rows, acc), do: Enum.reverse(acc)

  defp replay([%{"t" => "colmetadata", "columns" => columns} | rest], ctx, _, _, acc) do
    {recorded, rest} = Enum.split_while(rest, &(&1["t"] == "row"))
    rows = rows(ctx, columns, Enum.map(recorded, & &1["values"]))
    tokens = [colmetadata(columns) | Enum.map(rows, &row(&1, columns))]
    replay(rest, ctx, columns, length(rows), Enum.reverse(tokens, acc))
  end

  defp replay([%{"t" => kind} = done | rest], ctx, columns, rows, acc)
       when kind in ~w(done doneproc doneinproc) do
    count =
      if columns != nil and Bitwise.band(done["status"], 0x10) != 0, do: rows, else: done["count"]

    byte = %{"done" => 0xFD, "doneproc" => 0xFE, "doneinproc" => 0xFF}[kind]
    token = <<byte, done["status"]::16-little, done["cmd"]::16-little, count::64-little>>
    replay(rest, ctx, nil, 0, [token | acc])
  end

  defp replay([%{"t" => "returnstatus", "value" => v} | rest], ctx, c, r, acc),
    do: replay(rest, ctx, c, r, [Tds.return_status(v) | acc])

  defp replay([%{"t" => "info"} = info | rest], ctx, c, r, acc),
    do:
      replay(rest, ctx, c, r, [
        Tds.notice(:info, info["number"], info["class"], info["message"]) | acc
      ])

  defp replay([_other | rest], ctx, c, r, acc), do: replay(rest, ctx, c, r, acc)

  # ---------------------------------------------------------------- rows

  defp rows(ctx, columns, recorded) do
    recorded = Enum.map(recorded, &rename_server(&1, ctx.server))
    names = Enum.map(columns, &String.downcase(&1["name"]))

    case ctx.role do
      "databases" ->
        databases_rows(recorded, names)

      "database_by_name" ->
        database_rows(recorded, names, ctx.params["_msparam_0"])

      "tables" ->
        if ctx.database == @database, do: table_rows(names), else: master_only(recorded, ctx)

      "schemas" ->
        if ctx.database == @database, do: schema_rows(recorded), else: recorded

      _ ->
        recorded
    end
  end

  defp master_only(recorded, ctx), do: if(ctx.database == "master", do: recorded, else: [])

  # kurwadb is cloned from master's row, as a user database.
  defp databases_rows(recorded, names) do
    case Enum.find(recorded, &("master" in &1)) do
      nil ->
        recorded

      master ->
        mine = as_database(master, names, @database)
        Enum.sort_by([mine | recorded], &sort_key/1)
    end
  end

  defp database_rows(recorded, names, name) do
    cond do
      name not in databases() ->
        []

      recorded != [] ->
        Enum.map(recorded, &as_database(&1, names, name))

      true ->
        [Enum.map(names, &if(&1 in ~w(name databasename databasename2), do: name, else: nil))]
    end
  end

  defp as_database(row, names, name) do
    row
    |> Enum.zip(names)
    |> Enum.map(fn
      {_v, column} when column in ~w(id database_id) and name == @database -> 5
      {_v, "issystemobject"} when name == @database -> false
      {"master", _} -> name
      {v, _} when is_binary(v) -> String.replace(v, "'master'", "'#{name}'")
      {v, _} -> v
    end)
  end

  defp table_rows(names) do
    for set <- sets(), {schema, table} = split(set) do
      Enum.map(names, fn
        "schema" -> schema
        "name" -> table
        "id" -> object_id(set)
        _ -> nil
      end)
    end
    |> Enum.sort_by(fn row -> Enum.map(row, &sort_key/1) end)
  end

  defp schema_rows(recorded) do
    have = MapSet.new(recorded, fn [name | _] -> String.downcase(name) end)

    {:ok, schemas} = Kurwa.Registry.schemas()

    extra =
      for s <- schemas,
          s not in ["public", "dbo"],
          not MapSet.member?(have, String.downcase(s)),
          do: [s]

    Enum.sort_by(recorded ++ extra, &sort_key/1)
  end

  defp sets do
    case Kurwa.Namespace.list() do
      {:ok, %{sets: sets}} -> ["kurwa" | sets]
      _ -> ["kurwa"]
    end
  end

  defp split(set) do
    case String.split(set, ".", parts: 2) do
      [schema, table] -> {schema, table}
      [table] -> {"dbo", table}
    end
  end

  @doc "A set's object_id: stable, positive, an int."
  def object_id(set), do: 1_000_000 + :erlang.phash2(set, 1_000_000_000)

  defp sort_key(row) when is_list(row), do: row |> hd() |> sort_key()
  defp sort_key(v) when is_binary(v), do: String.downcase(v)
  defp sort_key(v), do: v

  defp rename_server(row, server), do: Enum.map(row, &rename(&1, server))

  defp rename(v, server) when is_binary(v), do: String.replace(v, @recorded_server, server)

  defp rename(%{"value" => v} = variant, server) when is_binary(v),
    do: %{variant | "value" => String.replace(v, @recorded_server, server)}

  defp rename(v, _server), do: v

  @doc "This node's server name: its host."
  def server_name do
    case node() |> Atom.to_string() |> String.split("@") do
      [_, host] when host != "nohost" -> host
      _ -> "kurwadb"
    end
  end

  # ---------------------------------------------------------------- encoding

  defp colmetadata(columns) do
    [
      0x81,
      <<length(columns)::16-little>>,
      Enum.map(columns, fn c ->
        [
          <<c["usertype"]::32-little, c["flags"]::16-little>>,
          type_info(c),
          Tds.b_varchar(c["name"])
        ]
      end)
    ]
  end

  @fixed [0x30, 0x34, 0x38, 0x7F, 0x32, 0x3B, 0x3E, 0x3C, 0x3D, 0x3A, 0x7A, 0x1F]
  @bytelen [0x24, 0x26, 0x68, 0x6D, 0x6E, 0x6F]
  @scaled [0x29, 0x2A, 0x2B]
  @decimal [0x6A, 0x6C]
  @ushort [0xA5, 0xA7, 0xAD, 0xAF, 0xE7, 0xEF]
  @collated [0xA7, 0xAF, 0xE7, 0xEF, 0x23, 0x63]
  @long [0x23, 0x63, 0x22]

  defp type_info(%{"code" => code} = c) do
    cond do
      code in @fixed -> code
      code in @bytelen -> [code, c["len"]]
      code in @decimal -> [code, c["len"], c["precision"], c["scale"]]
      code in @scaled -> [code, c["scale"]]
      code == 0x28 -> code
      code in @ushort -> [code, <<c["len"]::16-little>>, collation(c)]
      code in @long -> [code, <<c["len"]::32-little>>, collation(c), table(c)]
      code == 0x62 -> [code, <<c["len"]::32-little>>]
      code == 0xF1 -> [code, 0]
    end
  end

  defp collation(%{"code" => code, "collation" => hex}) when code in @collated,
    do: Base.decode16!(hex, case: :lower)

  defp collation(_), do: []

  defp table(%{"table" => parts}) when is_list(parts),
    do: [length(parts), Enum.map(parts, &Tds.us_varchar/1)]

  defp table(_), do: [0]

  defp row(values, columns), do: [0xD1, Enum.zip_with(values, columns, &value/2)]

  defp value(nil, %{"code" => code}) when code in @fixed, do: raise("NULL in a NOT NULL column")
  defp value(v, %{"code" => 0x30}), do: <<v>>
  defp value(v, %{"code" => 0x34}), do: <<v::16-little-signed>>
  defp value(v, %{"code" => 0x38}), do: <<v::32-little-signed>>
  defp value(v, %{"code" => 0x7F}), do: <<v::64-little-signed>>
  defp value(v, %{"code" => 0x32}), do: <<if(v, do: 1, else: 0)>>
  defp value(v, %{"code" => 0x3B}), do: <<v::32-float-little>>
  defp value(v, %{"code" => 0x3E}), do: <<v::64-float-little>>
  defp value(%{"hex" => hex}, %{"code" => code}) when code in @fixed, do: hex(hex)

  defp value(nil, %{"code" => code})
       when code in @bytelen or code in @decimal or code in @scaled or code == 0x28,
       do: <<0>>

  defp value(v, %{"code" => 0x26, "len" => len}), do: <<len, v::size(len * 8)-little-signed>>
  defp value(v, %{"code" => 0x68}), do: <<1, if(v, do: 1, else: 0)>>
  defp value(v, %{"code" => 0x6D, "len" => 4}) when is_number(v), do: <<4, v::32-float-little>>
  defp value(v, %{"code" => 0x6D}) when is_number(v), do: <<8, v::64-float-little>>
  defp value(%{"date" => hex}, %{"code" => 0x28}), do: [byte_size(hex(hex)), hex(hex)]

  defp value(%{"hex" => hex}, %{"code" => code})
       when code in @bytelen or code in @decimal or code in @scaled,
       do: [byte_size(hex(hex)), hex(hex)]

  defp value(nil, %{"code" => code, "len" => 0xFFFF}) when code in @ushort,
    do: <<0xFFFFFFFFFFFFFFFF::64>>

  defp value(v, %{"code" => code, "len" => 0xFFFF}) when code in @ushort do
    data = text_bytes(v, code)
    <<byte_size(data)::64-little, byte_size(data)::32-little, data::binary, 0::32>>
  end

  defp value(nil, %{"code" => code}) when code in @ushort, do: <<0xFFFF::16>>

  defp value(v, %{"code" => code}) when code in @ushort do
    data = text_bytes(v, code)
    <<byte_size(data)::16-little, data::binary>>
  end

  defp value(nil, %{"code" => code}) when code in @long, do: <<0>>

  defp value(v, %{"code" => code}) when code in @long do
    data = text_bytes(v, code)
    <<16, 0::128, 0::64, byte_size(data)::32-little, data::binary>>
  end

  defp value(nil, %{"code" => 0x62}), do: <<0::32>>

  defp value(v, %{"code" => 0x62}) do
    body = variant(v)
    <<byte_size(body)::32-little, body::binary>>
  end

  defp value(v, %{"code" => 0xF1}) do
    data = Tds.ucs2(v || "")
    <<byte_size(data)::64-little, byte_size(data)::32-little, data::binary, 0::32>>
  end

  defp text_bytes(%{"hex" => hex}, _code), do: hex(hex)
  defp text_bytes(v, code) when code in [0xE7, 0xEF, 0x63], do: Tds.ucs2(to_string(v))
  defp text_bytes(v, _code), do: :unicode.characters_to_binary(to_string(v), :utf8, :latin1)

  defp variant(%{"variant" => "nvarchar", "value" => v, "props" => props}) do
    props = hex(props)
    <<0xE7, byte_size(props), props::binary, Tds.ucs2(v)::binary>>
  end

  defp variant(%{"variant" => "varchar", "value" => v, "props" => props}) do
    props = hex(props)
    <<0xA7, byte_size(props), props::binary, v::binary>>
  end

  # Any other base type, replayed byte for byte.
  defp variant(%{"raw" => raw}), do: hex(raw)
  defp variant(%{"variant" => "int", "value" => v}), do: <<0x38, 0, v::32-little-signed>>
  defp variant(%{"variant" => "bigint", "value" => v}), do: <<0x7F, 0, v::64-little-signed>>
  defp variant(%{"variant" => "smallint", "value" => v}), do: <<0x34, 0, v::16-little-signed>>
  defp variant(%{"variant" => "tinyint", "value" => v}), do: <<0x30, 0, v>>
  defp variant(%{"variant" => "bit", "value" => v}), do: <<0x32, 0, if(v, do: 1, else: 0)>>

  defp hex(hex), do: Base.decode16!(hex, case: :lower)
end
