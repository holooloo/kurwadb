defmodule Kurwa.NineP.Fs do
  @moduledoc """
  The kurwadb tree as 9P sees it.

  The mapping is not a metaphor - it is the same data model. A set has no values,
  and a directory of empty files has no values either, so:

      stat   -> member?      walking to a key fails with ENOENT when it is absent
      create -> add
      remove -> delete

      /ctl                write "compact" | "gc" | "sync" | "join node@host"
      /stats              read: local keys, lamport, members
      /ring               read: ring membership and placement settings
      /keys/<key>         the default set, key as the file name
      /b64/<base64url>    the default set, for keys that are not valid file names
      /sets/<set>/<key>   a named set (`Kurwa.Namespace`)

  Two honest limits, both consequences of having no scans:

  * `/keys`, `/b64` and `/sets/<set>` cannot be listed. Reading them is an error
    rather than an empty directory, because an empty listing would be a lie.
  * `/sets` lists as empty. Sets have no existence of their own - a set is
    whichever keys carry its prefix - so there is nothing to enumerate.

  This module holds no state: a fid is just a path, and the server hands it back
  on every call.
  """

  alias Kurwa.Cluster
  alias Kurwa.Config
  alias Kurwa.Key
  alias Kurwa.Namespace
  alias Kurwa.NineP.Proto
  alias Kurwa.NineP.Stat
  alias Kurwa.Ring

  import Bitwise, only: [|||: 2, &&&: 2]

  @enoent "file does not exist"
  @eperm "permission denied"
  @noscan "kurwadb has no scans: stat a key directly instead of listing"

  @type path ::
          {:dir, :root | :keys | :b64 | :sets | {:set, binary()}}
          | {:file, :ctl | :stats | :ring | {:key, binary() | nil, binary(), binary()}}

  @root {:dir, :root}

  @doc "The path a fresh attach lands on."
  def root, do: @root

  @doc """
  Resolves one path element from `path`.

  For a key this is where the quorum read happens: a walk that succeeds *is* a
  positive membership answer, which is what makes `stat` the natural client-side
  `member?`.
  """
  @spec walk(path(), binary()) :: {:ok, path()} | {:error, binary()}
  def walk(path, "."), do: {:ok, path}
  def walk(path, ".."), do: {:ok, parent(path)}

  def walk({:dir, :root}, name) do
    case name do
      "ctl" -> {:ok, {:file, :ctl}}
      "stats" -> {:ok, {:file, :stats}}
      "ring" -> {:ok, {:file, :ring}}
      "keys" -> {:ok, {:dir, :keys}}
      "b64" -> {:ok, {:dir, :b64}}
      "sets" -> {:ok, {:dir, :sets}}
      _ -> {:error, @enoent}
    end
  end

  def walk({:dir, :keys}, name), do: key_walk(nil, name, name)

  def walk({:dir, :b64}, name) do
    case Base.url_decode64(name, padding: false) do
      {:ok, key} -> key_walk(nil, key, name)
      :error -> {:error, "not a base64url name"}
    end
  end

  def walk({:dir, :sets}, name) do
    if Namespace.valid_name?(name),
      do: {:ok, {:dir, {:set, name}}},
      else: {:error, "not a usable set name"}
  end

  def walk({:dir, {:set, set}}, name), do: key_walk(set, name, name)

  # Files are leaves: walking past one is an error, not an ENOENT.
  def walk({:file, _}, _name), do: {:error, "not a directory"}

  @doc "Parent of `path`; the root is its own parent, as 9P expects."
  def parent({:dir, :root}), do: @root
  def parent({:dir, :keys}), do: @root
  def parent({:dir, :b64}), do: @root
  def parent({:dir, :sets}), do: @root
  def parent({:dir, {:set, _}}), do: {:dir, :sets}
  def parent({:file, {:key, nil, _key, _name}}), do: {:dir, :keys}
  def parent({:file, {:key, set, _key, _name}}), do: {:dir, {:set, set}}
  def parent({:file, _}), do: @root

  @doc "Qid of a path: `{type, version, unique-path}`."
  @spec qid(path()) :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}
  def qid({:dir, :root}), do: {Proto.qtdir(), 0, 1}
  def qid({:file, :ctl}), do: {Proto.qtfile(), 0, 2}
  def qid({:file, :stats}), do: {Proto.qtfile(), 0, 3}
  def qid({:file, :ring}), do: {Proto.qtfile(), 0, 4}
  def qid({:dir, :keys}), do: {Proto.qtdir(), 0, 5}
  def qid({:dir, :b64}), do: {Proto.qtdir(), 0, 6}
  def qid({:dir, :sets}), do: {Proto.qtdir(), 0, 7}
  def qid({:dir, {:set, set}}), do: {Proto.qtdir(), 0, Ring.hash("set/" <> set)}

  def qid({:file, {:key, set, key, _name}}),
    do: {Proto.qtfile(), 0, Ring.hash(Key.encode(set, key))}

  @doc "Name of a path as it appears in its parent directory."
  def name({:dir, :root}), do: "/"
  def name({:dir, :keys}), do: "keys"
  def name({:dir, :b64}), do: "b64"
  def name({:dir, :sets}), do: "sets"
  def name({:dir, {:set, set}}), do: set
  def name({:file, :ctl}), do: "ctl"
  def name({:file, :stats}), do: "stats"
  def name({:file, :ring}), do: "ring"
  def name({:file, {:key, _set, _key, name}}), do: name

  @doc "Stat of a path."
  @spec stat(path()) :: Stat.t()
  def stat({:dir, _} = path) do
    %Stat{qid: qid(path), mode: Proto.dmdir() ||| 0o555, name: name(path)}
  end

  def stat({:file, :ctl} = path) do
    %Stat{qid: qid(path), mode: 0o222, name: "ctl"}
  end

  def stat({:file, kind} = path) when kind in [:stats, :ring] do
    %Stat{qid: qid(path), mode: 0o444, name: name(path), length: byte_size(content(kind))}
  end

  # A key file is empty on purpose: its existence is the whole payload.
  def stat({:file, {:key, _, _, _}} = path) do
    %Stat{qid: qid(path), mode: 0o444, name: name(path)}
  end

  @doc "May this path be opened in `mode`?"
  @spec openable(path(), non_neg_integer()) :: :ok | {:error, binary()}
  def openable(path, mode) do
    write? = (mode &&& 3) in [1, 2]

    cond do
      write? and path == {:file, :ctl} -> :ok
      write? -> {:error, @eperm}
      path == {:file, :ctl} -> {:error, "ctl is write-only"}
      true -> :ok
    end
  end

  @doc """
  What a read of this path returns.

  Directories answer with stat entries, files with bytes, and the unlistable
  directories with an error that says why.
  """
  @spec contents(path()) :: {:file, binary()} | {:dir, [Stat.t()]} | {:error, binary()}
  def contents({:dir, :root}) do
    {:dir,
     Enum.map(
       [
         {:file, :ctl},
         {:file, :stats},
         {:file, :ring},
         {:dir, :keys},
         {:dir, :b64},
         {:dir, :sets}
       ],
       &stat/1
     )}
  end

  # Sets exist only as a prefix on their keys, so there is nothing to enumerate.
  def contents({:dir, :sets}), do: {:dir, []}
  def contents({:dir, dir}) when dir in [:keys, :b64], do: {:error, @noscan}
  def contents({:dir, {:set, _}}), do: {:error, @noscan}
  def contents({:file, :ctl}), do: {:error, "ctl is write-only"}
  def contents({:file, kind}) when kind in [:stats, :ring], do: {:file, content(kind)}
  def contents({:file, {:key, _, _, _}}), do: {:file, ""}

  @doc "Creating a name in a directory: `add`, or a no-op `mkdir` for a set."
  @spec create(path(), binary(), non_neg_integer()) :: {:ok, path()} | {:error, binary()}
  def create({:dir, :sets}, name, perm) do
    cond do
      (perm &&& Proto.dmdir()) == 0 ->
        {:error, "/sets holds sets, not keys - create a directory"}

      not Namespace.valid_name?(name) ->
        {:error, "not a usable set name"}

      true ->
        # Sets need no creation; saying yes keeps `mkdir` honest about the result.
        {:ok, {:dir, {:set, name}}}
    end
  end

  def create({:dir, :keys}, name, perm), do: add(nil, name, name, perm)

  def create({:dir, :b64}, name, perm) do
    case Base.url_decode64(name, padding: false) do
      {:ok, key} -> add(nil, key, name, perm)
      :error -> {:error, "not a base64url name"}
    end
  end

  def create({:dir, {:set, set}}, name, perm), do: add(set, name, name, perm)
  def create(_path, _name, _perm), do: {:error, @eperm}

  @doc "Removing a key file deletes the key. Nothing else may be removed."
  @spec remove(path()) :: :ok | {:error, binary()}
  def remove({:file, {:key, nil, key, _name}}), do: unwrap(Kurwa.delete(key))
  def remove({:file, {:key, set, key, _name}}), do: unwrap(Namespace.delete(set, key))
  def remove(_path), do: {:error, @eperm}

  @doc "Writes go to `/ctl` only, one command per write."
  @spec write(path(), binary()) :: {:ok, non_neg_integer()} | {:error, binary()}
  def write({:file, :ctl}, data) do
    case control(String.trim(data)) do
      :ok -> {:ok, byte_size(data)}
      {:error, reason} -> {:error, reason}
    end
  end

  def write(_path, _data), do: {:error, @eperm}

  defp control("compact") do
    Kurwa.Store.compact()
    :ok
  end

  defp control("gc") do
    Kurwa.Store.gc()
    :ok
  end

  defp control("sync") do
    Kurwa.Store.sync()
    :ok
  end

  defp control("join " <> target) do
    node = target |> String.trim() |> String.to_atom()

    if Node.connect(node) == true do
      Cluster.check(node)
      :ok
    else
      {:error, "cannot reach #{node}"}
    end
  end

  defp control(other), do: {:error, "unknown control message: #{inspect(other)}"}

  defp add(set, key, name, perm) do
    cond do
      (perm &&& Proto.dmdir()) != 0 ->
        {:error, "a key is not a directory"}

      true ->
        result = if set, do: Namespace.add(set, key), else: Kurwa.add(key)

        case unwrap(result) do
          :ok -> {:ok, {:file, {:key, set, key, name}}}
          error -> error
        end
    end
  end

  defp key_walk(set, key, name) do
    result = if set, do: Namespace.member?(set, key), else: Kurwa.fetch(key)

    case result do
      {:ok, true} -> {:ok, {:file, {:key, set, key, name}}}
      {:ok, false} -> {:error, @enoent}
      {:error, reason} -> {:error, describe(reason)}
    end
  end

  defp content(:stats) do
    info = Kurwa.info()

    """
    node #{info.node}
    members #{length(info.members)}
    local_keys #{info.local_keys}
    lamport #{info.lamport}
    shards #{info.shards}
    engine #{inspect(info.engine)}
    """
  end

  defp content(:ring) do
    ring = Cluster.ring()

    members = ring |> Ring.nodes() |> Enum.map_join("\n", &"member #{&1}")

    """
    #{members}
    vnodes #{ring.vnodes}
    n #{Config.n()}
    r #{Config.r()}
    w #{Config.w()}
    strict_quorum #{Config.get(:strict_quorum)}
    """
  end

  defp unwrap(:ok), do: :ok
  defp unwrap({:error, reason}), do: {:error, describe(reason)}

  # 9P carries errors as text, so a quorum failure has to survive as a sentence.
  defp describe(:ring_empty), do: "no nodes in the ring"

  defp describe({:quorum_not_met, %{op: op, needed: needed, got: got}}),
    do: "#{op} quorum not met: needed #{needed} replicas, got #{got}"

  defp describe({:incomplete_union, sets}),
    do: "could not read every set: #{inspect(Map.keys(sets))}"

  defp describe(other), do: inspect(other)
end
