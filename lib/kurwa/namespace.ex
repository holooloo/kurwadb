defmodule Kurwa.Namespace do
  @moduledoc """
  Named sets, and set algebra across them on the read path.

  Straight out of Plan 9: a union mount (`bind -a b a`) makes one name resolve
  through several directories in order. For a store that holds nothing but keys,
  that is exactly set union - and it composes at read time, so a "view" over
  several sets costs nothing to create and copies no data.

      Kurwa.Namespace.add("blacklist", "ip:10.0.0.1")
      Kurwa.Namespace.member_any?(["blacklist", "greylist"], "ip:10.0.0.1")
      #=> {:ok, true}

  Each set in a union is a separate quorum read (the sets hash to different ring
  positions), and they run concurrently. `member_any?/3` stops at the first
  `true` and `member_all?/3` at the first `false`, because in either case the
  remaining sets cannot change the answer.

  Sets can be listed (`list/1`) because their names are themselves keys in a
  reserved set - see `Kurwa.Registry`. There is still no `count` per namespace
  and no listing of a set's *members*: both are scans. See
  `Kurwa.Coordinator.count/1` for the cluster-wide estimate.
  """

  alias Kurwa.Config
  alias Kurwa.Extractor
  alias Kurwa.Key
  alias Kurwa.Registry

  @type name :: binary()
  @type result :: {:ok, boolean()} | {:error, term()}

  @doc """
  Adds `key` to the set `name`. Takes the same options as `Kurwa.add/2`, `ttl:` included.

  The first add to a set also records that the set exists, so `list/1` can
  answer without a scan. That happens off the caller's path - see
  `Kurwa.Registry`.
  """
  @spec add(name(), binary(), keyword()) :: :ok | {:error, term()}
  def add(name, key, opts \\ []) do
    case Extractor.add(key, [set: name] ++ opts) do
      :ok ->
        Registry.register(name)
        :ok

      error ->
        error
    end
  end

  @doc "Every set the cluster knows about, and the nodes that could not be asked."
  @spec list(keyword()) :: {:ok, %{sets: [name()], unreachable: map()}}
  defdelegate list(opts \\ []), to: Registry

  @doc "Stops listing `name`. Its keys are untouched."
  @spec forget(name()) :: :ok | {:error, term()}
  defdelegate forget(name), to: Registry

  @doc "Removes `key` from the set `name`."
  @spec delete(name(), binary(), keyword()) :: :ok | {:error, term()}
  def delete(name, key, opts \\ []), do: Extractor.delete(key, [set: name] ++ opts)

  @doc "Is `key` in the set `name`?"
  @spec member?(name(), binary(), keyword()) :: result()
  def member?(name, key, opts \\ []), do: Extractor.member?(key, [set: name] ++ opts)

  @doc """
  Union: is `key` in *any* of these sets?

  A single `true` settles it, so an unreachable set only produces an error when
  no set answered `true`.
  """
  @spec member_any?([name()], binary(), keyword()) :: result()
  def member_any?(names, key, opts \\ []), do: combine(names, key, opts, true)

  @doc """
  Intersection: is `key` in *every* one of these sets?

  Mirror image of `member_any?/3` - one `false` settles it.
  """
  @spec member_all?([name()], binary(), keyword()) :: result()
  def member_all?(names, key, opts \\ []), do: combine(names, key, opts, false)

  @doc "Names that `add/3` and friends will accept."
  @spec valid_name?(term()) :: boolean()
  defdelegate valid_name?(name), to: Key

  # `decisive` is the answer that ends the search: true for a union, false for an
  # intersection. Anything else, and we have to hear from every set.
  defp combine([], _key, _opts, decisive), do: {:ok, not decisive}

  defp combine(names, key, opts, decisive) do
    timeout = Keyword.get(opts, :timeout, Config.request_timeout())

    names
    |> Task.async_stream(fn name -> {name, member?(name, key, opts)} end,
      ordered: false,
      max_concurrency: max(length(names), 1),
      on_timeout: :kill_task,
      timeout: timeout * 2
    )
    |> Enum.reduce_while({not decisive, []}, fn
      {:ok, {_name, {:ok, ^decisive}}}, _acc ->
        {:halt, {decisive, []}}

      {:ok, {_name, {:ok, _other}}}, acc ->
        {:cont, acc}

      {:ok, {name, {:error, reason}}}, {answer, errors} ->
        {:cont, {answer, [{name, reason} | errors]}}

      {:exit, reason}, {answer, errors} ->
        {:cont, {answer, [{:unknown, reason} | errors]}}
    end)
    |> case do
      {answer, []} -> {:ok, answer}
      {^decisive, _errors} -> {:ok, decisive}
      {_answer, errors} -> {:error, {:incomplete_union, Map.new(errors)}}
    end
  end
end
