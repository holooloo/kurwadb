defmodule Kurwa.Key do
  @moduledoc """
  Storage keys are namespace-qualified.

  Everything below `Kurwa.Coordinator` deals in *storage keys*: a length-prefixed
  namespace followed by the caller's key.

      <<0>> <> key                              the unnamed default set
      <<byte_size(name), name::binary>> <> key  a named set

  The length prefix is what makes the encoding total. A plain `name <> "\\0" <> key`
  separator would let the key `"blacklist\\0user:1"` in the default set collide
  with `user:1` in the `blacklist` set, and kurwadb accepts arbitrary binary
  keys, so that collision is reachable rather than theoretical.

  Namespaces also decide placement: the ring hashes the *storage* key, so two
  sets spread across the cluster independently instead of piling the same key's
  replicas onto the same nodes.
  """

  @max_name 255
  # ':' because a Redis user's sets are called things like seen:orders, and it
  # is as safe as '.' in a URL path segment and a 9P file name.
  @name_pattern ~r/^[A-Za-z0-9][A-Za-z0-9_.:\-]*$/

  # Reserved namespaces begin with an underscore, which `valid_name?/1` rejects,
  # so nothing a caller can spell will ever land in one.
  @registry "_sets"
  @hint "_hint"

  @type name :: binary() | nil
  @type storage_key :: binary()

  @doc "Storage key for the default (unnamed) set."
  @spec encode(binary()) :: storage_key()
  def encode(key) when is_binary(key), do: <<0, key::binary>>

  @doc """
  Storage key for `key` inside `name`. `nil` means the default set.

  Raises `ArgumentError` on a namespace that `valid_name?/1` rejects - the name
  comes from the caller's own code or from a route that validated it first.
  """
  @spec encode(name(), binary()) :: storage_key()
  def encode(nil, key) when is_binary(key), do: encode(key)

  def encode(name, key) when is_binary(name) and is_binary(key) do
    unless valid_name?(name) do
      raise ArgumentError, "invalid namespace #{inspect(name)}"
    end

    <<byte_size(name)::8, name::binary, key::binary>>
  end

  @doc "Splits a storage key back into `{namespace, key}`."
  @spec decode(storage_key()) :: {name(), binary()}
  def decode(<<0, key::binary>>), do: {nil, key}

  def decode(<<len::8, rest::binary>>) when byte_size(rest) >= len do
    <<name::binary-size(^len), key::binary>> = rest
    {name, key}
  end

  @doc """
  Is this a usable namespace name?

  Deliberately narrow: names appear in URL path segments and, once the 9P
  frontend lands, in directory names, so they stay to characters that survive
  both.
  """
  @spec valid_name?(term()) :: boolean()
  def valid_name?(name) when is_binary(name) do
    byte_size(name) in 1..@max_name and Regex.match?(@name_pattern, name)
  end

  def valid_name?(_), do: false

  @doc "Longest namespace name the prefix can hold."
  def max_name_length, do: @max_name

  @doc """
  Storage key recording that the set `name` exists.

  The registry is an ordinary set whose members are set names, which is what
  makes it replicate, merge and repair itself with no new machinery. Its
  namespace is `_sets`, and the leading underscore is the whole guarantee:
  `valid_name?/1` refuses it, so `encode/2` can never produce a colliding key.
  """
  @spec registry_key(binary()) :: storage_key()
  def registry_key(name) when is_binary(name),
    do: <<byte_size(@registry)::8, @registry::binary, name::binary>>

  @doc "The set name a registry key records, or `:error` if it is not one."
  @spec registry_name(storage_key()) :: {:ok, binary()} | :error
  def registry_name(<<len::8, rest::binary>>) when len == byte_size(@registry) do
    case rest do
      <<@registry, name::binary>> -> {:ok, name}
      _ -> :error
    end
  end

  def registry_name(_), do: :error

  @doc """
  Is this a key kurwadb keeps for itself?

  Any namespace starting with an underscore is reserved, which `valid_name?/1`
  makes unreachable from outside. System keys route to a shard of their own, so
  that walking them costs their own number rather than the number of keys.
  """
  @spec system?(storage_key()) :: boolean()
  def system?(<<len::8, ?_, _rest::binary>>) when len > 0, do: true
  def system?(_), do: false

  @doc """
  Storage key for a write that `target` was not around to take.

  A hint is an ordinary record: this key says who owes it and for what, and the
  record's own fields are the original's, so replaying it reconstructs the write
  exactly rather than approximately. Being an ordinary record is also what makes
  it durable - it goes through the same WAL as everything else.

  The original's own `alive?` lives in the key rather than in the record,
  because the record's `alive?` has a job of its own: `true` means the hint is
  still owed, `false` means it has been delivered. Keeping the original there
  too would make a hint for a *delete* indistinguishable from a hint that has
  already been handed over.
  """
  @spec hint_key(node(), storage_key(), boolean()) :: storage_key()
  def hint_key(target, storage_key, original_alive?)
      when is_atom(target) and is_binary(storage_key) and is_boolean(original_alive?) do
    name = Atom.to_string(target)
    flag = if original_alive?, do: 1, else: 0

    <<byte_size(@hint)::8, @hint::binary, flag::8, byte_size(name)::8, name::binary,
      storage_key::binary>>
  end

  @doc "Splits a hint key back into `{target, storage_key, original_alive?}`."
  @spec hint_parts(storage_key()) :: {:ok, node(), storage_key(), boolean()} | :error
  def hint_parts(<<len::8, rest::binary>>) when len == byte_size(@hint) do
    case rest do
      <<@hint, flag::8, name_len::8, name::binary-size(name_len), storage_key::binary>> ->
        {:ok, String.to_atom(name), storage_key, flag == 1}

      _ ->
        :error
    end
  end

  def hint_parts(_), do: :error

  @doc """
  Is this key this node's own business, never to be replicated or compared?

  Hints are: they record what *this* node owes someone else. Registry entries
  are not - they are ordinary replicated data that happens to live in a reserved
  namespace.
  """
  @spec local_only?(storage_key()) :: boolean()
  def local_only?(<<len::8, rest::binary>>) when len == byte_size(@hint),
    do: match?(<<@hint, _::binary>>, rest)

  def local_only?(_), do: false
end
