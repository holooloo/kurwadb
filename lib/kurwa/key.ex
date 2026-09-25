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
  @name_pattern ~r/^[A-Za-z0-9][A-Za-z0-9_.\-]*$/

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
end
