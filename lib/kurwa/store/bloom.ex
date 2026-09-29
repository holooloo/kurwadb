defmodule Kurwa.Store.Bloom do
  @moduledoc """
  A Bloom filter, which for this store is not an optimisation but the whole read
  path.

  "Is this key here" is the only question kurwadb ever asks a table, and a Bloom
  filter answers it from memory, wrongly in one direction only: a `false` is
  certain, a `true` might be a miss that costs one seek. That is exactly the
  shape that lets an on-disk table keep almost nothing in RAM - about 1.25 bytes
  per key at a 1% false-positive rate, against the 128 bytes a key costs in ETS.

  Built into `:atomics` so adding a key allocates nothing, then frozen into a
  binary that is stored in the table file and mapped back on open.

  ## Why FNV-1a and not `:erlang.phash2`

  The filter has to be readable by more than one implementation: a table written
  here is queried by whatever is fastest at the time, and the bits have to come
  out identical either way. `phash2` is an Erlang internal with no portable
  definition, so the hash is FNV-1a instead - sixty-four bits, one pass, four
  lines in any language - split into two halves and combined the
  Kirsch-Mitzenmacher way to get however many probes are needed.
  """

  import Bitwise

  @doc """
  Sizes a filter for `capacity` keys at `fp_rate`.

  `m = -n·ln(p) / (ln 2)²` and `k = (m/n)·ln 2`, rounded, which is the standard
  sizing: at 1% that lands near 10 bits and 7 hashes per key.
  """
  @spec new(pos_integer(), float()) :: map()
  def new(capacity, fp_rate \\ 0.01) when capacity > 0 and fp_rate > 0 and fp_rate < 1 do
    ln2 = :math.log(2)
    bits = max(ceil(-capacity * :math.log(fp_rate) / (ln2 * ln2)), 64)
    # round up to whole 64-bit words, so the bitmap has no ragged tail
    bits = bits + rem(64 - rem(bits, 64), 64)
    hashes = max(round(bits / capacity * ln2), 1)

    %{bits: bits, hashes: hashes, words: :atomics.new(div(bits, 64), signed: false)}
  end

  @doc "Adds a key to a filter under construction."
  @spec add(map(), binary()) :: :ok
  def add(%{bits: bits, hashes: hashes, words: words}, key) do
    Enum.each(0..(hashes - 1), fn i ->
      position = hash(key, i, bits)
      index = div(position, 64) + 1
      mask = 1 <<< rem(position, 64)
      :atomics.put(words, index, :atomics.get(words, index) ||| mask)
    end)
  end

  @doc "Freezes a filter under construction into the bytes that go in the file."
  @spec to_binary(map()) :: binary()
  def to_binary(%{bits: bits, words: words}) do
    for i <- 1..div(bits, 64), into: <<>>, do: <<:atomics.get(words, i)::little-64>>
  end

  @doc """
  Might `key` be in the set this filter was built from?

  `false` is certain. `true` means "look", and is wrong `fp_rate` of the time.

  Dispatched at compile time: when the native code is built in this calls into
  it, otherwise into `member_elixir?/4` below. No runtime check, because this is
  the hot path of the on-disk engine and a branch here would be a measurable
  share of the call.
  """
  @spec member?(binary(), pos_integer(), pos_integer(), binary()) :: boolean()

  if Kurwa.Native.available?() do
    def member?(binary, bits, hashes, key),
      do: Kurwa.Native.bloom_member(binary, bits, hashes, key)
  else
    def member?(binary, bits, hashes, key), do: member_elixir?(binary, bits, hashes, key)
  end

  @doc """
  The Elixir implementation, always present.

  Kept reachable whether or not the native one was built, so the two can be
  checked against each other rather than assumed to agree.
  """
  @spec member_elixir?(binary(), pos_integer(), pos_integer(), binary()) :: boolean()
  def member_elixir?(binary, bits, hashes, key) do
    Enum.all?(0..(hashes - 1), fn i ->
      position = hash(key, i, bits)
      word = :binary.part(binary, div(position, 64) * 8, 8)
      <<value::little-64>> = word
      (value >>> rem(position, 64) &&& 1) == 1
    end)
  end

  @doc "Is the fast path in use?"
  @spec native?() :: boolean()
  def native?, do: Kurwa.Native.available?()

  @doc "Bytes this filter occupies, and bytes per key at that capacity."
  @spec size(map() | pos_integer(), pos_integer()) :: %{bytes: pos_integer(), per_key: float()}
  def size(%{bits: bits}, capacity), do: size(bits, capacity)

  def size(bits, capacity) when is_integer(bits) do
    bytes = div(bits, 8)
    %{bytes: bytes, per_key: Float.round(bytes / capacity, 2)}
  end

  # One 64-bit hash split in half gives the two the combination needs, so the
  # key is walked once however many probes there are.
  defp hash(key, i, bits) do
    h = fnv1a(key)
    h1 = h &&& 0xFFFFFFFF
    h2 = h >>> 32
    rem(h1 + i * h2 + i * i, bits)
  end

  @fnv_offset 0xCBF29CE484222325
  @fnv_prime 0x100000001B3
  @mask64 0xFFFFFFFFFFFFFFFF

  @doc "FNV-1a over the key, 64 bit. Public so another implementation can be checked against it."
  @spec fnv1a(binary()) :: non_neg_integer()
  def fnv1a(key) when is_binary(key), do: fnv1a(key, @fnv_offset)

  defp fnv1a(<<>>, acc), do: acc

  defp fnv1a(<<byte, rest::binary>>, acc) do
    fnv1a(rest, Bitwise.band(Bitwise.bxor(acc, byte) * @fnv_prime, @mask64))
  end
end
