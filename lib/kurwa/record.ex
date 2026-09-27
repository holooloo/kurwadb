defmodule Kurwa.Record do
  @moduledoc """
  The only thing kurwadb stores: a key plus just enough metadata to merge two
  copies of it without asking anyone.

  Layout (also the literal ETS object, element 1 is the key):

      {key, lamport, node, alive?, wall, expires_at}

  * `lamport`    - Lamport counter of the coordinator that accepted the write.
  * `node`       - that coordinator, used only as a tiebreak so merges are total.
  * `alive?`     - `true` means "in the set", `false` is a tombstone.
  * `wall`       - wall-clock ms, used only to age tombstones out, never to order.
  * `expires_at` - wall-clock ms after which the key is not a member, or `:never`.

  Merge is last-writer-wins on `{lamport, node}`. This makes the store a
  convergent LWW-Set: any two replicas that have seen the same set of writes
  agree, in any order, with no coordination. The tradeoff is spelled out in the
  README - a concurrent delete can beat a concurrent add.

  ## Expiry

  `:never` is a deliberate choice of sentinel rather than `nil` or `0`. In Erlang
  term order every number sorts before every atom, so `expires_at < now` is
  already false for `:never` - the no-expiry case needs no special case at all,
  in the guards here or in the ETS match specs the sweeper uses.

  Expiry is the one place wall-clock time decides an answer, so it is subject to
  clock skew between nodes in a way that ordering is not. It is also a *pure
  function of the record*: every replica holding the same record reaches the same
  verdict at the same moment without exchanging anything, which is why an expired
  key needs no tombstone to stay deleted.
  """

  @type key :: binary()
  @type expiry :: integer() | :never
  @type t :: {key(), non_neg_integer(), node(), boolean(), integer(), expiry()}

  @doc "Builds a record. `wall` defaults to now and only matters for ageing tombstones out."
  @spec new(key(), non_neg_integer(), node(), boolean(), integer() | nil, expiry()) :: t()
  def new(key, lamport, node, alive?, wall \\ nil, expires_at \\ :never)
      when is_binary(key) and is_integer(lamport) and lamport >= 0 and is_atom(node) and
             is_boolean(alive?) and (expires_at == :never or is_integer(expires_at)) do
    {key, lamport, node, alive?, wall || System.system_time(:millisecond), expires_at}
  end

  @spec key(t()) :: key()
  def key({key, _, _, _, _, _}), do: key

  @spec lamport(t()) :: non_neg_integer()
  def lamport({_, lamport, _, _, _, _}), do: lamport

  @spec origin(t()) :: node()
  def origin({_, _, node, _, _, _}), do: node

  @doc """
  The tombstone flag on its own: `false` means a delete was recorded.

  This is not "is the key in the set" - an expired record is still `alive?`.
  Use `member?/1` for the question a client actually asks.
  """
  @spec alive?(t() | nil) :: boolean()
  def alive?(nil), do: false
  def alive?({_, _, _, alive?, _, _}), do: alive?

  @doc """
  Is the key in the set right now: not a tombstone, and not past its expiry.

  Records with no expiry answer without reading the clock at all, so putting TTL
  in the model costs the keys that do not use it nothing.
  """
  @spec member?(t() | nil) :: boolean()
  def member?(nil), do: false
  def member?({_, _, _, false, _, _}), do: false
  def member?({_, _, _, true, _, :never}), do: true
  def member?({_, _, _, true, _, expires_at}), do: expires_at > System.system_time(:millisecond)

  @spec expires_at(t()) :: expiry()
  def expires_at({_, _, _, _, _, expires_at}), do: expires_at

  @doc "Has this record passed its expiry as of `now`? Always false without one."
  @spec expired?(t(), integer()) :: boolean()
  def expired?({_, _, _, _, _, expires_at}, now), do: expires_at < now

  @doc "Milliseconds left before the key expires, or `:never`."
  @spec ttl(t()) :: non_neg_integer() | :never
  def ttl({_, _, _, _, _, :never}), do: :never

  def ttl({_, _, _, _, _, expires_at}),
    do: max(expires_at - System.system_time(:millisecond), 0)

  @spec wall(t()) :: integer()
  def wall({_, _, _, _, wall, _}), do: wall

  @doc "Strictly newer than: total order on `{lamport, node}`."
  @spec newer?(t(), t() | nil) :: boolean()
  def newer?(_new, nil), do: true

  def newer?({_, l1, n1, _, _, _}, {_, l2, n2, _, _, _}) do
    {l1, n1} > {l2, n2}
  end

  @doc """
  Picks the winner of two copies of the same key.

  Commutative, associative and idempotent - which is the whole point.
  """
  @spec merge(t() | nil, t() | nil) :: t() | nil
  def merge(nil, other), do: other
  def merge(rec, nil), do: rec

  def merge(a, b) do
    if newer?(a, b), do: a, else: b
  end

  @doc "Merges a list of copies (missing replicas pass `nil`)."
  @spec merge_all([t() | nil]) :: t() | nil
  def merge_all(records), do: Enum.reduce(records, nil, &merge/2)

  @doc "True when the two records are the same version of the key."
  @spec same_version?(t() | nil, t() | nil) :: boolean()
  def same_version?(nil, nil), do: true
  def same_version?(nil, _), do: false
  def same_version?(_, nil), do: false
  def same_version?({_, l, n, _, _, _}, {_, l, n, _, _, _}), do: true
  def same_version?(_, _), do: false
end
