defmodule Kurwa.Record do
  @moduledoc """
  The only thing kurwadb stores: a key plus just enough metadata to merge two
  copies of it without asking anyone.

  Layout (also the literal ETS object, element 1 is the key):

      {key, lamport, node, alive?, wall}

  * `lamport` - Lamport counter of the coordinator that accepted the write.
  * `node`    - that coordinator, used only as a tiebreak so merges are total.
  * `alive?`  - `true` means "in the set", `false` is a tombstone.
  * `wall`    - wall-clock ms, used *only* to expire tombstones, never to order.

  Merge is last-writer-wins on `{lamport, node}`. This makes the store a
  convergent LWW-Set: any two replicas that have seen the same set of writes
  agree, in any order, with no coordination. The tradeoff is spelled out in the
  README - a concurrent delete can beat a concurrent add.
  """

  @type key :: binary()
  @type t :: {key(), non_neg_integer(), node(), boolean(), integer()}

  @doc "Builds a record. `wall` defaults to now and only matters for tombstone GC."
  @spec new(key(), non_neg_integer(), node(), boolean(), integer() | nil) :: t()
  def new(key, lamport, node, alive?, wall \\ nil)
      when is_binary(key) and is_integer(lamport) and lamport >= 0 and is_atom(node) and
             is_boolean(alive?) do
    {key, lamport, node, alive?, wall || System.system_time(:millisecond)}
  end

  @spec key(t()) :: key()
  def key({key, _, _, _, _}), do: key

  @spec lamport(t()) :: non_neg_integer()
  def lamport({_, lamport, _, _, _}), do: lamport

  @spec origin(t()) :: node()
  def origin({_, _, node, _, _}), do: node

  @spec alive?(t() | nil) :: boolean()
  def alive?(nil), do: false
  def alive?({_, _, _, alive?, _}), do: alive?

  @spec wall(t()) :: integer()
  def wall({_, _, _, _, wall}), do: wall

  @doc "Strictly newer than: total order on `{lamport, node}`."
  @spec newer?(t(), t() | nil) :: boolean()
  def newer?(_new, nil), do: true

  def newer?({_, l1, n1, _, _}, {_, l2, n2, _, _}) do
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
  def same_version?({_, l, n, _, _}, {_, l, n, _, _}), do: true
  def same_version?(_, _), do: false
end
