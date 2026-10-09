defmodule Kurwa.Ring do
  @moduledoc """
  Consistent hash ring.

  Each node is hashed onto the ring `vnodes` times, so ownership is spread
  evenly and adding or removing a node moves only ~1/len(nodes) of the keyspace
  instead of reshuffling everything.

  The ring is a plain immutable struct: build it once per membership change and
  read it from anywhere. `preflist/3` is the only hot-path function and does a
  binary search over a tuple, so lookups stay O(log P) with no process hop.
  """

  defstruct nodes: [], vnodes: 128, points: {}

  @type t :: %__MODULE__{
          nodes: [node()],
          vnodes: pos_integer(),
          points: tuple()
        }

  @doc "Builds a ring for `nodes`. Deterministic: same input, same ring, on every node."
  @spec new([node()], pos_integer()) :: t()
  def new(nodes, vnodes \\ 128) when is_list(nodes) and is_integer(vnodes) and vnodes > 0 do
    nodes = nodes |> Enum.uniq() |> Enum.sort()

    points =
      for node <- nodes, i <- 0..(vnodes - 1) do
        {hash("#{node}/#{i}"), node}
      end
      |> Enum.sort()
      |> List.to_tuple()

    %__MODULE__{nodes: nodes, vnodes: vnodes, points: points}
  end

  @doc "Members of the ring."
  @spec nodes(t()) :: [node()]
  def nodes(%__MODULE__{nodes: nodes}), do: nodes

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{nodes: nodes}), do: length(nodes)

  @spec member?(t(), node()) :: boolean()
  def member?(%__MODULE__{nodes: nodes}, node), do: node in nodes

  @doc """
  The `n` nodes responsible for `key`, in preference order.

  Returns fewer than `n` entries when the cluster is smaller than `n` - the
  caller decides whether that still satisfies its quorum.
  """
  @spec preflist(t(), binary(), pos_integer()) :: [node()]
  def preflist(ring, key, n \\ 3)

  def preflist(%__MODULE__{nodes: []}, _key, _n), do: []

  def preflist(%__MODULE__{points: points, nodes: nodes}, key, n)
      when is_binary(key) and is_integer(n) and n > 0 do
    want = min(n, length(nodes))
    total = tuple_size(points)
    start = lower_bound(points, hash(key), 0, total)
    start = if start == total, do: 0, else: start

    walk(points, start, total, want, [], 0, 0)
  end

  @doc "First node of the preference list, or `nil` on an empty ring."
  @spec owner(t(), binary()) :: node() | nil
  def owner(ring, key) do
    case preflist(ring, key, 1) do
      [node] -> node
      [] -> nil
    end
  end

  @doc "64-bit ring position of a key."
  @spec hash(binary()) :: non_neg_integer()
  def hash(key) when is_binary(key) do
    <<position::unsigned-integer-size(64), _rest::binary>> = :crypto.hash(:sha256, key)
    position
  end

  # Lowest index whose point is >= h; `total` means "past the end", i.e. wrap.
  defp lower_bound(points, h, lo, hi) do
    if lo >= hi do
      lo
    else
      mid = div(lo + hi, 2)
      {point, _node} = elem(points, mid)

      if point >= h,
        do: lower_bound(points, h, lo, mid),
        else: lower_bound(points, h, mid + 1, hi)
    end
  end

  # Clockwise from `idx`, collecting distinct nodes. `want` is at most n -
  # three, as a rule - so the nodes seen so far are a short list, and `in`
  # on it is cheaper than building a MapSet for every lookup.
  defp walk(_points, _idx, total, want, acc, count, steps) when count >= want or steps >= total,
    do: Enum.reverse(acc)

  defp walk(points, total, total, want, acc, count, steps),
    do: walk(points, 0, total, want, acc, count, steps)

  defp walk(points, idx, total, want, acc, count, steps) do
    {_point, node} = elem(points, idx)

    if node in acc,
      do: walk(points, idx + 1, total, want, acc, count, steps + 1),
      else: walk(points, idx + 1, total, want, [node | acc], count + 1, steps + 1)
  end
end
