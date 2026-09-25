defmodule Kurwa.Placement do
  @moduledoc """
  Where a key's replicas belong, and which of them we can reach right now.

  The distinction matters more than it looks. A ring built only from *reachable*
  nodes moves ownership every time a node blinks: keys shift to new replicas on
  the way down and shift back on the way up, and nobody is responsible for the
  writes the absent node missed. So the ring is built from every node we have ever
  verified (`Kurwa.Cluster` keeps them), and reachability is applied afterwards:

      primaries  the n nodes that own the key, stable across outages
      up         the primaries we can talk to - where the write actually goes
      down       the primaries we cannot, which is exactly the hint list for
                 `Kurwa.Handoff`

  A node is only dropped from the ring when an operator says so
  (`Kurwa.Cluster.forget/1`), because until then its data is still its
  responsibility.
  """

  alias Kurwa.Cluster
  alias Kurwa.Config
  alias Kurwa.Ring

  @type t :: %{primaries: [node()], up: [node()], down: [node()]}

  @doc "Replica placement for `key`, against the live ring."
  @spec targets(binary(), pos_integer() | nil) :: t()
  def targets(key, n \\ nil), do: targets(key, n, Cluster.ring(), Cluster.up())

  @doc "Replica placement against a given ring and reachable set."
  @spec targets(binary(), pos_integer() | nil, Ring.t(), MapSet.t(node())) :: t()
  def targets(key, n, ring, reachable) do
    primaries = Ring.preflist(ring, key, n || Config.n())
    {up, down} = Enum.split_with(primaries, &MapSet.member?(reachable, &1))

    %{primaries: primaries, up: up, down: down}
  end
end
