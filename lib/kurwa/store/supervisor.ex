defmodule Kurwa.Store.Supervisor do
  @moduledoc "Starts one `Kurwa.Store.Shard` per local shard."

  use Supervisor

  alias Kurwa.Config

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # One past the configured count is the system shard (see Kurwa.Store).
    children = for index <- 0..Config.shards(), do: {Kurwa.Store.Shard, index}

    # one_for_one: a shard that dies takes only its own partition down, and
    # recovers it from the WAL on restart.
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10)
  end
end
