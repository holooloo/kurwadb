defmodule Kurwa.Application do
  @moduledoc false

  use Application

  alias Kurwa.Config

  require Logger

  @impl true
  def start(_type, _args) do
    Config.validate!()
    Kurwa.Clock.init()

    children =
      [
        Kurwa.Store.Supervisor,
        Kurwa.Cluster
      ] ++ gateway()

    Logger.info(
      "kurwadb: starting on #{node()} (n=#{Config.n()} r=#{Config.r()} w=#{Config.w()} " <>
        "shards=#{Config.shards()})"
    )

    Supervisor.start_link(children, strategy: :one_for_one, name: Kurwa.Supervisor)
  end

  defp gateway do
    if Config.get(:start_gateway) do
      port = Config.get(:http_port)
      [{Bandit, plug: Kurwa.Gateway.Router, scheme: :http, port: port}]
    else
      []
    end
  end
end
