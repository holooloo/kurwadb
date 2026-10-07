defmodule Kurwa.Application do
  @moduledoc false

  use Application

  alias Kurwa.Config

  require Logger

  @impl true
  def start(_type, _args) do
    Config.validate!()
    Kurwa.Clock.init()
    Kurwa.Procedures.load!()

    children =
      [
        {Task.Supervisor, name: Kurwa.TaskSupervisor},
        Kurwa.Metrics,
        Kurwa.Store.SSTable.Readers,
        Kurwa.Store.Supervisor,
        Kurwa.Replica.Endpoint,
        Kurwa.Registry,
        Kurwa.Handoff,
        Kurwa.Cluster,
        Kurwa.Repair,
        Kurwa.Extractor.Cache,
        Kurwa.Extractor.Flight
      ] ++ gateway() ++ ninep() ++ pg() ++ resp() ++ mysql() ++ mongo() ++ mssql()

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

  # Off by default: the registered 9P port needs privileges to bind, and not
  # every deployment wants a second frontend.
  defp mssql do
    if Config.get(:start_mssql) do
      port = Config.get(:mssql_port)

      [
        Supervisor.child_spec(
          {ThousandIsland, port: port, handler_module: Kurwa.Mssql.Server},
          id: :kurwa_mssql
        )
      ]
    else
      []
    end
  end

  defp mongo do
    if Config.get(:start_mongo) do
      port = Config.get(:mongo_port)

      [
        Supervisor.child_spec(
          {ThousandIsland, port: port, handler_module: Kurwa.Mongo.Server},
          id: :kurwa_mongo
        )
      ]
    else
      []
    end
  end

  defp mysql do
    if Config.get(:start_mysql) do
      port = Config.get(:mysql_port)

      [
        Supervisor.child_spec(
          {ThousandIsland, port: port, handler_module: Kurwa.Mysql.Server},
          id: :kurwa_mysql
        )
      ]
    else
      []
    end
  end

  defp resp do
    if Config.get(:start_resp) do
      port = Config.get(:resp_port)

      [
        Supervisor.child_spec(
          {ThousandIsland, port: port, handler_module: Kurwa.Resp.Server},
          id: :kurwa_resp
        )
      ]
    else
      []
    end
  end

  defp pg do
    if Config.get(:start_pg) do
      port = Config.get(:pg_port)

      [
        Supervisor.child_spec(
          {ThousandIsland, port: port, handler_module: Kurwa.Pg.Server},
          id: :kurwa_pg
        )
      ]
    else
      []
    end
  end

  defp ninep do
    if Config.get(:start_9p) do
      port = Config.get(:ninep_port)

      [
        Supervisor.child_spec(
          {ThousandIsland, port: port, handler_module: Kurwa.NineP.Server},
          id: :kurwa_9p
        )
      ]
    else
      []
    end
  end
end
