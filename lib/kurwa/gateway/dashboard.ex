defmodule Kurwa.Gateway.Dashboard do
  @moduledoc """
  The page at `/dashboard`: one self-contained HTML file, compiled into the
  module so a release needs nothing beside it. It polls `/dashboard/state`
  (`Kurwa.Metrics.cluster/0`) once a second and draws every node's frontends,
  coordinator, replica layer and shards, with requests moving between them.
  """

  @path Path.expand("../../../priv/dashboard/index.html", __DIR__)
  @external_resource @path
  @html File.read!(@path)

  def html, do: @html
end
