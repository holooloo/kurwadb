defmodule Kurwa.Mongo.MongoshTest do
  @moduledoc """
  The real mongosh against the real server. Skipped by default:

      mix test --include mongo
  """
  use ExUnit.Case, async: false

  @moduletag :mongo

  setup_all do
    bin =
      System.find_executable("mongosh") ||
        raise "mongosh is not installed; run without --include mongo"

    {:ok, server} = ThousandIsland.start_link(port: 0, handler_module: Kurwa.Mongo.Server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, bin: bin, uri: "mongodb://127.0.0.1:#{port}/kurwadb"}
  end

  test "insert, find, a duplicate and a refused scan", %{bin: bin, uri: uri} do
    coll = "shtest#{System.unique_integer([:positive])}"

    script = """
    db.#{coll}.insertMany([{_id: "a"}, {_id: "b"}]);
    print("found", db.#{coll}.countDocuments({_id: {$in: ["a", "b", "c"]}}));
    try { db.#{coll}.insertOne({_id: "a"}) } catch (e) { print("dup", e.code) }
    try { db.#{coll}.find().toArray() } catch (e) { print("scan", e.code) }
    """

    {out, 0} = System.cmd(bin, [uri, "--quiet", "--eval", script], stderr_to_stdout: true)
    assert out =~ "found 2"
    assert out =~ "dup 11000"
    assert out =~ "scan 2"
  end
end
