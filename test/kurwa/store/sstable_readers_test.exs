defmodule Kurwa.Store.SSTable.ReadersTest do
  use ExUnit.Case, async: true

  import Kurwa.TestHelpers, only: [tmp_dir: 1]

  alias Kurwa.Record
  alias Kurwa.Store.SSTable
  alias Kurwa.Store.SSTable.{Fd, Readers}

  setup do
    {:ok, dir: tmp_dir("sstable_readers")}
  end

  defp table!(dir) do
    records = for i <- 1..200, do: Record.new("key:#{1000 + i}", i, node(), true)
    path = Path.join(dir, "t.sst")
    {:ok, 200} = SSTable.write(path, records)
    {:ok, table} = SSTable.open(path)
    {table, records}
  end

  defp in_fresh_process(fun) do
    task = Task.async(fun)
    Task.await(task)
  end

  test "the application runs the pool" do
    assert Readers.running?()
  end

  # 0.8.0 cached a raw handle in whichever process read, and every replica read
  # runs in a fresh one - so each read opened and closed the file. 28.7 µs
  # instead of 3. A read must leave no handle behind in the process that asked.
  test "a read from a short-lived process opens nothing in it", %{dir: dir} do
    {table, records} = table!(dir)
    record = Enum.random(records)

    {found, held} =
      in_fresh_process(fn ->
        found = SSTable.get(table, Record.key(record))
        {found, Fd.count()}
      end)

    assert found == record
    assert held == 0

    SSTable.close(table)
  end

  test "many short-lived readers all get the right record", %{dir: dir} do
    {table, records} = table!(dir)

    results =
      records
      |> Enum.map(fn record -> Task.async(fn -> SSTable.get(table, Record.key(record)) end) end)
      |> Task.await_many()

    assert results == records
    SSTable.close(table)
  end

  test "a table that was closed is released in the readers and still readable after reopen",
       %{dir: dir} do
    {table, records} = table!(dir)
    record = hd(records)

    assert in_fresh_process(fn -> SSTable.get(table, Record.key(record)) end) == record
    SSTable.close(table)

    {:ok, reopened} = SSTable.open(table.path)
    assert in_fresh_process(fn -> SSTable.get(reopened, Record.key(record)) end) == record
    SSTable.close(reopened)
  end
end
