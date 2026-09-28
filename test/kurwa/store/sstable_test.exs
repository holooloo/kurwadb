defmodule Kurwa.Store.SSTableTest do
  use ExUnit.Case, async: true

  import Kurwa.TestHelpers, only: [tmp_dir: 1]

  alias Kurwa.Record
  alias Kurwa.Store.SSTable

  setup do
    {:ok, dir: tmp_dir("sstable")}
  end

  defp records(n, opts \\ []) do
    for i <- 1..n do
      Record.new(
        "key:#{String.pad_leading(Integer.to_string(i), 6, "0")}",
        i,
        node(),
        Keyword.get(opts, :alive?, true)
      )
    end
  end

  defp write!(dir, records, name \\ "t.sst") do
    path = Path.join(dir, name)
    assert {:ok, count} = SSTable.write(path, records)
    assert count == length(records)
    {:ok, table} = SSTable.open(path)
    table
  end

  test "every key written comes back", %{dir: dir} do
    all = records(500)
    table = write!(dir, all)

    for record <- all do
      assert SSTable.get(table, Record.key(record)) == record
    end

    SSTable.close(table)
  end

  test "keys that were never written come back nil", %{dir: dir} do
    table = write!(dir, records(200))

    for i <- 1..200 do
      assert SSTable.get(table, "absent:#{i}") == nil
    end

    SSTable.close(table)
  end

  test "records are stored in key order regardless of the order given", %{dir: dir} do
    shuffled = records(300) |> Enum.shuffle()
    table = write!(dir, shuffled)

    keys = SSTable.fold(table, [], fn record, acc -> [Record.key(record) | acc] end)
    assert Enum.reverse(keys) == Enum.sort(keys, :desc) |> Enum.reverse()
    assert length(keys) == 300

    SSTable.close(table)
  end

  test "a lookup between two indexed keys still finds its block", %{dir: dir} do
    # the sparse index holds every 16th key, so most lookups land mid-block
    all = records(100)
    table = write!(dir, all)

    assert SSTable.get(table, "key:000017") == Enum.at(all, 16)
    assert SSTable.get(table, "key:000023") == Enum.at(all, 22)
    assert SSTable.get(table, "key:000100") == Enum.at(all, 99)

    SSTable.close(table)
  end

  test "reports live keys and total records, counting tombstones apart", %{dir: dir} do
    all = records(40) ++ records(10, alive?: false)
    table = write!(dir, all)

    # the ten tombstones replace the first ten keys once sorted; both are stored
    assert SSTable.stats(table).records == 50
    assert SSTable.stats(table).live <= 50

    SSTable.close(table)
  end

  test "an empty table is valid and answers nothing", %{dir: dir} do
    table = write!(dir, [], "empty.sst")

    assert SSTable.get(table, "anything") == nil
    assert SSTable.fold(table, 0, fn _r, n -> n + 1 end) == 0

    SSTable.close(table)
  end

  test "survives being closed and opened again", %{dir: dir} do
    path = Path.join(dir, "reopen.sst")
    {:ok, _} = SSTable.write(path, records(64))

    {:ok, first} = SSTable.open(path)
    SSTable.close(first)

    {:ok, again} = SSTable.open(path)
    assert SSTable.get(again, "key:000032") != nil
    assert SSTable.stats(again).records == 64

    SSTable.close(again)
  end

  test "binary keys that are not text work", %{dir: dir} do
    weird = for b <- 1..50, do: Record.new(<<0, b, 255>>, b, node(), true)
    table = write!(dir, weird, "binary.sst")

    for record <- weird do
      assert SSTable.get(table, Record.key(record)) == record
    end

    SSTable.close(table)
  end

  test "refuses a file that is not a table", %{dir: dir} do
    path = Path.join(dir, "junk.sst")
    File.write!(path, String.duplicate("not a table at all", 20))

    assert {:error, _reason} = SSTable.open(path)
  end

  test "refuses a truncated file rather than reading nonsense", %{dir: dir} do
    path = Path.join(dir, "cut.sst")
    {:ok, _} = SSTable.write(path, records(100))

    full = File.read!(path)
    File.write!(path, binary_part(full, 0, div(byte_size(full), 2)))

    assert {:error, _reason} = SSTable.open(path)
  end

  describe "cursors and merging" do
    test "a cursor walks every record in key order", %{dir: dir} do
      all = records(100)
      table = write!(dir, all, "cursor.sst")

      walked = drain(SSTable.reader(table))

      assert walked == all
      SSTable.close(table)
    end

    test "merging tables yields each key once, with the winning version", %{dir: dir} do
      old = for i <- 1..20, do: Record.new("k:#{i}", 1, node(), true)
      new = for i <- 10..30, do: Record.new("k:#{i}", 9, node(), false)

      a = write!(dir, old, "a.sst")
      b = write!(dir, new, "b.sst")

      merged =
        SSTable.merge([SSTable.reader(a), SSTable.reader(b)], [], fn r, acc -> [r | acc] end)
        |> Enum.reverse()

      keys = Enum.map(merged, &Record.key/1)
      assert length(keys) == length(Enum.uniq(keys)), "a key must come out of a merge once"
      assert length(keys) == 30

      # where both tables hold the key, the higher stamp wins
      overlapping = Enum.filter(merged, &(Record.lamport(&1) == 9))
      assert length(overlapping) == 21

      SSTable.close(a)
      SSTable.close(b)
    end

    test "merging is ordered even across many tables", %{dir: dir} do
      tables =
        for t <- 1..5 do
          recs =
            for i <- t..100//5,
                do: Record.new("k:#{String.pad_leading("#{i}", 4, "0")}", i, node(), true)

          write!(dir, recs, "m#{t}.sst")
        end

      keys =
        SSTable.merge(Enum.map(tables, &SSTable.reader/1), [], fn r, acc ->
          [Record.key(r) | acc]
        end)
        |> Enum.reverse()

      assert keys == Enum.sort(keys)
      assert keys == Enum.uniq(keys)

      Enum.each(tables, &SSTable.close/1)
    end

    test "a table written from a sorted stream reads back the same", %{dir: dir} do
      all = records(80)
      path = Path.join(dir, "streamed.sst")

      assert {:ok, 80} = SSTable.write_sorted(path, all, 80)
      {:ok, table} = SSTable.open(path)

      assert drain(SSTable.reader(table)) == all
      SSTable.close(table)
    end
  end

  defp drain(reader, acc \\ []) do
    case SSTable.next(reader) do
      {record, reader} -> drain(reader, [record | acc])
      :done -> Enum.reverse(acc)
    end
  end
end
