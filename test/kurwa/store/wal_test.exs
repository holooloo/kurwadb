defmodule Kurwa.Store.WalTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Kurwa.TestHelpers, only: [tmp_dir: 1, record: 2]

  alias Kurwa.Store.Wal

  setup do
    {:ok, dir: tmp_dir("wal")}
  end

  test "replays what it appended, in order", %{dir: dir} do
    {:ok, wal} = Wal.open(dir)

    records = for i <- 1..50, do: record("k#{i}", lamport: i)

    wal =
      Enum.reduce(records, wal, fn rec, wal ->
        {:ok, wal} = Wal.append(wal, rec)
        wal
      end)

    :ok = Wal.close(wal)

    assert {:ok, replayed, 50} = Wal.replay(dir, [], fn rec, acc -> [rec | acc] end)
    assert Enum.reverse(replayed) == records
  end

  test "an empty directory replays to nothing", %{dir: dir} do
    assert {:ok, [], 0} = Wal.replay(dir, [], fn rec, acc -> [rec | acc] end)
  end

  test "reopening appends instead of truncating", %{dir: dir} do
    {:ok, wal} = Wal.open(dir)
    {:ok, wal} = Wal.append(wal, record("a", lamport: 1))
    :ok = Wal.close(wal)

    {:ok, wal} = Wal.open(dir)
    {:ok, wal} = Wal.append(wal, record("b", lamport: 2))
    :ok = Wal.close(wal)

    assert {:ok, keys, 2} = Wal.replay(dir, [], fn {key, _, _, _, _, _}, acc -> acc ++ [key] end)
    assert keys == ["a", "b"]
  end

  test "compaction keeps the data and empties the log", %{dir: dir} do
    {:ok, wal} = Wal.open(dir)
    records = for i <- 1..10, do: record("k#{i}", lamport: i)

    wal =
      Enum.reduce(records, wal, fn rec, wal ->
        {:ok, wal} = Wal.append(wal, rec)
        wal
      end)

    assert wal.appended == 10
    assert {:ok, wal} = Wal.compact(wal, records)
    assert wal.appended == 0
    :ok = Wal.close(wal)

    assert File.exists?(Path.join(dir, "snapshot"))
    refute File.exists?(Path.join(dir, "snapshot.tmp"))

    # Everything now comes from the snapshot, so the WAL count is 0 and a fresh
    # snapshot is not due again.
    assert {:ok, replayed, 0} = Wal.replay(dir, [], fn rec, acc -> [rec | acc] end)
    assert Enum.sort(replayed) == Enum.sort(records)
  end

  test "snapshot and log are replayed together, log last", %{dir: dir} do
    {:ok, wal} = Wal.open(dir)
    {:ok, wal} = Wal.compact(wal, [record("k", lamport: 1)])
    {:ok, wal} = Wal.append(wal, record("k", lamport: 2))
    :ok = Wal.close(wal)

    assert {:ok, seen, 1} = Wal.replay(dir, [], fn rec, acc -> acc ++ [rec] end)
    assert Enum.map(seen, fn {_, lamport, _, _, _, _} -> lamport end) == [1, 2]
  end

  test "a torn tail ends the replay without losing earlier entries", %{dir: dir} do
    {:ok, wal} = Wal.open(dir)
    {:ok, wal} = Wal.append(wal, record("good", lamport: 1))
    :ok = Wal.close(wal)

    # simulate a write cut short by a crash
    path = Path.join(dir, "wal")
    File.write!(path, <<0, 0, 1, 44, 99, 99>>, [:append])

    log =
      capture_log(fn ->
        assert {:ok, [{"good", 1, _, _, _, _}], 1} =
                 Wal.replay(dir, [], fn rec, acc -> [rec | acc] end)
      end)

    assert log =~ "trailing bytes"
  end

  test "a corrupt entry stops the replay at that point", %{dir: dir} do
    {:ok, wal} = Wal.open(dir)
    {:ok, wal} = Wal.append(wal, record("good", lamport: 1))
    :ok = Wal.close(wal)

    path = Path.join(dir, "wal")
    payload = :erlang.term_to_binary(record("bad", lamport: 2))
    bogus_crc = 12_345
    File.write!(path, <<byte_size(payload)::32, bogus_crc::32, payload::binary>>, [:append])
    {:ok, wal} = Wal.open(dir)
    {:ok, wal} = Wal.append(wal, record("after", lamport: 3))
    :ok = Wal.close(wal)

    log =
      capture_log(fn ->
        assert {:ok, keys, 1} =
                 Wal.replay(dir, [], fn {key, _, _, _, _, _}, acc -> acc ++ [key] end)

        assert keys == ["good"]
      end)

    assert log =~ "corrupt entry"
  end

  test "a bogus length is treated as corruption, not as a huge allocation", %{dir: dir} do
    {:ok, wal} = Wal.open(dir)
    :ok = Wal.close(wal)

    File.write!(Path.join(dir, "wal"), <<0xFFFFFFFF::32, 0::32>>, [:append])

    log = capture_log(fn -> assert {:ok, [], 0} = Wal.replay(dir, [], fn r, a -> [r | a] end) end)
    assert log =~ "corrupt entry"
  end

  test "refuses a file that is not a kurwadb log", %{dir: dir} do
    File.write!(Path.join(dir, "wal"), "not a wal at all")

    assert {:error, {:bad_magic, _}} = Wal.replay(dir, [], fn r, a -> [r | a] end)
  end
end
