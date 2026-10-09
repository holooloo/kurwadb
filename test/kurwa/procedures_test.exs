defmodule Kurwa.ProceduresTest do
  use ExUnit.Case, async: false

  import Kurwa.TestHelpers, only: [tmp_dir: 1]

  alias Kurwa.Procedures
  alias Kurwa.Sql.Procedural

  setup do
    original = Application.get_env(:kurwadb, :procedures_dir)

    on_exit(fn ->
      Kurwa.Config.put(:procedures_dir, original)
      Procedures.reload()
    end)

    {:ok, dir: tmp_dir("procedures")}
  end

  describe "parsing" do
    test "parameters: types, defaults, OUTPUT, with or without parentheses" do
      {:ok, proc} =
        Procedural.parse_procedure("""
        CREATE OR ALTER PROCEDURE dbo.p
          @a NVARCHAR(200), @b INT = 5, @c BIT OUTPUT
        AS BEGIN RETURN 0 END
        """)

      assert proc.name == "p"

      assert [
               %{name: "a", type: :text, output: false},
               %{name: "b", type: :int, default: {:default, {:lit, 5}}},
               %{name: "c", type: :bit, output: true}
             ] = proc.params

      assert {:ok, %{name: "q", params: [%{name: "x"}]}} =
               Procedural.parse_procedure("CREATE PROC q (@x INT) AS SELECT @x")
    end

    test "what is refused says what it is" do
      for {body, words} <- [
            {"WHILE 1 = 1 PRINT 'x'", "WHILE"},
            {"DECLARE @t TABLE (k INT)", "tables or cursors"},
            {"BEGIN TRY PRINT 'x' END TRY", "TRY/CATCH"},
            {"SET @v = CASE WHEN 1 = 1 THEN 1 END", "CASE"}
          ] do
        assert {:error, _, message} =
                 Procedural.parse_procedure("CREATE PROCEDURE p @v INT AS #{body}")

        assert message =~ words, "#{body}: #{message}"
      end
    end

    test "IF NOT EXISTS ... INSERT on the same key stays one atomic statement" do
      {:ok, proc} =
        Procedural.parse_procedure("""
        CREATE PROCEDURE p @k NVARCHAR(10) AS
        IF NOT EXISTS (SELECT 1 FROM t WHERE [key] = @k) INSERT INTO t VALUES (@k)
        """)

      assert [{:sql, {:insert, "t", nil, [[{:param, "k"}]], nil, :nothing}}] = proc.body
    end
  end

  describe "the directory" do
    test "GO separates procedures, and script settings between them are skipped", %{dir: dir} do
      File.write!(Path.join(dir, "a.sql"), """
      SET ANSI_NULLS ON
      GO
      SET QUOTED_IDENTIFIER ON
      GO
      CREATE PROCEDURE dbo.one AS RETURN 1
      GO
      create procedure two as return 2
      go
      """)

      Kurwa.Config.put(:procedures_dir, dir)
      assert {:ok, 2} = Procedures.reload()
      assert Procedures.names() == ["one", "two"]
      assert %{name: "one"} = Procedures.lookup("DBO.One")
      hash = Procedures.hash()
      assert {:ok, 2} = Procedures.reload()
      assert Procedures.hash() == hash
    end

    test "a broken file is named, and what was loaded stays", %{dir: dir} do
      File.write!(Path.join(dir, "good.sql"), "CREATE PROCEDURE good AS RETURN 0")
      Kurwa.Config.put(:procedures_dir, dir)
      assert {:ok, 1} = Procedures.reload()

      File.write!(Path.join(dir, "bad.sql"), "CREATE PROCEDURE bad AS WHILE 1 = 1 PRINT 'x'")
      assert {:error, message} = Procedures.reload()
      assert message =~ "bad.sql"
      assert Procedures.names() == ["good"]
    end

    test "the examples in the repository load" do
      Kurwa.Config.put(:procedures_dir, Path.expand("examples/procedures"))
      assert {:ok, 3} = Procedures.reload()
      assert Procedures.names() == ["consume", "issue", "issue_and_consume"]
    end

    test "the same name twice is an error", %{dir: dir} do
      File.write!(
        Path.join(dir, "a.sql"),
        "CREATE PROCEDURE p AS RETURN 0\nGO\nCREATE PROCEDURE p AS RETURN 1"
      )

      Kurwa.Config.put(:procedures_dir, dir)
      assert {:error, message} = Procedures.reload()
      assert message =~ "defined twice"
    end
  end

  describe "running" do
    setup do
      :ok =
        Procedures.put_sources([
          """
          CREATE PROCEDURE classify @n INT, @label NVARCHAR(10) OUTPUT AS
          BEGIN
            IF @n IS NULL SET @label = 'none'
            ELSE IF @n > 10 AND NOT @n IN (99) SET @label = 'big'
            ELSE SET @label = 'small'
            RETURN @n + 1
          END
          """,
          "CREATE PROCEDURE outer_one @n INT AS BEGIN DECLARE @l NVARCHAR(10), @rc INT; EXEC @rc = classify @n, @l OUTPUT; SELECT @l AS label, @rc AS rc END",
          "CREATE PROCEDURE deep @n INT AS EXEC deep @n",
          "CREATE PROCEDURE fails AS BEGIN THROW 50001, N'no', 1; PRINT 'never' END"
        ])
    end

    defp ctx,
      do: %{
        session: %{user: "t", database: "kurwadb", pid: 1, settings: %{}, sysvars: %{}},
        procedure: &Procedures.lookup/1,
        observe: fn _, _, s -> s end
      }

    test "conditions, OUTPUT and the return code" do
      proc = Procedures.lookup("classify")

      for {n, label, rc} <- [
            {nil, "none", 0},
            {50, "big", 51},
            {99, "small", 100},
            {3, "small", 4}
          ] do
        {_events, code, [{"label", :text, got}], _} =
          Procedural.call(
            proc,
            [%{param: nil, value: n, output: false}, %{param: nil, value: nil, output: true}],
            ctx()
          )

        assert {got, code} == {label, rc}, "n = #{inspect(n)}"
      end
    end

    test "EXEC passes OUTPUT back into the caller's variable" do
      {events, 0, [], _} =
        Procedural.call(
          Procedures.lookup("outer_one"),
          [%{param: "n", value: 12, output: false}],
          ctx()
        )

      assert [{:rows, _, [["big", 13]], _}] =
               for({:result, _, {:rows, _, _, _} = r, 0} <- events, do: r)
    end

    test "recursion stops at SQL Server's nesting limit" do
      {events, nil, _, _} =
        Procedural.call(
          Procedures.lookup("deep"),
          [%{param: nil, value: 1, output: false}],
          ctx()
        )

      assert {:error, 217, message} = List.last(events)
      assert message =~ "32"
    end

    test "THROW stops the procedure with its number" do
      {events, nil, _, _} = Procedural.call(Procedures.lookup("fails"), [], ctx())
      assert events == [{:error, 50_001, "no"}]
    end

    test "a missing parameter is SQL Server's error 201" do
      {[{:error, 201, message}], nil, _, _} =
        Procedural.call(Procedures.lookup("classify"), [], ctx())

      assert message =~ "@n"
    end
  end
end
