defmodule Kurwa.Mssql.SsmsTest do
  # Every recorded SSMS catalog query has an answer that encodes; the full
  # comparison with the real server is deploy/tds_replay.py.
  use ExUnit.Case, async: true

  alias Kurwa.Mssql.Ssms

  test "the recorded templates load" do
    assert Ssms.count() > 50
  end

  test "SSMS's connect batch is answered, whitespace and case aside" do
    sql = """
    DECLARE @edition sysname;
    SET   @edition = cast(SERVERPROPERTY(N'EDITION') as sysname);
    SELECT case when @edition = N'SQL Azure' then 2 else 1 end as 'DatabaseEngineType',
    SERVERPROPERTY('EngineEdition') AS DatabaseEngineEdition,
    SERVERPROPERTY('ProductVersion') AS ProductVersion,
    @@MICROSOFTVERSION AS MicrosoftVersion;
    select host_platform from sys.dm_os_host_info
    if @edition = N'SQL Azure'
      select 'TCP' as ConnectionProtocol
    else
      exec ('select CONVERT(nvarchar(40),CONNECTIONPROPERTY(''net_transport'')) as ConnectionProtocol')
    """

    assert {:ok, tokens} = Ssms.answer(sql, %{}, "master")
    binary = IO.iodata_to_binary(tokens)
    assert :binary.match(binary, Kurwa.Mssql.Tds.ucs2("DatabaseEngineType")) != :nomatch
  end

  test "the database list has kurwadb, and a database by name answers for it" do
    sql =
      "SELECT dtb.name AS [Name], dtb.database_id AS [ID], CAST(has_dbaccess(dtb.name) AS bit) AS [IsAccessible] FROM master.sys.databases AS dtb ORDER BY [Name] ASC"

    assert {:ok, tokens} = Ssms.answer(sql, %{}, "master")
    assert :binary.match(IO.iodata_to_binary(tokens), Kurwa.Mssql.Tds.ucs2("kurwadb")) != :nomatch

    by_name =
      "SELECT dtb.collation_name AS [Collation], dtb.name AS [DatabaseName2] FROM master.sys.databases AS dtb WHERE (dtb.name=@_msparam_0)"

    {:ok, tokens} = Ssms.answer(by_name, %{"_msparam_0" => "kurwadb"}, "master")
    assert :binary.match(IO.iodata_to_binary(tokens), Kurwa.Mssql.Tds.ucs2("kurwadb")) != :nomatch

    {:ok, tokens} = Ssms.answer(by_name, %{"_msparam_0" => "nosuchdb"}, "master")

    refute :binary.match(IO.iodata_to_binary(tokens), Kurwa.Mssql.Tds.ucs2("nosuchdb")) !=
             :nomatch
  end

  test "unknown SQL is not a template" do
    assert Ssms.answer("SELECT key FROM seen WHERE key = 'a'", %{}, "kurwadb") == :nomatch
  end
end
