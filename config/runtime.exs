import Config

# Evaluated at boot, not at compile time - this is where a release picks up its
# environment.
if config_env() != :test do
  get = fn name, default -> System.get_env(name) || default end

  int = fn name, default ->
    (System.get_env(name) || default) |> to_string() |> String.to_integer()
  end

  seeds =
    System.get_env("KURWA_SEEDS", "")
    |> String.split(",", trim: true)
    |> Enum.map(&String.to_atom(String.trim(&1)))

  config :kurwadb,
    n: int.("KURWA_N", 3),
    r: int.("KURWA_R", 2),
    w: int.("KURWA_W", 2),
    strict_quorum: System.get_env("KURWA_STRICT_QUORUM") in ~w(1 true),
    shards: int.("KURWA_SHARDS", 8),
    vnodes: int.("KURWA_VNODES", 128),
    data_dir: get.("KURWA_DATA_DIR", "data"),
    http_port: int.("KURWA_HTTP_PORT", 4040),
    start_9p: System.get_env("KURWA_9P") in ~w(1 true),
    ninep_port: int.("KURWA_9P_PORT", 564),
    start_pg: System.get_env("KURWA_PG") in ~w(1 true),
    pg_port: int.("KURWA_PG_PORT", 5432),
    pg_auth:
      (case System.get_env("KURWA_PG_AUTH") do
         "md5" -> :md5
         "password" -> :password
         _ -> :scram
       end),
    pg_tls:
      (case {System.get_env("KURWA_PG_TLS_CERT"), System.get_env("KURWA_PG_TLS_KEY")} do
         {cert, key} when is_binary(cert) and is_binary(key) -> [certfile: cert, keyfile: key]
         _ -> nil
       end),
    start_resp: System.get_env("KURWA_RESP") in ~w(1 true),
    resp_port: int.("KURWA_RESP_PORT", 6379),
    start_mysql: System.get_env("KURWA_MYSQL") in ~w(1 true),
    mysql_port: int.("KURWA_MYSQL_PORT", 3306),
    start_mongo: System.get_env("KURWA_MONGO") in ~w(1 true),
    mongo_port: int.("KURWA_MONGO_PORT", 27017),
    start_mssql: System.get_env("KURWA_MSSQL") in ~w(1 true),
    procedures_dir: System.get_env("KURWA_PROCEDURES_DIR"),
    mssql_port: int.("KURWA_MSSQL_PORT", 1433),
    mssql_tls:
      (case {System.get_env("KURWA_MSSQL_TLS_CERT"), System.get_env("KURWA_MSSQL_TLS_KEY")} do
         {cert, key} when is_binary(cert) and is_binary(key) -> [certfile: cert, keyfile: key]
         _ -> nil
       end),
    cache: System.get_env("KURWA_CACHE") in ~w(1 true),
    wal_sync_on_write: System.get_env("KURWA_WAL_SYNC_ON_WRITE") in ~w(1 true),
    engine:
      if(System.get_env("KURWA_ENGINE") == "lsm", do: Kurwa.Store.Lsm, else: Kurwa.Store.Ets),
    auth_token: System.get_env("KURWA_AUTH_TOKEN"),
    tombstone_ttl: int.("KURWA_TOMBSTONE_TTL_MS", 86_400_000),
    seeds: seeds,
    # Where clients reach this node from outside, when that is not its own
    # address and ports (a container's published ports): shown on the
    # dashboard. KURWA_PUBLIC_PORTS is "pg=25432,mssql=1433,...".
    public_host: System.get_env("KURWA_PUBLIC_HOST"),
    public_ports:
      System.get_env("KURWA_PUBLIC_PORTS", "")
      |> String.split(",", trim: true)
      |> Map.new(fn pair ->
        [name, port] = String.split(pair, "=", parts: 2)
        {String.trim(name), String.to_integer(String.trim(port))}
      end)

  # debug logs every query the SQL frontends receive.
  if level = System.get_env("KURWA_LOG_LEVEL") do
    config :logger, level: String.to_existing_atom(level)
  end
end
