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
    cache: System.get_env("KURWA_CACHE") in ~w(1 true),
    wal_sync_on_write: System.get_env("KURWA_WAL_SYNC_ON_WRITE") in ~w(1 true),
    auth_token: System.get_env("KURWA_AUTH_TOKEN"),
    tombstone_ttl: int.("KURWA_TOMBSTONE_TTL_MS", 86_400_000),
    seeds: seeds
end
