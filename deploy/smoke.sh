#!/usr/bin/env bash
# Every driver check in test/drivers, against a running container over the
# network, from a machine that has the clients (see README, "Clients").
#
#     KURWA_PASSWORD=... deploy/smoke.sh 10.10.10.112
#
# Ports default to the ones in .env.example; override with the same names.
# PYTHON and NODE_PATH point at an environment with the drivers installed.
set -euo pipefail
cd "$(dirname "$0")/.."

host=${1:?usage: smoke.sh HOST}
pw=${KURWA_PASSWORD:?set KURWA_PASSWORD}
py=${PYTHON:-python3}
export KURWA_HOST=$host KURWA_PASSWORD=$pw PGPASSWORD=$pw PROCEDURES=1

mssql=${KURWA_HOST_MSSQL:-1433}
pg=${KURWA_HOST_PG:-25432}
mysql=${KURWA_HOST_MYSQL:-23306}
resp=${KURWA_HOST_RESP:-26379}
mongo=${KURWA_HOST_MONGO:-27117}
http=${KURWA_HOST_HTTP:-24040}

run() { echo "== $1"; shift; "$@"; }

run "http /info" curl -fsS -H "authorization: Bearer $pw" "http://$host:$http/info"; echo
run "pymssql + pyodbc" "$py" test/drivers/mssql_py_check.py "$mssql" "$pw"
run "tedious" node test/drivers/node_tedious_check.js "$mssql" "$pw"
run "psycopg" "$py" test/drivers/psycopg_check.py "host=$host port=$pg user=u password=$pw dbname=kurwadb"
run "node-postgres" node test/drivers/node_pg_check.js "$pg"
run "pymysql + mysql-connector" "$py" test/drivers/mysql_py_check.py "$mysql" "$pw"
run "mysql2" node test/drivers/node_mysql2_check.js "$mysql" "$pw"
run "redis-py" "$py" test/drivers/redis_py_check.py "$resp"
run "pymongo" "$py" test/drivers/pymongo_check.py "mongodb://u:$pw@$host:$mongo/?authSource=admin"
run "node mongodb" node test/drivers/node_mongodb_check.js "mongodb://u:$pw@$host:$mongo/?authSource=admin"
if [ -n "${PGJDBC:-}" ]; then
  # PGJDBC: postgresql.jar; JdbcCheck.class compiled next to it (see the file)
  run "pgjdbc" "${JAVA:-java}" -cp "$PGJDBC:$(dirname "$PGJDBC")" JdbcCheck "$host:$pg"
fi
echo "all driver checks passed against $host"
