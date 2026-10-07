"""kurwadb through Python's SQL Server drivers: pymssql (FreeTDS) always, and
pyodbc with Microsoft's ODBC Driver 18 when it is installed.

    python test/drivers/mssql_py_check.py 14399 [password]
"""
import os, sys
HOST = os.environ.get("KURWA_HOST", "127.0.0.1")

port = int(sys.argv[1])
password = sys.argv[2] if len(sys.argv) > 2 else "x"
checks = 0

def check(label, got, want):
    global checks
    if got != want:
        sys.exit(f"FAIL {label}: got {got!r}, want {want!r}")
    checks += 1

def exercise(name, conn, param):
    cur = conn.cursor()
    p = param
    cur.execute(f"INSERT INTO pyset VALUES ({p}), ({p})", ("a", "b"))
    check(f"{name} insert rowcount", cur.rowcount, 2)
    cur.execute(f"SELECT [key] FROM pyset WHERE [key] IN ({p}, {p}, {p})", ("a", "b", "zz"))
    check(f"{name} members", sorted(r[0] for r in cur.fetchall()), ["a", "b"])
    cur.execute(f"IF NOT EXISTS (SELECT 1 FROM pyset WHERE [key] = {p}) INSERT INTO pyset VALUES ({p})", ("c", "c"))
    check(f"{name} if not exists, new", cur.rowcount, 1)
    cur.execute(f"IF NOT EXISTS (SELECT 1 FROM pyset WHERE [key] = {p}) INSERT INTO pyset VALUES ({p})", ("c", "c"))
    check(f"{name} if not exists, again", cur.rowcount, 0)
    cur.execute(f"DELETE FROM pyset WHERE [key] = {p}", ("c",))
    check(f"{name} delete", cur.rowcount, 1)
    cur.execute("SELECT kurwa_member('pyset', 'a') AS m, kurwa_ttl('pyset', 'a') AS t")
    check(f"{name} bit and bigint", tuple(cur.fetchone()), (True, -1))
    cur.execute("SELECT DB_NAME(), @@TRANCOUNT")
    check(f"{name} db_name", cur.fetchone()[0], "kurwadb")
    try:
        cur.execute("SELECT [key] FROM pyset")
        sys.exit(f"FAIL {name} scan allowed")
    except Exception as e:
        check(f"{name} scan refused", "scan" in str(e), True)
    cur.execute("SELECT 1")
    check(f"{name} usable after error", cur.fetchone()[0], 1)

import pymssql
conn = pymssql.connect(server=HOST, port=port, user="sa", password=password, database="kurwadb", autocommit=True)
exercise("pymssql", conn, "%s")

# A stored procedure by name, when the node has the consume procedure loaded.
import os, time
if os.environ.get("PROCEDURES"):
    cur = conn.cursor()
    token = f"py{time.time_ns()}"
    # FreeTDS hands back callproc's OUTPUT values a call late, so pymssql
    # code reads them the usual way: EXEC with an OUTPUT variable in a batch.
    sql = "DECLARE @t BIT, @rc INT; EXEC @rc = dbo.consume %s, @t OUTPUT; SELECT @t, @rc"
    cur.execute(sql, (token,))
    check("pymssql exec procedure first", tuple(cur.fetchone()), (True, 0))
    cur.execute(sql, (token,))
    check("pymssql exec procedure second", tuple(cur.fetchone()), (False, 1))
conn.close()

try:
    import pyodbc
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
except ImportError:
    drivers = []

if drivers:
    for encrypt in ("yes", "no", "strict"):
        dsn = (f"DRIVER={{{drivers[-1]}}};SERVER={HOST},{port};DATABASE=kurwadb;UID=sa;PWD={password};"
               f"Encrypt={encrypt};TrustServerCertificate=yes")
        try:
            conn = pyodbc.connect(dsn, autocommit=False)
        except pyodbc.Error as e:
            if encrypt == "strict":
                print(f"  (odbc strict needs a trusted certificate: {str(e)[:80]})")
                continue
            raise
        exercise(f"odbc encrypt={encrypt}", conn, "?")
        conn.commit()
        conn.rollback()
        conn.close()
else:
    print("  (no ODBC driver for SQL Server installed; pyodbc skipped)")

print(f"mssql python drivers: {checks} checks passed")
