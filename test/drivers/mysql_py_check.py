"""kurwadb through PyMySQL and MySQL Connector/Python, text and binary protocol.

    python test/drivers/mysql_py_check.py 3399 [password]
"""
import os, sys
HOST = os.environ.get("KURWA_HOST", "127.0.0.1")
import pymysql
import mysql.connector

port = int(sys.argv[1])
password = sys.argv[2] if len(sys.argv) > 2 else ""
checks = 0

def check(label, got, want):
    global checks
    if got != want:
        sys.exit(f"FAIL {label}: got {got!r}, want {want!r}")
    checks += 1

# PyMySQL: the text protocol, parameters interpolated client-side.
conn = pymysql.connect(host=HOST, port=port, user="py", password=password, database="kurwadb", autocommit=True)
with conn.cursor() as cur:
    check("insert", cur.execute("INSERT INTO myset VALUES (%s), (%s)", ("a", "b")), 2)
    cur.execute("SELECT `key` FROM myset WHERE `key` IN (%s, %s, %s)", ("a", "b", "zz"))
    check("members", cur.fetchall(), (("a",), ("b",)))
    check("insert ignore", cur.execute("INSERT IGNORE INTO myset VALUES (%s), (%s)", ("a", "c")), 1)
    cur.execute("SELECT kurwa_member('myset', %s), kurwa_ttl('myset', %s)", ("c", "c"))
    check("functions", cur.fetchone(), (1, -1))
    check("delete present", cur.execute("DELETE FROM myset WHERE `key` = %s", ("c",)), 1)
    check("delete absent", cur.execute("DELETE FROM myset WHERE `key` = %s", ("c",)), 0)
    try:
        cur.execute("SELECT * FROM myset")
        sys.exit("FAIL scan allowed")
    except pymysql.err.NotSupportedError as e:
        check("scan refused", e.args[0], 1235)
    cur.execute("SELECT 1")
    check("usable after error", cur.fetchone(), (1,))
conn.close()

# Not autocommit: PyMySQL sends SET AUTOCOMMIT = 0, and commit/rollback.
conn = pymysql.connect(host=HOST, port=port, user="py", password=password, database="kurwadb")
with conn.cursor() as cur:
    cur.execute("INSERT INTO myset VALUES (%s)", ("t",))
conn.commit()
conn.rollback()
conn.close()

# Connector/Python, pure and with prepared statements: the binary protocol.
for pure in (True, False):
    try:
        cnx = mysql.connector.connect(host=HOST, port=port, user="py", password=password,
                                      database="kurwadb", use_pure=pure, autocommit=True)
    except Exception as e:
        if not pure:
            print(f"  (connector C extension unavailable: {e})")
            continue
        raise
    label = "pure" if pure else "c-ext"
    cur = cnx.cursor(prepared=True)
    cur.execute("INSERT INTO myset VALUES (?)", ("p1",))
    check(f"{label} prepared insert", cur.rowcount, 1)
    for k in ("p1", "a", "zz"):
        cur.execute("SELECT `key` FROM myset WHERE `key` = ?", (k,))
        rows = [tuple(x.decode() if isinstance(x, (bytes, bytearray)) else x for x in r) for r in cur.fetchall()]
        check(f"{label} prepared member {k}", rows, [] if k == "zz" else [(k,)])
    cur.execute("SELECT kurwa_member(?, ?), kurwa_count()", ("myset", "a"))
    member, count = cur.fetchone()
    check(f"{label} binary tinyint", member, 1)
    check(f"{label} binary bigint", isinstance(count, int), True)
    cur.close()
    cur = cnx.cursor()
    cur.execute("SELECT @@version_comment, DATABASE()")
    check(f"{label} sysvar", cur.fetchone()[1], "kurwadb")
    cnx.close()

print(f"mysql python drivers: {checks} checks passed")
