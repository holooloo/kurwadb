"""kurwadb through psycopg 3: the extended protocol as a real driver speaks it.

    python test/drivers/psycopg_check.py "host=127.0.0.1 port=5499 user=u dbname=kurwadb"
"""
import sys
import psycopg

dsn = sys.argv[1]
checks = 0

def check(label, got, want):
    global checks
    if got != want:
        sys.exit(f"FAIL {label}: got {got!r}, want {want!r}")
    checks += 1

# Default: not autocommit, so psycopg sends BEGIN itself and expects COMMIT.
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO pyset (key) VALUES (%s), (%s)", ("p1", "p2"))
        check("insert rowcount", cur.rowcount, 2)
        cur.execute("SELECT key FROM pyset WHERE key = %s", ("p1",))
        check("member", cur.fetchall(), [("p1",)])
        cur.execute("SELECT key FROM pyset WHERE key = %s", ("nope",))
        check("absent", cur.fetchall(), [])
        cur.execute("SELECT EXISTS (SELECT 1 FROM pyset WHERE key = %s)", ("p2",))
        check("exists", cur.fetchone(), (True,))
    conn.commit()

with psycopg.connect(dsn, autocommit=True) as conn:
    cur = conn.cursor()
    cur.executemany("INSERT INTO pyset VALUES (%s)", [(f"m{i}",) for i in range(50)])
    cur.execute("SELECT count(*) FROM pyset WHERE key IN (%s, %s, %s)", ("m1", "m49", "zz"))
    check("count of members", cur.fetchone(), (2,))

    # A Python list goes over as one text[] parameter.
    cur.execute("SELECT key FROM pyset WHERE key = ANY(%s)", (["m1", "m2", "zz"],))
    check("any with a list", cur.fetchall(), [("m1",), ("m2",)])
    bcur = conn.cursor(binary=True)
    bcur.execute("SELECT key FROM pyset WHERE key = ANY(%s)", (["m3", "zz"],))
    check("any with a list, binary", bcur.fetchall(), [("m3",)])

    # psycopg prepares server-side after prepare_threshold (5) executions.
    for i in range(10):
        cur.execute("SELECT key FROM pyset WHERE key = %s", (f"m{i}",))
        check(f"prepared member {i}", cur.fetchone(), (f"m{i}",))

    cur.execute("SELECT key FROM pyset WHERE key = %s", ("p1",), prepare=True)
    check("explicit prepare", cur.fetchone(), ("p1",))

    # Binary results.
    bcur = conn.cursor(binary=True)
    bcur.execute("SELECT kurwa_member('pyset', %s), kurwa_ttl('pyset', %s)", ("p1", "p1"))
    check("binary bool and int8", bcur.fetchone(), (True, -1))

    cur.execute("INSERT INTO pyset (key, ttl) VALUES (%s, %s)", ("short", 3600))
    cur.execute("SELECT key, ttl FROM pyset WHERE key = %s", ("short",))
    key, ttl = cur.fetchone()
    check("ttl key", key, "short")
    check("ttl range", 3590 <= ttl <= 3600, True)

    cur.execute("DELETE FROM pyset WHERE key = %s", ("p1",))
    check("delete present", cur.rowcount, 1)
    cur.execute("DELETE FROM pyset WHERE key = %s", ("p1",))
    check("delete absent", cur.rowcount, 0)

    try:
        cur.execute("SELECT * FROM pyset")
        sys.exit("FAIL scan was allowed")
    except psycopg.errors.FeatureNotSupported as e:
        check("scan refused", "scan" in str(e), True)

    # The connection is still usable after an error.
    cur.execute("SELECT kurwa_member(%s, %s)", ("pyset", "p2"))
    check("after error", cur.fetchone(), (True,))

# An error inside a transaction block puts it in the failed state, as PostgreSQL does.
with psycopg.connect(dsn) as conn:
    cur = conn.cursor()
    try:
        cur.execute("SELECT nosuchfunction()")
    except psycopg.errors.UndefinedFunction:
        pass
    check("failed transaction", conn.info.transaction_status, psycopg.pq.TransactionStatus.INERROR)
    conn.rollback()
    cur.execute("SELECT 1")
    check("after rollback", cur.fetchone(), (1,))

print(f"psycopg: {checks} checks passed")
