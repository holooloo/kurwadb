"""kurwadb through redis-py: the client most Python code already uses.

    python test/drivers/redis_py_check.py 6399
"""
import os, sys
HOST = os.environ.get("KURWA_HOST", "127.0.0.1")
import redis

port = int(sys.argv[1])
checks = 0

def check(label, got, want):
    global checks
    if got != want:
        sys.exit(f"FAIL {label}: got {got!r}, want {want!r}")
    checks += 1

for protocol in (2, 3):
    r = redis.Redis(host=HOST, port=port, password=os.environ.get("KURWA_PASSWORD"), protocol=protocol, decode_responses=True)
    p = f"[resp{protocol}] "
    key, s = f"py:{protocol}:once", f"py:{protocol}:seen"
    r.delete(key)
    # What an earlier run left behind: the store keeps it.
    r.srem(s, "a", "b", "x", *[f"m{i}" for i in range(100)])
    r.delete(f"py:{protocol}:watched", f"py:{protocol}:watched:done")

    # The idempotency pattern: only the first SET NX wins.
    check(p + "first set nx", r.set(key, 1, nx=True, ex=60), True)
    check(p + "second set nx", r.set(key, 1, nx=True, ex=60), None)
    check(p + "exists", r.exists(key), 1)
    check(p + "ttl", 55 <= r.ttl(key) <= 60, True)
    check(p + "delete", r.delete(key), 1)

    check(p + "sadd", r.sadd(s, "a", "b"), 2)
    check(p + "sismember", r.sismember(s, "a"), True if protocol == 3 else 1)
    check(p + "smismember", r.smismember(s, ["a", "z"]), [1, 0])
    check(p + "srem", r.srem(s, "a"), 1)

    # pipeline() wraps its batch in MULTI/EXEC by default.
    pipe = r.pipeline()
    pipe.sadd(s, "x").sismember(s, "x").exists(key)
    check(p + "pipeline", pipe.execute(), [1, 1 if protocol == 2 else True, 0])

    pipe = r.pipeline(transaction=False)
    for i in range(100):
        pipe.sadd(s, f"m{i}")
    check(p + "plain pipeline", sum(pipe.execute()), 100)

    # WATCH, as applications use it: optimistic, with WatchError on a change.
    other = redis.Redis(host=HOST, port=port, password=os.environ.get("KURWA_PASSWORD"), protocol=protocol, decode_responses=True)
    w = f"py:{protocol}:watched"
    r.set(w, 1)
    with r.pipeline() as pipe:
        pipe.watch(w)
        other.set(w, 2)          # another client moves the key
        pipe.multi()
        pipe.set(w + ":done", 1)
        try:
            pipe.execute()
            sys.exit("FAIL exec ran after the watched key changed")
        except redis.WatchError:
            check(p + "watch conflict", r.exists(w + ":done"), 0)
    with r.pipeline() as pipe:
        pipe.watch(w)
        pipe.multi()
        pipe.set(w + ":done", 1)
        check(p + "watch clean", pipe.execute(), [True])

    try:
        r.smembers(s)
        sys.exit("FAIL smembers allowed")
    except redis.ResponseError as e:
        check(p + "scan refused", "no scans" in str(e), True)

    try:
        r.get(key)
        sys.exit("FAIL get allowed")
    except redis.ResponseError as e:
        check(p + "get refused", "without values" in str(e), True)

print(f"redis-py: {checks} checks passed")
