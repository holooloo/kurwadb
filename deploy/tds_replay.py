"""Replays a recorded client session (tds_record.py) against kurwadb and
compares each answer with the real SQL Server's: errors, result sets, and
every column's name and type. Rows are reported, not compared - kurwadb's
databases are not the reference server's.

    python3 deploy/tds_replay.py ssms.jsonl HOST PORT [PASSWORD] [--show N]

Each distinct request is sent once, in the order first seen. Exit status 0
when every request is answered without an error and with the reference's
shape.
"""
import json, socket, struct, sys
from collections import OrderedDict

sys.path.insert(0, __import__("os").path.dirname(__file__))
from tds_decode import exchanges, tokens  # noqa: E402


def packet(kind, payload, size=4096):
    out, chunk = b"", size - 8
    parts = [payload[i:i + chunk] for i in range(0, len(payload), chunk)] or [b""]
    for i, part in enumerate(parts):
        last = i == len(parts) - 1
        out += struct.pack(">BBHHBB", kind, 1 if last else 0, len(part) + 8, 0, i + 1, 0) + part
    return out


def recv_message(s):
    body = b""
    while True:
        head = recv_exact(s, 8)
        kind, status, length = head[0], head[1], struct.unpack(">H", head[2:4])[0]
        body += recv_exact(s, length - 8)
        if status & 1:
            return kind, body


def recv_exact(s, n):
    out = b""
    while len(out) < n:
        chunk = s.recv(n - len(out))
        if not chunk:
            raise ConnectionError("closed")
        out += chunk
    return out


def ucs2(text):
    return text.encode("utf-16-le")


def login7(user, password, app):
    scrambled = bytes(((b << 4 & 0xF0 | b >> 4) ^ 0xA5) for b in ucs2(password))
    fields = [ucs2("replay-host"), ucs2(user), scrambled, ucs2(app), ucs2(""), b"", ucs2(""), ucs2(""), ucs2("kurwadb")]
    offset, offsets, data = 94, b"", b""
    for i, f in enumerate(fields):
        offsets += struct.pack("<HH", offset, len(f) // 2)
        data += f
        offset += len(f)
        if i == 8:
            offsets += b"\x00" * 6 + struct.pack("<HHHHHHI", offset, 0, offset, 0, offset, 0, 0)
    body = offsets + data
    head = struct.pack("<IIIIII", 36 + len(body), 0x74000004, 4096, 7, 0, 0) + bytes([0xE0, 0x03, 0, 0]) + struct.pack("<II", 0, 0x409)
    return head + body


def connect(host, port, password):
    s = socket.create_connection((host, port), timeout=10)
    prelogin = bytes([0x00, 0, 11, 0, 6, 0x01, 0, 17, 0, 1, 0xFF, 16, 0, 0, 0, 0, 0, 0x02])
    s.sendall(packet(0x12, prelogin))
    recv_message(s)
    s.sendall(packet(0x10, login7("sa", password, "Microsoft SQL Server Management Studio")))
    _, reply = recv_message(s)
    toks = tokens(reply)
    if not any(t["t"] == "loginack" for t in toks):
        raise SystemExit(f"login failed: {toks}")
    return s


# Types a client reads the same way: a NOT NULL int and a nullable one.
SAME = {"intn": "int", "bitn": "bit", "floatn": "float"}


def shape(reply):
    sets, errors = [], []
    for t in reply:
        if t["t"] == "colmetadata":
            sets.append({"columns": [(c["name"], SAME.get(c.get("type"), c.get("type")), c.get("len"), c.get("precision"), c.get("scale")) for c in t["columns"]], "rows": 0})
        elif t["t"] == "row" and sets:
            sets[-1]["rows"] += 1
        elif t["t"] == "error":
            errors.append(f'{t["number"]}: {t["message"]}')
    return sets, errors


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    show = int(sys.argv[sys.argv.index("--show") + 1]) if "--show" in sys.argv else 0
    path, host, port = args[0], args[1], int(args[2])
    password = args[3] if len(args) > 3 else "x"

    distinct = OrderedDict()
    for e in exchanges(path):
        if e["kind"] == "attention" or any(t["t"] in ("decode_error", "unknown") for t in e["reply"]):
            continue
        distinct.setdefault(e["request_hex"], e)

    s = connect(host, port, password)
    ok = bad = 0
    for i, e in enumerate(distinct.values()):
        payload = bytes.fromhex(e["request_hex"])
        try:
            s.sendall(packet(0x01 if e["kind"] == "sql_batch" else 0x03, payload))
            _, reply = recv_message(s)
            mine = tokens(reply)
        except Exception as ex:  # noqa: BLE001
            print(f"#{i} CONNECTION {ex}")
            s = connect(host, port, password)
            bad += 1
            continue
        ref_sets, _ = shape(e["reply"])
        my_sets, my_errors = shape(mine)
        problems = list(my_errors)
        if len(ref_sets) != len(my_sets):
            problems.append(f"result sets: reference {len(ref_sets)}, kurwadb {len(my_sets)}")
        for k, (a, b) in enumerate(zip(ref_sets, my_sets)):
            if a["columns"] != b["columns"]:
                problems.append(f"set {k} columns differ:\n      ref  {a['columns']}\n      mine {b['columns']}")
        label = (e["sql"] or e["rpc"] or "").strip().replace("\r", "").replace("\n", " ")[:110]
        if problems:
            bad += 1
            print(f"#{i} FAIL {label}")
            for p in problems[:4]:
                print("    " + p)
        else:
            ok += 1
            rows = ", ".join(f'{a["rows"]}->{b["rows"]}' for a, b in zip(ref_sets, my_sets))
            print(f"#{i} ok   {label}   rows {rows}")
        if show and i + 1 >= show:
            break
    print(f"\n{ok} ok, {bad} failing, of {len(distinct)} distinct requests")
    sys.exit(0 if bad == 0 else 1)


if __name__ == "__main__":
    main()
