"""Decodes TDS token streams: the replies recorded by tds_record.py, and the
replies of a server the replay harness talks to. Shared by tds_replay.py.

    python3 deploy/tds_decode.py ssms.jsonl > reference.json

The output, per request: what the client sent (SQL text, or the RPC's name)
and what the server answered - result sets with each column's name and exact
type, rows, return status and values, errors, DONE counts.
"""
import json, struct, sys

FIXED = {0x30: ("tinyint", 1), 0x34: ("smallint", 2), 0x38: ("int", 4), 0x7F: ("bigint", 8),
         0x32: ("bit", 1), 0x3B: ("real", 4), 0x3E: ("float", 8), 0x3C: ("money", 8),
         0x3D: ("datetime", 8), 0x3A: ("smalldatetime", 4), 0x7A: ("smallmoney", 4), 0x1F: ("null", 0)}
BYTELEN = {0x24: "uniqueidentifier", 0x26: "intn", 0x68: "bitn", 0x6A: "decimal", 0x6C: "numeric",
           0x6D: "floatn", 0x6E: "moneyn", 0x6F: "datetimen", 0x28: "date", 0x29: "time",
           0x2A: "datetime2", 0x2B: "datetimeoffset", 0x2F: "char_legacy", 0x27: "varchar_legacy",
           0x2D: "binary_legacy", 0x25: "varbinary_legacy"}
USHORT = {0xA5: "varbinary", 0xA7: "varchar", 0xAD: "binary", 0xAF: "char", 0xE7: "nvarchar", 0xEF: "nchar"}
LONG = {0x23: "text", 0x63: "ntext", 0x22: "image"}
COLLATED = {0xA7, 0xAF, 0xE7, 0xEF, 0x23, 0x63}


class Reader:
    def __init__(self, data):
        self.d, self.i = data, 0

    def take(self, n):
        v = self.d[self.i:self.i + n]
        if len(v) < n:
            raise EOFError(f"wanted {n} at {self.i}, have {len(v)}")
        self.i += n
        return v

    def u8(self): return self.take(1)[0]
    def u16(self): return struct.unpack("<H", self.take(2))[0]
    def u32(self): return struct.unpack("<I", self.take(4))[0]
    def u64(self): return struct.unpack("<Q", self.take(8))[0]
    def bvarchar(self): return self.take(self.u8() * 2).decode("utf-16-le")
    def usvarchar(self): return self.take(self.u16() * 2).decode("utf-16-le")
    def left(self): return len(self.d) - self.i


def type_info(r, param=False):
    t = r.u8()
    info = {"code": t}
    if t in FIXED:
        info["type"], info["len"] = FIXED[t]
    elif t in BYTELEN:
        info["type"] = BYTELEN[t]
        if t in (0x29, 0x2A, 0x2B):
            info["scale"] = r.u8()
        elif t == 0x28:
            pass
        else:
            info["len"] = r.u8()
            if t in (0x6A, 0x6C):
                info["precision"], info["scale"] = r.u8(), r.u8()
            if t in (0x2F, 0x27):
                info["collation"] = r.take(5).hex()
    elif t in USHORT:
        info["type"] = USHORT[t]
        info["len"] = r.u16()
        if t in COLLATED:
            info["collation"] = r.take(5).hex()
    elif t in LONG:
        info["type"] = LONG[t]
        info["len"] = r.u32()
        if t in COLLATED:
            info["collation"] = r.take(5).hex()
        if not param:  # a column names its table; a parameter does not
            parts = r.u8()
            info["table"] = [r.usvarchar() for _ in range(parts)]
    elif t == 0x62:
        info["type"], info["len"] = "sql_variant", r.u32()
    elif t == 0xF1:
        info["type"] = "xml"
        if r.u8():
            info["schema"] = [r.bvarchar(), r.bvarchar(), r.usvarchar()]
    elif t == 0xF0:
        info["type"] = "udt"
        info["len"] = r.u16()
        info["udt"] = [r.bvarchar(), r.bvarchar(), r.bvarchar(), r.usvarchar()]
    else:
        raise ValueError(f"unknown type 0x{t:02x}")
    return info


def plp(r):
    total = r.u64()
    if total == 0xFFFFFFFFFFFFFFFF:
        return None
    out = b""
    while True:
        n = r.u32()
        if n == 0:
            return out
        out += r.take(n)


def value(r, info, param=False):
    t = info["code"]
    if t in FIXED:
        return fixed_value(t, r.take(info["len"]))
    if t in BYTELEN:
        if t == 0x28:
            n = r.u8()
            return None if n == 0 else {"date": r.take(n).hex()}
        n = r.u8()
        if n == 0 and t != 0x24 or (t == 0x24 and n == 0):
            return None
        raw = r.take(n)
        if t == 0x26:
            return int.from_bytes(raw, "little", signed=True)
        if t == 0x68:
            return bool(raw[0])
        if t == 0x6D:
            return struct.unpack("<f" if n == 4 else "<d", raw)[0]
        if t in (0x2F, 0x27):
            return raw.decode("latin-1")
        return {"hex": raw.hex()}
    if t in USHORT:
        if info["len"] == 0xFFFF:
            raw = plp(r)
        else:
            n = r.u16()
            raw = None if n == 0xFFFF else r.take(n)
        if raw is None:
            return None
        if t in (0xE7, 0xEF):
            return raw.decode("utf-16-le")
        if t in (0xA7, 0xAF):
            return raw.decode("latin-1")
        return {"hex": raw.hex()}
    if t in LONG and param:  # a parameter's text is length + data, no text pointer
        n = r.u32()
        if n == 0xFFFFFFFF:
            return None
        raw = r.take(n)
        return raw.decode("utf-16-le") if t == 0x63 else raw.decode("latin-1")
    if t in LONG:
        ptr = r.u8()
        if ptr == 0:
            return None
        r.take(ptr)
        r.take(8)
        raw = r.take(r.u32())
        return raw.decode("utf-16-le") if t == 0x63 else raw.decode("latin-1") if t == 0x23 else {"hex": raw.hex()}
    if t == 0x62:
        n = r.u32()
        if n == 0:
            return None
        raw = r.take(n)
        base, props = raw[0], raw[1]
        body = raw[2 + props:]
        propbytes = raw[2:2 + props].hex()
        if base in (0xE7, 0xEF):
            return {"variant": "nvarchar", "value": body.decode("utf-16-le"), "props": propbytes}
        if base in (0xA7, 0xAF):
            return {"variant": "varchar", "value": body.decode("latin-1"), "props": propbytes}
        if base == 0x38:
            return {"variant": "int", "value": int.from_bytes(body, "little", signed=True)}
        if base == 0x7F:
            return {"variant": "bigint", "value": int.from_bytes(body, "little", signed=True)}
        if base == 0x34:
            return {"variant": "smallint", "value": int.from_bytes(body, "little", signed=True)}
        if base == 0x30:
            return {"variant": "tinyint", "value": body[0]}
        if base == 0x32:
            return {"variant": "bit", "value": bool(body[0])}
        return {"variant": f"0x{base:02x}", "raw": raw.hex()}
    if t == 0xF1:
        return (plp(r) or b"").decode("utf-16-le")
    if t == 0xF0:
        raw = plp(r)
        return None if raw is None else {"hex": raw.hex()}
    raise ValueError(f"no value reader for 0x{t:02x}")


def fixed_value(t, raw):
    if t in (0x30,):
        return raw[0]
    if t in (0x34, 0x38, 0x7F):
        return int.from_bytes(raw, "little", signed=True)
    if t == 0x32:
        return bool(raw[0])
    if t == 0x3B:
        return struct.unpack("<f", raw)[0]
    if t == 0x3E:
        return struct.unpack("<d", raw)[0]
    return {"hex": raw.hex()}


def tokens(data, cek=False):
    """The token stream as a list of dicts. `cek`: the connection negotiated
    column encryption, so COLMETADATA carries a CEK table (SSMS asks for it)."""
    r = Reader(data)
    out, cols = [], None
    while r.left():
        tok = r.u8()
        if tok == 0x81:
            n = r.u16()
            if cek:
                for _ in range(r.u16()):
                    raise ValueError("CEK table entries are not decoded")
            if n == 0xFFFF:
                cols = []
                out.append({"t": "colmetadata", "columns": []})
                continue
            cols = []
            for _ in range(n):
                user, flags = r.u32(), r.u16()
                info = type_info(r)
                info["name"] = r.bvarchar()
                info["flags"] = flags
                info["usertype"] = user
                cols.append(info)
            out.append({"t": "colmetadata", "columns": cols})
        elif tok == 0xD1:
            out.append({"t": "row", "values": [value(r, c) for c in cols]})
        elif tok == 0xD2:
            bitmap = r.take((len(cols) + 7) // 8)
            vals = []
            for i, c in enumerate(cols):
                vals.append(None if bitmap[i // 8] & (1 << (i % 8)) else value(r, c))
            out.append({"t": "row", "values": vals})
        elif tok in (0xFD, 0xFE, 0xFF):
            status, cmd, count = r.u16(), r.u16(), r.u64()
            out.append({"t": {0xFD: "done", 0xFE: "doneproc", 0xFF: "doneinproc"}[tok], "status": status, "cmd": cmd, "count": count})
        elif tok in (0xAA, 0xAB):
            body = Reader(r.take(r.u16()))
            number, state, cls = body.u32(), body.u8(), body.u8()
            msg = body.usvarchar()
            out.append({"t": "error" if tok == 0xAA else "info", "number": number, "class": cls, "message": msg})
        elif tok == 0xE3:
            body = r.take(r.u16())
            out.append({"t": "envchange", "type": body[0]})
        elif tok == 0x79:
            out.append({"t": "returnstatus", "value": struct.unpack("<i", r.take(4))[0]})
        elif tok == 0xAC:
            ordinal = r.u16()
            name = r.bvarchar()
            status = r.u8()
            r.u32(); r.u16()
            info = type_info(r)
            out.append({"t": "returnvalue", "name": name, "type": info, "value": value(r, info)})
        elif tok == 0xA9:
            r.take(r.u16())
        elif tok == 0xA4:
            r.take(r.u16())
        elif tok == 0xA5:
            r.take(r.u16())
        elif tok == 0xAD:
            r.take(r.u16())
            out.append({"t": "loginack"})
        elif tok == 0xAE:
            while True:
                fid = r.u8()
                if fid == 0xFF:
                    break
                r.take(r.u32())
        elif tok == 0xE4:
            r.take(r.u16())
        else:
            out.append({"t": "unknown", "token": tok, "at": r.i})
            break
    return out


def rpc_params(payload):
    """(name, [(param name, value)]) of the first RPC in a payload."""
    r = Reader(payload)
    r.take(r.u32() - 4)
    n = r.u16()
    if n == 0xFFFF:
        name = {0x0A: "sp_executesql", 0x0B: "sp_prepare", 0x0C: "sp_execute", 0x0D: "sp_prepexec",
                0x0F: "sp_unprepare"}.get(r.u16(), "proc?")
    else:
        name = r.take(n * 2).decode("utf-16-le")
    r.u16()  # option flags
    params = []
    while r.left():
        if r.d[r.i] in (0x80, 0xFF, 0xFE):  # batch separator
            break
        pname = r.bvarchar()
        r.u8()  # status
        info = type_info(r, param=True)
        params.append((pname, value(r, info, param=True)))
    return name, params


def rpc_name(payload):
    r = Reader(payload)
    r.take(r.u32() - 4)  # ALL_HEADERS, its length counting itself
    n = r.u16()
    if n == 0xFFFF:
        return {0x0A: "sp_executesql", 0x0B: "sp_prepare", 0x0C: "sp_execute", 0x0D: "sp_prepexec",
                0x0F: "sp_unprepare", 1: "sp_cursor"}.get(r.u16(), "proc?")
    return r.take(n * 2).decode("utf-16-le")


def exchanges(path, cek=True):
    """(request, reply-tokens) pairs per connection, in order."""
    rows = [json.loads(l) for l in open(path)]
    pending, out = {}, []
    for row in rows:
        conn = row.get("conn")
        if row.get("type") in ("sql_batch", "rpc", "attention"):
            pending.setdefault(conn, []).append(row)
        elif row.get("dir") == "s2c" and row.get("type") == "reply" and pending.get(conn):
            req = pending[conn].pop(0)
            payload = bytes.fromhex(row["hex"])
            try:
                toks = tokens(payload, cek)
            except Exception as e:  # noqa: BLE001
                toks = [{"t": "decode_error", "error": str(e)}]
            out.append({"conn": conn, "kind": req["type"],
                        "sql": req.get("sql") if req["type"] == "sql_batch" else None,
                        "rpc": rpc_name(bytes.fromhex(req["hex"])) if req["type"] == "rpc" else None,
                        "request_hex": req["hex"], "reply": toks})
    return out


if __name__ == "__main__":
    json.dump(exchanges(sys.argv[1]), sys.stdout, indent=1, default=str)
