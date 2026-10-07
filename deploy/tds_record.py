"""A recording TDS proxy: what a client (SSMS) says to a real SQL Server, and
what the server answers, message by message, as JSON lines.

    python3 deploy/tds_record.py LISTEN_PORT UPSTREAM_HOST UPSTREAM_PORT LOG

It turns encryption off in PRELOGIN both ways, so everything after it is
readable: connect with Encrypt = Optional. The log is the reference for what
kurwadb's SQL Server frontend has to answer and with which column types.
"""
import json, socket, sys, threading, time

listen_port, up_host, up_port, log_path = int(sys.argv[1]), sys.argv[2], int(sys.argv[3]), sys.argv[4]
lock = threading.Lock()
log = open(log_path, "a", buffering=1)
TYPES = {0x01: "sql_batch", 0x03: "rpc", 0x04: "reply", 0x06: "attention", 0x0E: "tm", 0x10: "login7", 0x12: "prelogin"}


def no_encryption(payload):
    """Sets the ENCRYPTION option of a PRELOGIN payload to NOT_SUP (2)."""
    data = bytearray(payload)
    i = 0
    while i < len(data) and data[i] != 0xFF:
        token, offset, length = data[i], int.from_bytes(data[i + 1:i + 3], "big"), int.from_bytes(data[i + 3:i + 5], "big")
        if token == 0x01 and length >= 1:
            data[offset] = 0x02
        i += 5
    return bytes(data)


def record(conn_id, direction, mtype, payload):
    entry = {"t": round(time.time(), 3), "conn": conn_id, "dir": direction, "type": TYPES.get(mtype, mtype), "hex": payload.hex()}
    if mtype == 0x01 and len(payload) >= 4:
        headers = int.from_bytes(payload[0:4], "little")
        entry["sql"] = payload[headers:].decode("utf-16-le", "replace")
    if mtype == 0x10:
        entry["hex"] = "(login7 omitted)"
    with lock:
        log.write(json.dumps(entry) + "\n")


def messages(sock):
    """Yields (type, status_of_last, payload, raw_packets) per TDS message."""
    buf = b""
    payload, raw = b"", b""
    while True:
        while len(buf) < 8:
            chunk = sock.recv(65536)
            if not chunk:
                return
            buf += chunk
        length = int.from_bytes(buf[2:4], "big")
        while len(buf) < length:
            chunk = sock.recv(65536)
            if not chunk:
                return
            buf += chunk
        packet, buf = buf[:length], buf[length:]
        payload += packet[8:]
        raw += packet
        if packet[1] & 0x01:
            yield packet[0], payload, raw
            payload, raw = b"", b""


def pump(conn_id, src, dst, direction):
    try:
        for mtype, payload, raw in messages(src):
            if mtype == 0x12 or (direction == "s2c" and mtype == 0x04 and payload[:1] == b"\x00" and len(payload) < 64):
                patched = no_encryption(payload)
                if patched != payload:
                    record(conn_id, direction, mtype, payload)
                    header = bytearray(raw[:8])
                    header[1] |= 0x01
                    header[2:4] = (len(patched) + 8).to_bytes(2, "big")
                    dst.sendall(bytes(header) + patched)
                    continue
            record(conn_id, direction, mtype, payload)
            dst.sendall(raw)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def main():
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", listen_port))
    server.listen(64)
    n = 0
    while True:
        client, addr = server.accept()
        n += 1
        upstream = socket.create_connection((up_host, up_port))
        with lock:
            log.write(json.dumps({"t": round(time.time(), 3), "conn": n, "open": addr[0]}) + "\n")
        threading.Thread(target=pump, args=(n, client, upstream, "c2s"), daemon=True).start()
        threading.Thread(target=pump, args=(n, upstream, client, "s2c"), daemon=True).start()


main()
