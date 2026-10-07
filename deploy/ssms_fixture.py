"""Builds priv/mssql/ssms.json - the reference answers kurwadb's SQL Server
frontend gives to the catalog queries SQL Server Management Studio sends -
from sessions recorded against a real SQL Server with tds_record.py.

    python3 deploy/ssms_fixture.py recording.jsonl [more.jsonl ...]

Each request is keyed by its normalised text (lowercase, whitespace
collapsed): a SQL batch's text, or the statement of an sp_executesql. Its
answer is the real server's token stream, with exact column types. A `role`
marks the answers whose rows kurwadb makes from its own data (databases,
tables, schemas); the rest are replayed as recorded.
"""
import json, os, re, sys

sys.path.insert(0, os.path.dirname(__file__))
from tds_decode import exchanges, rpc_params  # noqa: E402

OUT = os.path.join(os.path.dirname(__file__), "..", "priv", "mssql", "ssms.json")


def normalise(sql):
    s = re.sub(r"\s+", " ", sql.lower()).strip()
    return re.sub(r"[;\s]+$", "", s)


def role(fp):
    if "from master.sys.databases as dtb" in fp:
        return "database_by_name" if "where (dtb.name=@_msparam_0)" in fp else "databases"
    if "from sys.tables as tbl" in fp:
        return "tables"
    if re.search(r"from sys\.schemas as s order by", fp):
        return "schemas"
    return None


def main(paths):
    templates = {}
    for path in paths:
        for e in exchanges(path):
            if e["kind"] == "attention" or any(t["t"] in ("decode_error", "unknown") for t in e["reply"]):
                continue
            if e["kind"] == "sql_batch":
                kind, text = "batch", e["sql"]
            else:
                name, params = rpc_params(bytes.fromhex(e["request_hex"]))
                if name != "sp_executesql" or not params:
                    continue
                kind, text = "executesql", params[0][1]
            # a leading USE is handled before the lookup, as it is for any batch
            text = re.sub(r"^\s*use\s+\[[^\]]+\]\s*;?", "", text, flags=re.I)
            fp = normalise(text)
            if not fp or fp in templates or fp.startswith("select 1") or fp == "select @@trancount":
                continue
            reply = [t for t in e["reply"] if t["t"] not in ("envchange", "loginack")]
            templates[fp] = {"kind": kind, "fp": fp, "role": role(fp), "reply": reply}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(sorted(templates.values(), key=lambda t: t["fp"]), f, separators=(",", ":"), default=str)
    print(f"{len(templates)} templates -> {os.path.normpath(OUT)}")
    for t in templates.values():
        if t["role"]:
            print(f"  {t['role']:17} {t['fp'][:100]}")


if __name__ == "__main__":
    main(sys.argv[1:])
