"""kurwadb through PyMongo.

    python test/drivers/pymongo_check.py "mongodb://127.0.0.1:27099/"
"""
import sys
import datetime
from bson import ObjectId
import pymongo
from pymongo.errors import DuplicateKeyError, OperationFailure, BulkWriteError

uri = sys.argv[1]
checks = 0

def check(label, got, want):
    global checks
    if got != want:
        sys.exit(f"FAIL {label}: got {got!r}, want {want!r}")
    checks += 1

client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
check("ping", client.admin.command("ping")["ok"], 1.0)
db = client.kurwadb
seen = db["pyseen"]
# What an earlier run left behind: the store keeps it.
seen.delete_many({"_id": {"$in": ["a", "b", "c", "d", "e", "f", "job:1", "ttl"]}})

check("insert_many", len(seen.insert_many([{"_id": "a"}, {"_id": "b"}]).inserted_ids), 2)
check("find $in", list(seen.find({"_id": {"$in": ["a", "zz"]}})), [{"_id": "a"}])
check("find_one", seen.find_one({"_id": "b"}), {"_id": "b"})
check("find_one absent", seen.find_one({"_id": "nope"}), None)
check("count_documents", seen.count_documents({"_id": {"$in": ["a", "b", "c"]}}), 2)

try:
    seen.insert_one({"_id": "a"})
    sys.exit("FAIL duplicate accepted")
except DuplicateKeyError as e:
    check("E11000", e.code, 11000)

# insert_many ordered stops at the duplicate; unordered goes on.
try:
    seen.insert_many([{"_id": "c"}, {"_id": "a"}, {"_id": "d"}])
except BulkWriteError as e:
    check("ordered stops", e.details["nInserted"], 1)
check("d not inserted", seen.find_one({"_id": "d"}), None)
try:
    seen.insert_many([{"_id": "a"}, {"_id": "e"}], ordered=False)
except BulkWriteError as e:
    check("unordered continues", e.details["nInserted"], 1)

# The dedup idiom.
r = seen.update_one({"_id": "job:1"}, {"$setOnInsert": {}}, upsert=True)
check("upsert new", (r.upserted_id, r.matched_count), ("job:1", 0))
r = seen.update_one({"_id": "job:1"}, {"$setOnInsert": {}}, upsert=True)
check("upsert existing", (r.upserted_id, r.matched_count), (None, 1))

check("delete_one", seen.delete_one({"_id": "a"}).deleted_count, 1)
check("delete_one again", seen.delete_one({"_id": "a"}).deleted_count, 0)
check("delete_many", seen.delete_many({"_id": {"$in": ["b", "c", "zz"]}}).deleted_count, 2)

oid = ObjectId()
seen.insert_one({"_id": oid})
check("ObjectId _id", seen.find_one({"_id": oid}), {"_id": oid})

soon = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
seen.insert_one({"_id": "ttl", "expireAt": soon})
check("expireAt accepted", seen.find_one({"_id": "ttl"}), {"_id": "ttl"})

for bad, label in (({}, "empty filter"), ({"name": "x"}, "other field")):
    try:
        list(seen.find(bad))
        sys.exit(f"FAIL {label} allowed")
    except OperationFailure as e:
        check(label, "_id" in str(e) or "scan" in str(e), True)

try:
    seen.insert_one({"_id": "f", "name": "x"})
    sys.exit("FAIL fields accepted")
except OperationFailure as e:
    check("fields refused", "keys only" in str(e), True)

check("collections", "pyseen" in db.list_collection_names(), True)
check("indexes", [i["name"] for i in seen.list_indexes()], ["_id_"])
check("server version", client.server_info()["version"], "7.0.0")

# A set made through SQL or Redis is the same collection.
print(f"pymongo: {checks} checks passed")
