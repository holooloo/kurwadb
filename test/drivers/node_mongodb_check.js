// kurwadb through the official MongoDB Node.js driver.
//
//     node test/drivers/node_mongodb_check.js mongodb://127.0.0.1:27099/
const { MongoClient, ObjectId } = require(process.env.NODE_MONGODB || "mongodb");

let checks = 0;
const check = (label, got, want) => {
  const g = JSON.stringify(got), w = JSON.stringify(want);
  if (g !== w) { console.error(`FAIL ${label}: got ${g}, want ${w}`); process.exit(1); }
  checks++;
};

(async () => {
  const client = new MongoClient(process.argv[2], { serverSelectionTimeoutMS: 5000 });
  await client.connect();
  const coll = client.db("kurwadb").collection("nodeseen");

  let r = await coll.insertMany([{ _id: "n1" }, { _id: "n2" }]);
  check("insertMany", r.insertedCount, 2);
  check("find $in", await coll.find({ _id: { $in: ["n1", "zz"] } }).toArray(), [{ _id: "n1" }]);
  check("countDocuments", await coll.countDocuments({ _id: { $in: ["n1", "n2", "n3"] } }), 2);

  try { await coll.insertOne({ _id: "n1" }); console.error("FAIL dup"); process.exit(1); }
  catch (e) { check("E11000", e.code, 11000); }

  r = await coll.updateOne({ _id: "job" }, { $setOnInsert: {} }, { upsert: true });
  check("upsert", r.upsertedCount, 1);

  check("deleteOne", (await coll.deleteOne({ _id: "n2" })).deletedCount, 1);

  const id = new ObjectId();
  await coll.insertOne({ _id: id });
  check("ObjectId", String((await coll.findOne({ _id: id }))._id), String(id));

  try { await coll.find({}).toArray(); console.error("FAIL scan"); process.exit(1); }
  catch (e) { check("scan refused", e.code, 2); }

  await client.close();
  console.log(`mongodb (node): ${checks} checks passed`);
})().catch((e) => { console.error("FAIL", e); process.exit(1); });
