// One load-generating process for bench/mongo.sh: `concurrency` findOne calls
// in flight on one MongoClient for `seconds`, then prints how many completed.
const { MongoClient } = require(process.env.NODE_MONGODB || "mongodb");
const [uri, seconds, concurrency, kind, keys] = process.argv.slice(2);

(async () => {
  const client = new MongoClient(uri, { maxPoolSize: Number(concurrency) });
  await client.connect();
  const coll = client.db("kurwadb").collection("kurwa");
  let done = 0;
  const until = Date.now() + Number(seconds) * 1000;
  const worker = async () => {
    while (Date.now() < until) {
      const k = kind === "hit" ? String(1 + Math.floor(Math.random() * Number(keys))) : "absent:" + done;
      await coll.findOne({ _id: k });
      done++;
    }
  };
  await Promise.all(Array.from({ length: Number(concurrency) }, worker));
  await client.close();
  process.stdout.write(String(done) + "\n");
})().catch((e) => { console.error(e); process.exit(1); });
