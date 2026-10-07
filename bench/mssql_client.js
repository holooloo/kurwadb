// One load-generating process for bench/mssql.sh: `connections` tedious
// connections, each running sp_executesql lookups back to back for `seconds`.
const { Connection, Request, TYPES } = require(process.env.NODE_TEDIOUS || "tedious");
const [port, seconds, connections, kind, keys] = process.argv.slice(2);

const open = () => new Promise((resolve, reject) => {
  const c = new Connection({
    server: "127.0.0.1",
    authentication: { type: "default", options: { userName: "bench", password: "x" } },
    options: { port: Number(port), database: "kurwadb", encrypt: process.env.ENCRYPT !== "false", trustServerCertificate: true },
  });
  c.on("connect", (err) => (err ? reject(err) : resolve(c)));
  c.connect();
});

const lookup = (c, k) => new Promise((resolve, reject) => {
  const r = new Request("SELECT [key] FROM kurwa WHERE [key] = @k", (err) => (err ? reject(err) : resolve()));
  r.addParameter("k", TYPES.NVarChar, k);
  c.execSql(r);
});

(async () => {
  const conns = await Promise.all(Array.from({ length: Number(connections) }, open));
  let done = 0;
  const until = Date.now() + Number(seconds) * 1000;
  await Promise.all(conns.map(async (c) => {
    while (Date.now() < until) {
      const k = kind === "hit" ? String(1 + Math.floor(Math.random() * Number(keys))) : "absent:" + done;
      await lookup(c, k);
      done++;
    }
  }));
  conns.forEach((c) => c.close());
  process.stdout.write(String(done) + "\n");
})().catch((e) => { console.error(e); process.exit(1); });
