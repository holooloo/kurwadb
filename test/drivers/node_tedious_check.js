// kurwadb through tedious, the Node.js TDS driver: sp_executesql with typed
// parameters, and prepared statements.
//
//     node test/drivers/node_tedious_check.js 14399 [password]
const { Connection, Request, TYPES } = require(process.env.NODE_TEDIOUS || "tedious");

const port = Number(process.argv[2] || 1433);
let checks = 0;
const check = (label, got, want) => {
  const g = JSON.stringify(got), w = JSON.stringify(want);
  if (g !== w) { console.error(`FAIL ${label}: got ${g}, want ${w}`); process.exit(1); }
  checks++;
};

const conn = new Connection({
  server: process.env.KURWA_HOST || "127.0.0.1",
  authentication: { type: "default", options: { userName: "sa", password: process.argv[3] || "x" } },
  options: { port, database: "kurwadb", encrypt: true, trustServerCertificate: true, rowCollectionOnDone: true },
});

const run = (sql, params = []) => new Promise((resolve, reject) => {
  const rows = [];
  const req = new Request(sql, (err, rowCount) => (err ? reject(err) : resolve({ rows, rowCount })));
  for (const [name, type, value] of params) req.addParameter(name, type, value);
  req.on("row", (cols) => rows.push(cols.map((c) => c.value)));
  conn.execSql(req);
});

conn.on("connect", async (err) => {
  if (err) { console.error("FAIL connect", err); process.exit(1); }
  try {
    let r = await run("INSERT INTO tset VALUES (@a), (@b)", [["a", TYPES.NVarChar, "t1"], ["b", TYPES.NVarChar, "t2"]]);
    check("insert", r.rowCount, 2);
    r = await run("SELECT [key] FROM tset WHERE [key] IN (@a, @b, @c)", [["a", TYPES.NVarChar, "t1"], ["b", TYPES.VarChar, "t2"], ["c", TYPES.NVarChar, "zz"]]);
    check("members", r.rows.sort(), [["t1"], ["t2"]]);
    r = await run("SELECT kurwa_member(@s, @k) AS m", [["s", TYPES.NVarChar, "tset"], ["k", TYPES.NVarChar, "t1"]]);
    check("bit", r.rows, [[true]]);
    r = await run("INSERT INTO tset (key, ttl) VALUES (@k, @ttl)", [["k", TYPES.NVarChar, "ttl"], ["ttl", TYPES.Int, 3600]]);
    check("int parameter as ttl", r.rowCount, 1);
    r = await run("IF NOT EXISTS (SELECT 1 FROM tset WHERE [key] = @k) INSERT INTO tset VALUES (@k)", [["k", TYPES.NVarChar, "t1"]]);
    check("if not exists, present", r.rowCount, 0);
    // prepare / execute / unprepare: sp_prepare and sp_execute.
    const prepared = new Request("SELECT [key] FROM tset WHERE [key] = @k", () => {});
    prepared.addParameter("k", TYPES.NVarChar);
    await new Promise((resolve, reject) => { prepared.on("prepared", resolve); prepared.on("error", reject); conn.prepare(prepared); });
    for (const [k, want] of [["t1", 1], ["zz", 0], ["t2", 1]]) {
      const got = await new Promise((resolve, reject) => {
        let n = 0;
        prepared.removeAllListeners("row");
        prepared.on("row", () => n++);
        prepared.removeAllListeners("requestCompleted");
        prepared.on("requestCompleted", () => resolve(n));
        prepared.removeAllListeners("error");
        prepared.on("error", reject);
        conn.execute(prepared, { k });
      });
      check(`prepared ${k}`, got, want);
    }
    await new Promise((resolve) => { prepared.removeAllListeners("requestCompleted"); prepared.on("requestCompleted", resolve); conn.unprepare(prepared); });

    // A stored procedure by name, with an OUTPUT parameter and a return code -
    // only when the node was started with KURWA_PROCEDURES_DIR holding consume.
    if (process.env.PROCEDURES) {
      const callConsume = (token) => new Promise((resolve, reject) => {
        const out = {};
        const req = new Request("dbo.consume", (err) => (err ? reject(err) : resolve(out)));
        req.addParameter("token", TYPES.NVarChar, token);
        req.addOutputParameter("taken", TYPES.Bit);
        req.on("returnValue", (name, value) => { out[name] = value; });
        req.on("doneProc", (_rowCount, _more, returnStatus) => { out.rc = returnStatus; });
        conn.callProcedure(req);
      });
      const token = "tok" + Date.now();
      check("procedure first call", await callConsume(token), { taken: true, rc: 0 });
      check("procedure second call", await callConsume(token), { taken: false, rc: 1 });
    }

    try { await run("SELECT [key] FROM tset"); console.error("FAIL scan"); process.exit(1); }
    catch (e) { check("scan refused", e.number, 50000); }
    console.log(`tedious: ${checks} checks passed`);
    conn.close();
  } catch (e) { console.error("FAIL", e); process.exit(1); }
});
conn.connect();
