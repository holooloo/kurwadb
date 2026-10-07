// kurwadb through node-postgres: parameters and named prepared statements.
//
//     node test/drivers/node_pg_check.js 5499
const { Client } = require(process.env.NODE_PG || "pg");

const port = Number(process.argv[2] || 5432);
let checks = 0;
const check = (label, got, want) => {
  const g = JSON.stringify(got), w = JSON.stringify(want);
  if (g !== w) { console.error(`FAIL ${label}: got ${g}, want ${w}`); process.exit(1); }
  checks++;
};

(async () => {
  // PGPASSWORD and PGSSL=1 exercise SCRAM and TLS: node-postgres has its own
  // SCRAM, separate from libpq's.
  const client = new Client({
    host: process.env.KURWA_HOST || "127.0.0.1", port, user: "node", database: "kurwadb",
    password: process.env.PGPASSWORD,
    ssl: process.env.PGSSL ? { rejectUnauthorized: false } : false,
  });
  await client.connect();

  let r = await client.query("INSERT INTO nodeset VALUES ($1), ($2)", ["n1", "n2"]);
  check("insert rowCount", r.rowCount, 2);

  r = await client.query("SELECT key FROM nodeset WHERE key IN ($1, $2, $3)", ["n1", "n2", "n3"]);
  check("members", r.rows, [{ key: "n1" }, { key: "n2" }]);

  r = await client.query("SELECT key FROM nodeset WHERE key = ANY($1)", [["n1", "n2", "nope"]]);
  check("any with an array", r.rows, [{ key: "n1" }, { key: "n2" }]);

  for (let i = 0; i < 5; i++) {
    r = await client.query({ name: "is-member", text: "SELECT key FROM nodeset WHERE key = $1", values: [i % 2 ? "n1" : "zz"] });
    check(`named prepared ${i}`, r.rows.length, i % 2 ? 1 : 0);
  }

  r = await client.query("SELECT kurwa_member($1, $2) AS m, kurwa_count() AS c", ["nodeset", "n2"]);
  check("bool param result", r.rows[0].m, true);
  check("int8 comes back as a string", typeof r.rows[0].c, "string");

  r = await client.query("DELETE FROM nodeset WHERE key = $1 RETURNING key", ["n2"]);
  check("delete returning", r.rows, [{ key: "n2" }]);

  try {
    await client.query("SELECT key FROM nodeset");
    console.error("FAIL scan allowed"); process.exit(1);
  } catch (e) {
    check("scan refused code", e.code, "0A000");
  }

  r = await client.query("SELECT 1 AS one");
  check("usable after error", r.rows, [{ one: 1 }]);

  await client.end();
  console.log(`node-postgres: ${checks} checks passed`);
})().catch((e) => { console.error("FAIL", e); process.exit(1); });
