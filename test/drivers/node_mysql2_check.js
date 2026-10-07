// kurwadb through mysql2: query() is the text protocol, execute() prepares.
//
//     node test/drivers/node_mysql2_check.js 3399 [password]
const mysql = require(process.env.NODE_MYSQL2 || "mysql2/promise");

const port = Number(process.argv[2] || 3306);
let checks = 0;
const check = (label, got, want) => {
  const g = JSON.stringify(got), w = JSON.stringify(want);
  if (g !== w) { console.error(`FAIL ${label}: got ${g}, want ${w}`); process.exit(1); }
  checks++;
};

(async () => {
  const conn = await mysql.createConnection({
    host: process.env.KURWA_HOST || "127.0.0.1", port, user: "node", password: process.argv[3] || "", database: "kurwadb",
  });

  let [r] = await conn.query("INSERT INTO nset VALUES (?), (?)", ["n1", "n2"]);
  check("insert affectedRows", r.affectedRows, 2);

  let [rows] = await conn.query("SELECT `key` FROM nset WHERE `key` IN (?)", [["n1", "n2", "zz"]]);
  check("members (text)", rows, [{ key: "n1" }, { key: "n2" }]);

  for (const k of ["n1", "zz", "n2"]) {
    [rows] = await conn.execute("SELECT `key` FROM nset WHERE `key` = ?", [k]);
    check(`execute ${k}`, rows.length, k === "zz" ? 0 : 1);
  }

  [rows] = await conn.execute("SELECT kurwa_member(?, ?) AS m, kurwa_count() AS c", ["nset", "n1"]);
  check("binary tinyint", rows[0].m, 1);

  [r] = await conn.execute("DELETE FROM nset WHERE `key` = ?", ["n2"]);
  check("execute delete", r.affectedRows, 1);

  try {
    await conn.query("SELECT `key` FROM nset");
    console.error("FAIL scan allowed"); process.exit(1);
  } catch (e) {
    check("scan refused", e.errno, 1235);
  }

  await conn.end();
  console.log(`mysql2: ${checks} checks passed`);
})().catch((e) => { console.error("FAIL", e); process.exit(1); });
