import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { connect, KurwaError } from "../src/index.js";
import { startServer, proxy } from "./server.js";

let server, db;
const id = () => `js:${process.pid}:${Math.random().toString(36).slice(2)}`;

before(async () => {
  server = await startServer();
  db = await connect({ nodes: server.nodes, password: server.password });
});

after(async () => {
  await db?.close();
  await server?.stop();
});

test("add, has, delete on the default set", async () => {
  const k = id();
  assert.equal(await db.has(k), false);
  await db.add(k);
  assert.equal(await db.has(k), true);
  assert.equal(await db.delete(k), true);
  assert.equal(await db.delete(k), false);
  assert.equal(await db.has(k), false);
});

test("addNew: one winner among concurrent callers", async () => {
  const k = id();
  const results = await Promise.all(Array.from({ length: 25 }, () => db.addNew(k, { ttl: 60 })));
  assert.equal(results.filter(Boolean).length, 1);
  assert.equal(await db.addNew(k), false);
});

test("ttl expires a key", async () => {
  const k = id();
  await db.add(k, { ttlMs: 300 });
  assert.equal(await db.has(k), true);
  await new Promise((r) => setTimeout(r, 700));
  assert.equal(await db.has(k), false);
});

test("hasMany answers in order, in one round trip", async () => {
  const [a, b, c] = [id(), id(), id()];
  await db.add(a);
  await db.add(c);
  assert.deepEqual(await db.hasMany([a, b, c]), [true, false, true]);
  assert.deepEqual(await db.hasMany([]), []);
});

test("binary keys", async () => {
  const k = Buffer.from([0, 255, 1, 254, 10, 13]);
  await db.add(k);
  assert.equal(await db.has(k), true);
  assert.equal(await db.has(Buffer.from([0, 255, 1])), false);
});

test("named sets", async () => {
  const s = db.set(`jsset-${process.pid}`);
  const k = id();
  await s.add(k);
  assert.equal(await s.has(k), true);
  assert.equal(await db.has(k), false, "a named set is not the default set");
  assert.deepEqual(await s.hasMany([k, id()]), [true, false]);
  assert.equal(await s.delete(k), true);
  assert.equal(await s.has(k), false);
  await assert.rejects(s.add(k, { ttl: 5 }), (e) => e instanceof KurwaError && e.code === "UNSUPPORTED");
  await assert.rejects(s.addNew(k), (e) => e.code === "UNSUPPORTED");
});

test("thousands of concurrent requests are pipelined", async () => {
  const keys = Array.from({ length: 2000 }, (_, i) => `${id()}:${i}`);
  await Promise.all(keys.filter((_, i) => i % 2 === 0).map((k) => db.add(k)));
  const answers = await Promise.all(keys.map((k) => db.has(k)));
  assert.deepEqual(answers, keys.map((_, i) => i % 2 === 0));
});

test("discovers the cluster from a seed", async () => {
  const nodes = db.nodes();
  assert.ok(nodes.length >= 1);
  assert.ok(nodes.every((n) => typeof n.name === "string" && n.port > 0));
  assert.ok(nodes.some((n) => n.up));
});

test("server errors are KurwaErrors with the server's message", async () => {
  await assert.rejects(
    connect({ nodes: server.nodes, password: "wrong", discover: false }),
    (e) => e instanceof KurwaError && e.code === "SERVER" && /WRONGPASS|invalid/i.test(e.message)
  );
});

test("a dead seed is skipped", async () => {
  const c = await connect({ nodes: ["127.0.0.1:1", ...server.nodes], password: server.password, connectTimeout: 500 });
  assert.equal(await c.has(id()), false);
  await c.close();
});

test("requests move to another node when one dies", async () => {
  const p = await proxy(server.nodes[0]);
  const c = await connect({ nodes: [p.address, ...server.nodes], password: server.password, discover: false, probeInterval: 100 });
  const k = id();
  await c.add(k);
  // load both nodes, then kill one mid-flight
  const inflight = Array.from({ length: 200 }, () => c.has(k));
  const killed = p.kill();
  const results = await Promise.allSettled(inflight);
  assert.ok(results.every((r) => r.status === "fulfilled" && r.value === true), "idempotent reads retried elsewhere");
  for (let i = 0; i < 50; i++) assert.equal(await c.has(k), true);
  assert.equal(c.nodes().find((n) => n.host + ":" + n.port === p.address).up, false);
  await killed;

  // the node comes back on the same address: a probe finds it
  const again = await proxy(server.nodes[0], p.port);
  const deadline = Date.now() + 3000;
  while (!c.nodes().find((n) => n.port === p.port).up && Date.now() < deadline) await new Promise((r) => setTimeout(r, 50));
  assert.equal(c.nodes().find((n) => n.port === p.port).up, true);
  await c.close();
  await again.kill();
});
