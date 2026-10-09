// Latency of has() with routing to the key's replica, against without it:
// sequential requests, median and p99 in microseconds.
//
//     KURWA_NODES=10.10.10.112:26379 KURWA_PASSWORD=... node bench/routing.js [requests]

import { connect } from "../src/index.js";

const nodes = (process.env.KURWA_NODES || "127.0.0.1:6379").split(",");
const password = process.env.KURWA_PASSWORD;
const count = Number(process.argv[2] || 3000);

async function measure(routing) {
  const db = await connect({ nodes, password, routing });
  const keys = Array.from({ length: 500 }, (_, i) => `bench:routing:${i}`);
  await Promise.all(keys.map((k) => db.add(k)));
  for (let i = 0; i < 500; i++) await db.has(keys[i % keys.length]); // warm up
  const times = [];
  for (let i = 0; i < count; i++) {
    const t = process.hrtime.bigint();
    await db.has(keys[i % keys.length]);
    times.push(Number(process.hrtime.bigint() - t) / 1000);
  }
  await db.close();
  times.sort((a, b) => a - b);
  return { median: times[times.length >> 1], p99: times[Math.floor(times.length * 0.99)] };
}

// alternate, so drift on the machine or the network hits both alike
const runs = { on: [], off: [] };
for (let round = 0; round < 3; round++) {
  runs.off.push(await measure(false));
  runs.on.push(await measure(true));
}
const med = (xs, f) => xs.map(f).sort((a, b) => a - b)[xs.length >> 1];
for (const [name, rs] of Object.entries(runs)) {
  console.log(`routing ${name}: median ${med(rs, (r) => r.median).toFixed(0)} µs, p99 ${med(rs, (r) => r.p99).toFixed(0)} µs (${count} sequential has(), median of 3 runs)`);
}
