# kurwadb for Node.js

A client for [kurwadb](../../README.md): add a key, ask whether it is there,
delete it - and `addNew`, the atomic "first one wins" that idempotency checks
are built on. No dependencies, Node 18 or later, types included.

```sh
npm install ./clients/js        # from a checkout; not on npm yet
```

```js
import { connect } from "kurwadb";

const db = await connect({ nodes: ["10.10.10.112:26379"], password: process.env.KURWA_TOKEN });

if (await db.addNew(`payment:${id}`, { ttl: 86400 })) {
  await charge();                       // exactly one caller gets here
}

await db.add("order:1029");
await db.has("order:1029");             // true
await db.hasMany(["a", "b", "c"]);      // [true, false, true], one round trip
await db.delete("order:1029");          // true, then false

const seen = db.set("seen");            // a named set
await seen.add("ip:10.0.0.1");
await seen.has("ip:10.0.0.1");

db.nodes();                             // [{ name, host, port, up, inFlight }]
await db.close();
```

## How it talks to the cluster

It speaks RESP3 to each node's Redis frontend (`HELLO 3`, the token as the
password, `kurwadb-js/<version>` as the client name, which the dashboard
shows), with requests pipelined: many in flight on a connection, answered in
order.

**Discovery.** Any one node is enough to start. The client asks it for the
cluster's members with `KURWA.NODES` - each node's name and the address
clients reach its RESP frontend at (the published one, when the node is
behind port forwarding) - and asks again every 10 seconds and whenever a
node fails.

**Routing.** Any kurwadb node coordinates any request, but a node holding a
replica of the key answers its own copy inline, without a network hop. So the
client also fetches the ring (`KURWA.RING`: vnodes, n and the members),
rebuilds it exactly as the server does - SHA-256 of `"<node>/<i>"` for each
of `vnodes` points per member, the key's position the first 8 bytes of
SHA-256 of its storage key, a 0 byte then the key in the default set, or the
set name's length and name then the key - and sends each request to the first
healthy node in the key's preference list. `hasMany` splits its keys by
replica and sends one pipeline per node, in parallel. With no ring (an older
server, or `routing: false`) or no replica reachable, a request goes to the
healthy node with the fewest requests in flight. Each node gets a pool of
connections (two by default) and a request the least busy of them.
`db.replicas(key, set)` shows where a key lives;
`node bench/routing.js` measures routing on against off.

**What it does not buy yet.** Measured on 2026-10-09 against five local nodes
(n=3, r=2): 86 µs a read either way, and in Go about 40 000 reads/s from 64
goroutines either way. A quorum read waits for one remote replica whether or
not the coordinator holds a copy, and the server sends the read to all n
replicas, so landing on a replica saves one message in three - too little to
see. The gain comes when the server reads from r replicas instead of all n
and the client sends each key to one of them; routing is the client half of
that, and is right today, but not faster.

**Failover.** A connection that fails marks its node down; its in-flight
requests fail with `code: "CONNECTION"`. Reads and `add` are retried once on
another node, since running them twice is harmless. `addNew` and `delete`
are not: their answer says what *this* call changed, and a retry after a
first attempt that did land would answer wrongly (`addNew` would say
"exists" for the key it added). A node marked down is probed every 2
seconds and used again once it answers.

## Options

| option | default | |
|---|---|---|
| `nodes` | `["127.0.0.1:6379"]` | seeds, `host:port` of RESP frontends |
| `password` | none | kurwadb's auth token |
| `user` | `"kurwa"` | any name; kurwadb has one secret |
| `pool` | 2 | connections per node |
| `connectTimeout` | 3000 | ms |
| `requestTimeout` | 5000 | ms; a request that times out may still have run |
| `refreshInterval` | 10000 | ms between `KURWA.NODES`; 0 never |
| `probeInterval` | 2000 | ms between retries of a down node |
| `discover` | true | false uses exactly the `nodes` given |

## Errors

Everything rejects with `KurwaError`, whose `code` is `SERVER` (kurwadb said
no, with its message), `CONNECTION`, `TIMEOUT`, `UNSUPPORTED` or `CLOSED`.

## Limits

- Over RESP, named-set keys take no ttl and have no atomic `addNew`: `SADD`
  looks, then writes. Both work on the default set (`SET ... PX`, `SET NX`).
- Routing picks the first healthy replica, not the least busy one: a hot key
  loads one node.
- No TLS yet.

## Tests

```sh
npm test        # starts a kurwadb node from this repository (needs elixir)
KURWA_RESP_NODES=10.10.10.112:26379 KURWA_PASSWORD=... npm test   # against a running one
```
