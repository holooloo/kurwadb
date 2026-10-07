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

**Routing.** Every kurwadb node coordinates any request, so there is no
routing table: each request goes to the healthy node with the fewest
requests in flight, over the least busy of its connections (two per node by
default).

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
- Routing does not follow the ring: a request costs one hop from the node it
  lands on to the replicas, whichever node that is.
- No TLS yet.

## Tests

```sh
npm test        # starts a kurwadb node from this repository (needs elixir)
KURWA_RESP_NODES=10.10.10.112:26379 KURWA_PASSWORD=... npm test   # against a running one
```
