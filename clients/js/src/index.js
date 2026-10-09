// kurwadb for Node.js.
//
//     import { connect } from "kurwadb";
//     const db = await connect({ nodes: ["10.10.10.112:26379"], password: "..." });
//     if (await db.addNew(`payment:${id}`, { ttl: 86400 })) charge();
//
// Any kurwadb node coordinates any request, but the one that holds a replica
// of the key answers its own copy without a network hop. So the client learns
// the cluster's nodes (KURWA.NODES) and its ring (KURWA.RING) from one of
// them, keeps a few pipelined connections to each, and sends every request
// to the first healthy replica of its key - or, when the ring is unknown or
// no replica is reachable, to the healthy node with the fewest in flight. It
// moves to another node when one fails.

import { Connection } from "./connection.js";
import { KurwaError } from "./errors.js";
import { Ring, storageKey } from "./ring.js";

export { KurwaError } from "./errors.js";

export const VERSION = "0.1.0";

const DEFAULTS = {
  user: "kurwa",
  password: null,
  pool: 2,
  connectTimeout: 3000,
  requestTimeout: 5000,
  refreshInterval: 10000,
  probeInterval: 2000,
  discover: true,
  routing: true,
  name: `kurwadb-js/${VERSION}`,
};

/** Connects to a cluster through any of `opts.nodes` ("host:port"). */
export async function connect(opts = {}) {
  const client = new Client(opts);
  await client.start();
  return client;
}

class Node {
  constructor(client, { name, host, port }) {
    this.client = client;
    this.name = name || `${host}:${port}`;
    this.host = host;
    this.port = Number(port);
    this.up = true;
    this.conns = [];
    this.opening = null;
    this.downSince = null;
  }

  get key() {
    return `${this.host}:${this.port}`;
  }

  get load() {
    return this.conns.reduce((sum, c) => sum + c.load, 0);
  }

  /** The least busy open connection, opening the pool on first use. */
  async connection() {
    this.conns = this.conns.filter((c) => !c.closed);
    if (this.conns.length < this.client.opts.pool) {
      if (!this.opening) {
        // Errors are kept, not thrown here: a pool topping itself up in the
        // background must not leave a rejection nobody awaits.
        this.opening = this.#open()
          .then(() => null, (e) => e)
          .finally(() => (this.opening = null));
      }
      if (this.conns.length === 0) {
        const error = await this.opening;
        if (error && this.conns.length === 0) throw error;
      }
    }
    this.conns = this.conns.filter((c) => !c.closed);
    if (this.conns.length === 0) throw new KurwaError(`kurwadb: no connection to ${this.key}`, "CONNECTION", { node: this.name });
    return this.conns.reduce((best, c) => (c.load < best.load ? c : best));
  }

  async #open() {
    const o = this.client.opts;
    const conn = new Connection({
      host: this.host,
      port: this.port,
      user: o.user,
      password: o.password,
      name: o.name,
      connectTimeout: o.connectTimeout,
      requestTimeout: o.requestTimeout,
      label: this.name,
    });
    await conn.open();
    this.conns.push(conn);
    return conn;
  }

  markDown() {
    if (this.up) {
      this.up = false;
      this.downSince = Date.now();
    }
    for (const c of this.conns) c.close();
    this.conns = [];
  }

  close() {
    for (const c of this.conns) c.close();
    this.conns = [];
  }
}

class Client {
  constructor(opts) {
    this.opts = { ...DEFAULTS, ...opts };
    const seeds = this.opts.nodes || ["127.0.0.1:6379"];
    this.seeds = seeds.map(parseAddress);
    this.nodeMap = new Map();
    this.ring = null;
    this.timers = [];
    this.closed = false;
  }

  async start() {
    let lastError = null;
    for (const seed of this.seeds) {
      const node = new Node(this, seed);
      try {
        await node.connection();
        this.#add(node);
        if (this.opts.discover) await this.refresh(node);
        break;
      } catch (e) {
        node.close();
        lastError = e;
      }
    }
    if (this.nodeMap.size === 0) {
      throw lastError || new KurwaError("kurwadb: no seed node answered", "CONNECTION");
    }
    if (!this.opts.discover) {
      for (const seed of this.seeds) if (!this.nodeMap.has(`${seed.host}:${seed.port}`)) this.#add(new Node(this, seed));
    }
    if (this.opts.discover && this.opts.refreshInterval > 0) {
      this.timers.push(setInterval(() => this.refresh().catch(() => {}), this.opts.refreshInterval).unref());
    }
    this.timers.push(setInterval(() => this.#probe(), this.opts.probeInterval).unref());
  }

  #add(node) {
    this.nodeMap.set(node.key, node);
  }

  /**
   * Asks a node for the cluster's members and their addresses. Nodes that
   * left are dropped, new ones added; a node the cluster says is down is
   * not sent requests until a probe reaches it.
   */
  async refresh(via) {
    const node = via || this.#pick();
    const conn = await node.connection();
    let reply;
    try {
      reply = await conn.send(["KURWA.NODES"]);
    } catch (e) {
      if (e.code === "SERVER") return; // a server without KURWA.NODES: keep the seeds
      throw e;
    }
    const seen = new Set();
    for (const [name, host, port, up] of reply) {
      const entry = { name: str(name), host: str(host), port: Number(port) };
      const key = `${entry.host}:${entry.port}`;
      seen.add(key);
      let known = this.nodeMap.get(key);
      if (!known) {
        known = new Node(this, entry);
        this.#add(known);
      }
      known.name = entry.name;
      if (!(up === true || up === 1) && known.up) known.markDown();
    }
    // the node we asked through stays even if it reports itself by another address
    for (const [key, n] of this.nodeMap) {
      if (!seen.has(key) && n !== node) {
        n.close();
        this.nodeMap.delete(key);
      }
    }
    if (this.opts.routing) await this.#loadRing(conn);
  }

  // The ring, rebuilt only when its members, vnodes or n changed. A server
  // without KURWA.RING leaves routing off.
  async #loadRing(conn) {
    let reply;
    try {
      reply = await conn.send(["KURWA.RING"]);
    } catch (e) {
      if (e.code === "SERVER") {
        this.ring = null;
        return;
      }
      throw e;
    }
    const [vnodes, n, members] = reply;
    const names = members.map(str);
    const r = this.ring;
    const same = r && r.vnodes === Number(vnodes) && r.n === Number(n) && r.members.join("\n") === [...names].sort().join("\n");
    if (!same) this.ring = new Ring(names, Number(vnodes), Number(n));
  }

  /** The node a request about `skey` (a storage key) goes to. */
  #route(skey, exclude) {
    if (this.opts.routing && this.ring && skey) {
      for (const name of this.ring.preflist(skey)) {
        for (const node of this.nodeMap.values()) {
          if (node.name === name && node.up && node !== exclude) return node;
        }
      }
    }
    return this.#pick(exclude);
  }

  async #probe() {
    for (const node of this.nodeMap.values()) {
      if (node.up || this.closed) continue;
      try {
        const conn = await node.connection();
        await conn.send(["PING"]);
        node.up = true;
        node.downSince = null;
      } catch {
        node.markDown();
      }
    }
  }

  #pick(exclude) {
    let best = null;
    for (const node of this.nodeMap.values()) {
      if (!node.up || node === exclude) continue;
      if (!best || node.load < best.load) best = node;
    }
    if (!best) throw new KurwaError("kurwadb: no node is reachable", "CONNECTION");
    return best;
  }

  /**
   * Runs `commands` on one node. A connection failure marks the node down;
   * an idempotent request is then tried once more on another node.
   */
  async run(commands, { idempotent, route = null }) {
    if (this.closed) throw new KurwaError("kurwadb: client is closed", "CLOSED");
    let node = this.#route(route);
    for (let attempt = 0; ; attempt++) {
      try {
        const conn = await node.connection();
        return await conn.sendMany(commands);
      } catch (e) {
        if (e.code !== "CONNECTION" || attempt > 0 || !idempotent) {
          if (e.code === "CONNECTION") node.markDown();
          throw e;
        }
        node.markDown();
        if (this.opts.discover) this.refresh().catch(() => {});
        node = this.#route(route, node);
      }
    }
  }

  /**
   * Runs one command per item, each on its own key's replica: items bound
   * for the same node go as one pipeline, the pipelines in parallel.
   * Replies come back in the items' order.
   */
  async runEach(set, items, command) {
    if (!(this.opts.routing && this.ring)) {
      return this.run(items.map(command), { idempotent: true });
    }
    const groups = new Map();
    items.forEach((item, i) => {
      const skey = storageKey(set, item);
      const node = this.#route(skey);
      const g = groups.get(node) || { skey, idx: [] };
      g.idx.push(i);
      groups.set(node, g);
    });
    const out = new Array(items.length);
    await Promise.all(
      [...groups.values()].map(async ({ skey, idx }) => {
        const replies = await this.run(idx.map((i) => command(items[i])), { idempotent: true, route: skey });
        idx.forEach((i, j) => (out[i] = replies[j]));
      }),
    );
    return out;
  }

  // ------------------------------------------------------------- the API

  /** Adds `key` to the default set. `ttl` in seconds, or `ttlMs`. */
  async add(key, opts = {}) {
    await this.run([["SET", key, "1", ...px(opts)]], { idempotent: true, route: storageKey(null, key) });
  }

  /**
   * Adds `key` only if it is not there: true for the one caller that added
   * it, false for everyone else - the idempotency check. Not retried on
   * another node, since a retry could be told "exists" by its own first try.
   */
  async addNew(key, opts = {}) {
    const [reply] = await this.run([["SET", key, "1", "NX", ...px(opts)]], { idempotent: false, route: storageKey(null, key) });
    return reply != null;
  }

  /** Is `key` in the default set? */
  async has(key) {
    const [n] = await this.run([["EXISTS", key]], { idempotent: true, route: storageKey(null, key) });
    return n > 0;
  }

  /** Membership of each key, in order, in one round trip. */
  async hasMany(keys) {
    if (keys.length === 0) return [];
    const replies = await this.runEach(null, keys, (k) => ["EXISTS", k]);
    return replies.map((n) => n > 0);
  }

  /** Removes `key`. True if it was there. */
  async delete(key) {
    const [n] = await this.run([["DEL", key]], { idempotent: false, route: storageKey(null, key) });
    return n > 0;
  }

  /** The same operations on the named set `name`. */
  set(name) {
    return new NamedSet(this, name);
  }

  /** The members holding `key` (in `set`, default the default set), first preferred; [] without a ring. */
  replicas(key, set = null) {
    return this.ring ? this.ring.preflist(storageKey(set, key)) : [];
  }

  /** The nodes this client knows, with their state and requests in flight. */
  nodes() {
    return [...this.nodeMap.values()].map((n) => ({ name: n.name, host: n.host, port: n.port, up: n.up, inFlight: n.load }));
  }

  /** Closes every connection. */
  async close() {
    this.closed = true;
    for (const t of this.timers) clearInterval(t);
    for (const n of this.nodeMap.values()) n.close();
  }
}

class NamedSet {
  constructor(client, name) {
    this.client = client;
    this.name = name;
  }

  /** Adds `key`. Named sets take no ttl over RESP (SADD has none). */
  async add(key, opts = {}) {
    if (opts.ttl != null || opts.ttlMs != null) {
      throw new KurwaError("kurwadb: a ttl on a named-set key is not available over RESP; use the default set", "UNSUPPORTED");
    }
    await this.client.run([["SADD", this.name, key]], { idempotent: true, route: storageKey(this.name, key) });
  }

  /** Not available over RESP for named sets: SADD looks, then writes. */
  async addNew() {
    throw new KurwaError("kurwadb: addNew is only atomic on the default set over RESP (SET NX); use db.addNew", "UNSUPPORTED");
  }

  async has(key) {
    const [n] = await this.client.run([["SISMEMBER", this.name, key]], { idempotent: true, route: storageKey(this.name, key) });
    return n === true || n > 0;
  }

  async hasMany(keys) {
    if (keys.length === 0) return [];
    const replies = await this.client.runEach(this.name, keys, (k) => ["SISMEMBER", this.name, k]);
    return replies.map((n) => n === true || n > 0);
  }

  async delete(key) {
    const [n] = await this.client.run([["SREM", this.name, key]], { idempotent: false, route: storageKey(this.name, key) });
    return n > 0;
  }
}

function px(opts) {
  if (opts.ttlMs != null) return ["PX", String(Math.max(1, Math.round(opts.ttlMs)))];
  if (opts.ttl != null) return ["PX", String(Math.max(1, Math.round(opts.ttl * 1000)))];
  return [];
}

function parseAddress(address) {
  if (typeof address === "object") return { host: address.host, port: Number(address.port) };
  const i = address.lastIndexOf(":");
  if (i < 0) return { host: address, port: 6379 };
  return { host: address.slice(0, i).replace(/^\[|\]$/g, ""), port: Number(address.slice(i + 1)) };
}

const str = (v) => (Buffer.isBuffer(v) ? v.toString() : String(v));
