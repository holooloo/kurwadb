// The server's consistent-hash ring (lib/kurwa/ring.ex), rebuilt bit for bit
// so the client can send a key straight to one of its replicas.
//
// A ring position is the first 8 bytes of SHA-256, big-endian. Each member
// is placed `vnodes` times, at "<node name>/<i>". A key's preference list
// walks clockwise from the first point at or after the key's position,
// collecting distinct members until it has `n`. The key hashed is the
// storage key (lib/kurwa/key.ex): a 0 byte then the key in the default set,
// or the set name's length, the name, then the key in a named set.

import { createHash } from "node:crypto";

const hash = (buf) => createHash("sha256").update(buf).digest().readBigUInt64BE(0);

export class Ring {
  /** `members`: node names as KURWA.RING / KURWA.NODES report them. */
  constructor(members, vnodes, n) {
    this.members = [...new Set(members)].sort();
    this.vnodes = vnodes;
    this.n = n;
    const points = [];
    for (const name of this.members) {
      for (let i = 0; i < vnodes; i++) points.push([hash(Buffer.from(`${name}/${i}`)), name]);
    }
    // Elixir sorts {position, node} tuples: by position, then by name.
    points.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0));
    this.positions = points.map((p) => p[0]);
    this.owners = points.map((p) => p[1]);
  }

  /** The members responsible for a storage key, in preference order. */
  preflist(storageKey, n = this.n) {
    const total = this.positions.length;
    if (total === 0) return [];
    const want = Math.min(n, this.members.length);
    const h = hash(storageKey);
    let lo = 0;
    let hi = total;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this.positions[mid] >= h) hi = mid;
      else lo = mid + 1;
    }
    const start = lo === total ? 0 : lo;
    const out = [];
    for (let step = 0; step < total && out.length < want; step++) {
      const owner = this.owners[(start + step) % total];
      if (!out.includes(owner)) out.push(owner);
    }
    return out;
  }
}

/** The storage key: what the server hashes to place `key` in `set` (null: default set). */
export function storageKey(set, key) {
  const k = Buffer.isBuffer(key) ? key : Buffer.from(String(key));
  if (set == null || set === "") return Buffer.concat([Buffer.from([0]), k]);
  const name = Buffer.from(set);
  return Buffer.concat([Buffer.from([name.length]), name, k]);
}
