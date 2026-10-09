/** Options for `connect`. */
export interface ConnectOptions {
  /** Seed nodes, "host:port" of their RESP frontends. Any one is enough. */
  nodes?: string[];
  /** kurwadb's auth token. */
  password?: string | null;
  /** Any user name: kurwadb has one secret. Default "kurwa". */
  user?: string;
  /** Connections per node. Default 2. */
  pool?: number;
  connectTimeout?: number;
  requestTimeout?: number;
  /** How often to re-read the cluster's members, ms. Default 10000; 0 never. */
  refreshInterval?: number;
  /** How often to retry a node marked down, ms. Default 2000. */
  probeInterval?: number;
  /** Ask the seed for the other nodes (KURWA.NODES). Default true. */
  discover?: boolean;
  /**
   * Send each request to the first healthy replica of its key, from the
   * cluster's ring (KURWA.RING), so the coordinator answers its own copy
   * without a network hop. Default true; needs discover. Without a ring, or
   * with no replica reachable, requests go to the least busy healthy node.
   */
  routing?: boolean;
  /** Client name shown on the server (CLIENT SETNAME). */
  name?: string;
}

export interface TtlOptions {
  /** Seconds until the key expires. */
  ttl?: number;
  /** Milliseconds until the key expires. */
  ttlMs?: number;
}

export type Key = string | Buffer;

export interface SetOps {
  add(key: Key, opts?: TtlOptions): Promise<void>;
  /** True only for the caller that added the key. */
  addNew(key: Key, opts?: TtlOptions): Promise<boolean>;
  has(key: Key): Promise<boolean>;
  hasMany(keys: Key[]): Promise<boolean[]>;
  /** True if the key was there. */
  delete(key: Key): Promise<boolean>;
}

export interface NodeInfo {
  name: string;
  host: string;
  port: number;
  up: boolean;
  inFlight: number;
}

export interface Kurwa extends SetOps {
  /** A named set. Over RESP it takes no ttl and has no atomic addNew. */
  set(name: string): SetOps;
  nodes(): NodeInfo[];
  /** The nodes holding `key` in `set` (default: the default set), first preferred; [] without a ring. */
  replicas(key: Key, set?: string | null): string[];
  refresh(): Promise<void>;
  close(): Promise<void>;
}

export function connect(opts?: ConnectOptions): Promise<Kurwa>;

export class KurwaError extends Error {
  /** SERVER: the server refused; CONNECTION: a node failed; TIMEOUT; UNSUPPORTED; CLOSED. */
  code: "SERVER" | "CONNECTION" | "TIMEOUT" | "UNSUPPORTED" | "CLOSED";
  node?: string;
}

export const VERSION: string;
