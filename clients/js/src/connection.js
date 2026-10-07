// One TCP connection to one node: HELLO 3 with AUTH and a client name, then
// pipelined commands, answered in order.

import net from "node:net";
import { encode, Parser, ReplyError } from "./resp.js";
import { connectionError, serverError, timeoutError } from "./errors.js";

export class Connection {
  constructor({ host, port, user, password, name, connectTimeout, requestTimeout, label }) {
    Object.assign(this, { host, port, user, password, name, connectTimeout, requestTimeout });
    this.label = label || `${host}:${port}`;
    this.pending = []; // FIFO of { resolve, reject, timer, done }
    this.closed = false;
    this.ready = null;
  }

  /** Connects and says HELLO. Resolves to the HELLO reply. */
  open() {
    if (this.ready) return this.ready;
    this.ready = new Promise((resolve, reject) => {
      const socket = net.connect({ host: this.host, port: this.port });
      socket.setNoDelay(true);
      this.socket = socket;
      const parser = new Parser((reply) => this.#reply(reply));

      const timer = setTimeout(() => {
        socket.destroy();
        reject(connectionError(`kurwadb: connecting to ${this.label} timed out`, this.label));
      }, this.connectTimeout);

      socket.once("connect", () => {
        clearTimeout(timer);
        const hello = ["HELLO", "3"];
        if (this.password != null) hello.push("AUTH", this.user, this.password);
        if (this.name) hello.push("SETNAME", this.name);
        this.send(hello).then(resolve, (e) => {
          this.close();
          reject(e);
        });
      });

      socket.on("data", (chunk) => {
        try {
          parser.feed(chunk);
        } catch (e) {
          socket.destroy(e);
        }
      });

      socket.on("error", (e) => {
        clearTimeout(timer);
        this.#fail(connectionError(`kurwadb: ${this.label}: ${e.message}`, this.label));
        reject(connectionError(`kurwadb: ${this.label}: ${e.message}`, this.label));
      });

      socket.on("close", () => {
        clearTimeout(timer);
        this.#fail(connectionError(`kurwadb: connection to ${this.label} closed`, this.label));
        reject(connectionError(`kurwadb: connection to ${this.label} closed`, this.label));
      });
    });
    return this.ready;
  }

  /** Requests not answered yet. */
  get load() {
    return this.pending.length;
  }

  /** Sends one command; resolves to its reply, rejects with a KurwaError. */
  send(args) {
    return this.sendMany([args]).then((replies) => replies[0]);
  }

  /**
   * Sends several commands in one write. Resolves to their replies in order;
   * a server error in any of them rejects the whole batch with that error.
   */
  sendMany(commands) {
    if (this.closed) return Promise.reject(connectionError(`kurwadb: connection to ${this.label} closed`, this.label));
    const promises = commands.map(
      () =>
        new Promise((resolve, reject) => {
          const entry = { resolve, reject, done: false, timer: null };
          entry.timer = setTimeout(() => {
            // The reply may still come: the entry stays in the FIFO so it is
            // matched and dropped, and later replies stay in step.
            entry.done = true;
            reject(timeoutError(this.requestTimeout, this.label));
          }, this.requestTimeout);
          this.pending.push(entry);
        })
    );
    this.socket.write(Buffer.concat(commands.map(encode)));
    return Promise.all(promises);
  }

  #reply(reply) {
    const entry = this.pending.shift();
    if (!entry || entry.done) return;
    entry.done = true;
    clearTimeout(entry.timer);
    if (reply instanceof ReplyError) entry.reject(serverError(reply.message));
    else entry.resolve(reply);
  }

  #fail(error) {
    this.closed = true;
    const pending = this.pending;
    this.pending = [];
    for (const entry of pending) {
      clearTimeout(entry.timer);
      if (!entry.done) {
        entry.done = true;
        entry.reject(error);
      }
    }
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    try {
      this.socket.end();
    } catch {}
    this.#fail(connectionError(`kurwadb: connection to ${this.label} closed`, this.label));
  }
}
