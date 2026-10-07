// RESP3 encoding of commands and an incremental parser of replies.
//
// Commands go out as arrays of bulk strings, which every RESP version
// accepts. Replies are parsed as RESP3 (kurwadb answers HELLO 3), with the
// RESP2 shapes too, so a reply is decoded the same way either way.

const CRLF = Buffer.from("\r\n");

/** Encodes one command: an array of strings, Buffers or numbers. */
export function encode(args) {
  const parts = [Buffer.from(`*${args.length}\r\n`)];
  for (const arg of args) {
    const b = Buffer.isBuffer(arg) ? arg : Buffer.from(String(arg));
    parts.push(Buffer.from(`$${b.length}\r\n`), b, CRLF);
  }
  return Buffer.concat(parts);
}

/** A server error reply (`-ERR ...`), returned as a value, not thrown. */
export class ReplyError {
  constructor(message) {
    this.message = message;
  }
}

const INCOMPLETE = Symbol("incomplete");

/**
 * Feeds bytes in, calls `onReply` for every complete top-level reply.
 * Push messages (`>`) are dropped: kurwadb sends none.
 */
export class Parser {
  constructor(onReply) {
    this.onReply = onReply;
    this.buffer = Buffer.alloc(0);
  }

  feed(chunk) {
    this.buffer = this.buffer.length ? Buffer.concat([this.buffer, chunk]) : chunk;
    for (;;) {
      if (this.buffer.length === 0) return;
      const result = this.#value(0);
      if (result === INCOMPLETE) return;
      const [value, next, push] = result;
      this.buffer = this.buffer.subarray(next);
      if (!push) this.onReply(value);
    }
  }

  #line(at) {
    const end = this.buffer.indexOf(CRLF, at);
    if (end < 0) return INCOMPLETE;
    return [this.buffer.toString("utf8", at + 1, end), end + 2];
  }

  // [value, nextOffset, isPush] or INCOMPLETE
  #value(at) {
    if (at >= this.buffer.length) return INCOMPLETE;
    const type = String.fromCharCode(this.buffer[at]);
    const line = this.#line(at);
    if (line === INCOMPLETE) return INCOMPLETE;
    const [text, next] = line;

    switch (type) {
      case "+":
        return [text, next];
      case "-":
        return [new ReplyError(text), next];
      case ":":
        return [Number(text), next];
      case "(":
        return [BigInt(text), next];
      case ",":
        return [text === "inf" ? Infinity : text === "-inf" ? -Infinity : Number(text), next];
      case "#":
        return [text === "t", next];
      case "_":
        return [null, next];
      case "$":
      case "=":
      case "!": {
        const len = Number(text);
        if (len < 0) return [null, next];
        if (this.buffer.length < next + len + 2) return INCOMPLETE;
        let data = this.buffer.subarray(next, next + len);
        if (type === "=") data = data.subarray(4); // "txt:" prefix
        const value = type === "!" ? new ReplyError(data.toString()) : Buffer.from(data);
        return [value, next + len + 2];
      }
      case "*":
      case "~":
      case ">": {
        const n = Number(text);
        if (n < 0) return [null, next];
        const items = [];
        let cursor = next;
        for (let i = 0; i < n; i++) {
          const item = this.#value(cursor);
          if (item === INCOMPLETE) return INCOMPLETE;
          items.push(item[0]);
          cursor = item[1];
        }
        return [items, cursor, type === ">"];
      }
      case "%":
      case "|": {
        const n = Number(text);
        const map = new Map();
        let cursor = next;
        for (let i = 0; i < n; i++) {
          const k = this.#value(cursor);
          if (k === INCOMPLETE) return INCOMPLETE;
          const v = this.#value(k[1]);
          if (v === INCOMPLETE) return INCOMPLETE;
          map.set(Buffer.isBuffer(k[0]) ? k[0].toString() : k[0], v[0]);
          cursor = v[1];
        }
        if (type === "|") {
          // An attribute precedes the reply it describes: skip it.
          const real = this.#value(cursor);
          if (real === INCOMPLETE) return INCOMPLETE;
          return real;
        }
        return [map, cursor];
      }
      default:
        throw new Error(`kurwadb: unexpected RESP type byte ${JSON.stringify(type)}`);
    }
  }
}
