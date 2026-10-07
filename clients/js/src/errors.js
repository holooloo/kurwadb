/** Any failure the client reports. `code` says which kind. */
export class KurwaError extends Error {
  constructor(message, code = "SERVER", details = {}) {
    super(message);
    this.name = "KurwaError";
    this.code = code;
    Object.assign(this, details);
  }
}

/** The server answered with an error: retrying elsewhere will not help. */
export const serverError = (message) => new KurwaError(message, "SERVER");

/** The connection failed or closed: another node may well answer. */
export const connectionError = (message, node) => new KurwaError(message, "CONNECTION", { node });

/** No reply in time. The request may still have run. */
export const timeoutError = (ms, node) =>
  new KurwaError(`kurwadb: no reply within ${ms} ms`, "TIMEOUT", { node });
