// A kurwadb node for the tests: KURWA_RESP_NODES (comma-separated host:port)
// and KURWA_PASSWORD if given, else one started here from the repository.

import { spawn } from "node:child_process";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { fileURLToPath } from "node:url";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export function freePort() {
  return new Promise((resolve) => {
    const s = net.createServer().listen(0, "127.0.0.1", () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });
}

function reachable(port) {
  return new Promise((resolve) => {
    const s = net.connect({ host: "127.0.0.1", port }, () => {
      s.destroy();
      resolve(true);
    });
    s.on("error", () => resolve(false));
  });
}

export async function startServer() {
  if (process.env.KURWA_RESP_NODES) {
    return { nodes: process.env.KURWA_RESP_NODES.split(","), password: process.env.KURWA_PASSWORD, stop: async () => {} };
  }
  const port = await freePort();
  const http = await freePort();
  const password = "js-test-secret";
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "kurwadb-js-"));
  const child = spawn("elixir", ["-S", "mix", "run", "--no-halt"], {
    cwd: repo,
    detached: true,
    stdio: ["ignore", "ignore", "pipe"],
    env: {
      ...process.env,
      PATH: `${os.homedir()}/.cargo/bin:/opt/homebrew/bin:${process.env.PATH}`,
      KURWA_RESP: "1",
      KURWA_RESP_PORT: String(port),
      KURWA_HTTP_PORT: String(http),
      KURWA_N: "1",
      KURWA_R: "1",
      KURWA_W: "1",
      KURWA_DATA_DIR: dataDir,
      KURWA_AUTH_TOKEN: password,
    },
  });
  let stderr = "";
  child.stderr.on("data", (d) => (stderr += d));
  const deadline = Date.now() + 120000;
  while (!(await reachable(port))) {
    if (child.exitCode != null || Date.now() > deadline) throw new Error("kurwadb did not start:\n" + stderr);
    await new Promise((r) => setTimeout(r, 250));
  }
  return {
    nodes: [`127.0.0.1:${port}`],
    port,
    password,
    stop: async () => {
      try {
        process.kill(-child.pid, "SIGTERM");
      } catch {}
      fs.rmSync(dataDir, { recursive: true, force: true });
    },
  };
}

/** A TCP forwarder to `port`, standing in for a second node that can die. */
// A TCP proxy to `target` ("host:port") that can be killed, to make a node
// disappear from under a client.
export async function proxy(target, listenPort = 0) {
  const [host, port] = [target.slice(0, target.lastIndexOf(":")), Number(target.split(":").pop())];
  const sockets = new Set();
  const server = net.createServer((client) => {
    const upstream = net.connect({ host, port });
    sockets.add(client).add(upstream);
    client.pipe(upstream).pipe(client);
    const done = () => {
      client.destroy();
      upstream.destroy();
    };
    client.on("error", done).on("close", done);
    upstream.on("error", done).on("close", done);
  });
  await new Promise((r) => server.listen(listenPort, "127.0.0.1", r));
  return {
    address: `127.0.0.1:${server.address().port}`,
    port: server.address().port,
    kill: () =>
      new Promise((r) => {
        server.close(r);
        for (const s of sockets) s.destroy();
      }),
  };
}
