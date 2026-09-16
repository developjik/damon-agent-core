// damon-agent — minimal Node.js client for the damond WS/JSON-RPC API.
// Zero dependencies. Requires Node >= 22 (global WebSocket), or pass a
// WHATWG-style WebSocket constructor via `webSocket` (e.g. the `ws`
// package's WebSocket works too).
//
//   import { DamonClient } from "damon-agent";
//   const client = await DamonClient.connect("ws://127.0.0.1:9470/ws");
//   await client.initialize();
//   const sessionId = await client.newSession(process.cwd());
//   for await (const ev of client.events()) {
//     if (ev.type === "update" && ev.update.sessionUpdate === "agent_message_chunk")
//       process.stdout.write(ev.update.content.text);
//     if (ev.type === "request")   // permission prompt
//       await client.respond(ev.id, { outcome: { outcome: "selected", optionId: "allow-once" } });
//     if (ev.type === "promptDone") break;
//   }
//   await client.prompt(sessionId, "hi");

export class DamonClient {
  #ws;
  #nextId = 0;
  #pending = new Map(); // id -> {resolve, reject}
  #queue = [];          // buffered events for events()
  #waiters = [];        // pending event consumers

  constructor(ws) {
    this.#ws = ws;
    ws.addEventListener("message", (e) => this.#onMessage(e.data));
    ws.addEventListener("close", () => this.#onClose());
    ws.addEventListener("error", () => this.#onClose());
  }

  /** Connect to a damond WS endpoint. `token` is sent as ?token= (and is
   *  only needed when the daemon has auth_token configured). */
  static async connect(url, { token, webSocket } = {}) {
    const WS = webSocket ?? globalThis.WebSocket;
    if (!WS) {
      throw new Error(
        "no WebSocket implementation — use Node >= 22 or pass { webSocket }"
      );
    }
    if (token) {
      const sep = url.includes("?") ? "&" : "?";
      url = `${url}${sep}token=${encodeURIComponent(token)}`;
    }
    const ws = new WS(url);
    await new Promise((resolve, reject) => {
      ws.addEventListener("open", resolve, { once: true });
      ws.addEventListener("error", (e) => reject(e.error ?? new Error("ws connect failed")), { once: true });
    });
    return new DamonClient(ws);
  }

  #onMessage(data) {
    const m = JSON.parse(typeof data === "string" ? data : data.toString());
    // Response to one of our requests.
    if (m.id !== undefined && m.method === undefined && this.#pending.has(m.id)) {
      const p = this.#pending.get(m.id);
      this.#pending.delete(m.id);
      if (m.error) p.reject(new RpcError(m.error.code, m.error.message));
      else p.resolve(m.result);
      return;
    }
    // Response to a server-initiated request we answered — ignore.
    if (m.method === undefined) return;

    let ev;
    if (m.method === "session/update") {
      ev = { type: "update", sessionId: m.params?.sessionId, update: m.params?.update };
    } else if (m.id !== undefined) {
      // Server-initiated request (e.g. session/request_permission).
      ev = { type: "request", id: m.id, method: m.method, params: m.params };
    } else {
      ev = { type: "notification", method: m.method, params: m.params };
    }
    this.#push(ev);
  }

  #onClose() {
    const err = new Error("connection closed");
    for (const p of this.#pending.values()) p.reject(err);
    this.#pending.clear();
    this.#push(null); // end events() iterators
  }

  #push(ev) {
    const w = this.#waiters.shift();
    if (w) w(ev);
    else this.#queue.push(ev);
  }

  #call(method, params) {
    const id = ++this.#nextId;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      this.#ws.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
    });
  }

  /** Respond to a server-initiated request (permission prompts). */
  async respond(id, result) {
    this.#ws.send(JSON.stringify({ jsonrpc: "2.0", id, result }));
  }

  /** Async iterator over daemon events: {type:"update"|"request"|"notification"|"promptDone"}.
   *  Ends when the connection closes. */
  async *events() {
    for (;;) {
      const ev = this.#queue.length
        ? this.#queue.shift()
        : await new Promise((r) => this.#waiters.push(r));
      if (ev === null) return;
      yield ev;
    }
  }

  initialize() {
    return this.#call("initialize", {
      protocolVersion: 1,
      clientCapabilities: {},
    });
  }

  /** Create a session; returns sessionId. `model` (optional) becomes the
   *  session's default model — accepts "provider/model", a glob-routed id,
   *  a discovered id, or a "model:level" thinking suffix. */
  async newSession(cwd, model) {
    const r = await this.#call("session/new", { cwd, mcpServers: [], model });
    return r.sessionId;
  }

  /** List sessions: [{sessionId, createdAt, model}]. */
  async listSessions() {
    const r = await this.#call("session/list", {});
    return r.sessions;
  }

  /** Resume an existing session; returns sessionId. Throws if unknown. */
  async resumeSession(sessionId) {
    const r = await this.#call("session/resume", { sessionId });
    return r.sessionId;
  }

  async deleteSession(sessionId) {
    return this.#call("session/delete", { sessionId });
  }

  /** Full-text search over all history: [{sessionId, messageId, snippet}]. */
  async search(query, limit = 10) {
    const r = await this.#call("session/search", { query, limit });
    return r.results;
  }

  /** Send a prompt. `model` (optional) overrides the session's default for
   *  this turn. Resolves with {stopReason} when the turn ends; the same
   *  outcome is also delivered as a {type:"promptDone"} event on the
   *  events() stream for consumers that only iterate events. */
  async prompt(sessionId, text, model) {
    const id = ++this.#nextId;
    const done = new Promise((resolve, reject) => {
      this.#pending.set(id, {
        resolve: (result) => {
          this.#push({ type: "promptDone", sessionId, result });
          resolve(result);
        },
        reject: (error) => {
          this.#push({ type: "promptDone", sessionId, error });
          reject(error);
        },
      });
    });
    // Callers may consume the outcome via events() only — don't let an
    // ignored promise rejection crash the process.
    done.catch(() => {});
    this.#ws.send(JSON.stringify({
      jsonrpc: "2.0", id, method: "session/prompt",
      params: { sessionId, model, prompt: [{ type: "text", text }] },
    }));
    return done;
  }

  /** Cancel the session's in-flight turn (notification; no response). */
  cancel(sessionId) {
    this.#ws.send(JSON.stringify({
      jsonrpc: "2.0", method: "session/cancel", params: { sessionId },
    }));
  }

  close() {
    this.#ws.close();
  }
}

export class RpcError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}
