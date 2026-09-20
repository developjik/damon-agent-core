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
  #url;                 // redial target (unticketed base URL)
  #WS;                  // WebSocket constructor
  #token;               // bearer token for ticket fetches
  #fetch;               // fetch impl for ticket fetches
  #closed = false;      // close() called — stop reconnecting
  #connected;           // Promise resolved when the link is up
  #markConnected;

  constructor(ws, url, WS, token, fetchFn) {
    this.#url = url;
    this.#WS = WS;
    this.#token = token;
    this.#fetch = fetchFn;
    this.#connected = new Promise((r) => (this.#markConnected = r));
    this.#attach(ws);
    // connect() only resolves after "open" — the link is already up.
    this.#markConnected();
  }

  #attach(ws) {
    this.#ws = ws;
    ws.addEventListener("message", (e) => this.#onMessage(e.data));
    // Guard on identity: a failed redial's close must not retrigger this.
    ws.addEventListener("close", () => { if (this.#ws === ws) this.#onClose(); });
    ws.addEventListener("error", () => { if (this.#ws === ws) this.#onClose(); });
  }

  /** Connect to a damond WS endpoint. When `token` is set, a single-use
   *  ticket is fetched from POST /v1/ws_ticket (Bearer auth) and sent as
   *  ?ticket= — the token never appears in a URL. If the link drops the
   *  client redials with backoff (100ms → 5s, fresh ticket each attempt);
   *  calls made while down wait up to 10s for the link. */
  static async connect(url, { token, webSocket, fetchImpl } = {}) {
    const WS = webSocket ?? globalThis.WebSocket;
    if (!WS) {
      throw new Error(
        "no WebSocket implementation — use Node >= 22 or pass { webSocket }"
      );
    }
    const fetchFn = fetchImpl ?? globalThis.fetch;
    const wsUrl = token ? await ticketedUrl(url, token, fetchFn) : url;
    const ws = new WS(wsUrl);
    // Bound the handshake — a socket that never opens nor errors would
    // otherwise hang connect() forever.
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("ws connect timed out")), 10000);
      ws.addEventListener("open", () => { clearTimeout(t); resolve(); }, { once: true });
      ws.addEventListener("error", (e) => { clearTimeout(t); reject(e.error ?? new Error("ws connect failed")); }, { once: true });
    });
    return new DamonClient(ws, url, WS, token, fetchFn);
  }


  #onMessage(data) {
    // A malformed or binary frame must not kill the process — the Rust
    // server tolerates bad JSON the same way.
    let m;
    try {
      m = JSON.parse(typeof data === "string" ? data : data.toString());
    } catch {
      return;
    }
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
    // Detach FIRST: 'error' then 'close' both reach here, and without
    // this the listener's `this.#ws === ws` guard passes twice — two
    // redial loops, two live sockets, duplicated events.
    this.#ws = null;
    if (this.#closed) {
      const err = new Error("connection closed");
      for (const p of this.#pending.values()) p.reject(err);
      this.#pending.clear();
      this.#push(null); // end events() iterators
      return;
    }
    // Link dropped: fail pending calls, park future calls on a fresh
    // connected-promise, and redial with backoff until the daemon returns.
    const err = new Error("connection lost; reconnecting");
    for (const p of this.#pending.values()) p.reject(err);
    this.#pending.clear();
    this.#connected = new Promise((r) => (this.#markConnected = r));
    this.#push({ type: "disconnected" });
    this.#redial(100);
  }

  async #redial(delay) {
    while (!this.#closed) {
      await new Promise((r) => {
        const t = setTimeout(r, delay);
        t.unref?.(); // unref the backoff sleep so a post-close redial never keeps the process alive
      });
      if (this.#closed) return;
      try {
        // Fresh ticket per attempt — the last one was consumed or expired.
        const u = this.#token ? await ticketedUrl(this.#url, this.#token, this.#fetch) : this.#url;
        const ws = new this.#WS(u);
        // Same 10s gate as connect() — a socket that never opens nor
        // errors would stall the whole loop; the timeout rejects into
        // the catch below and counts as a failed attempt.
        let timer;
        try {
          await Promise.race([
            new Promise((resolve, reject) => {
              ws.addEventListener("open", resolve, { once: true });
              ws.addEventListener("error", (e) => reject(e.error ?? new Error("ws connect failed")), { once: true });
            }),
            new Promise((_, reject) => {
              timer = setTimeout(() => reject(new Error("ws connect timed out")), 10000);
            }),
          ]);
        } finally {
          clearTimeout(timer);
          // A timed-out handshake leaves the socket CONNECTING — abandon it or it opens unnoticed and leaks.
          if (ws.readyState === 0 || ws.readyState === 2) { try { ws.close(); } catch {} }
        }
        this.#attach(ws);
        // close() may have run while this socket was connecting — the
        // loop's #closed check already passed, so re-check here or the
        // fresh socket leaks open on a closed client.
        if (this.#closed) { ws.close(); return; }
        this.#markConnected();
        this.#push({ type: "reconnected" });
        return;
      } catch {
        delay = Math.min(delay * 2, 5000);
      }
    }
  }

  #push(ev) {
    const w = this.#waiters.shift();
    if (w) w(ev);
    else {
      this.#queue.push(ev);
      // A client that prompts but never iterates events() must not grow
      // this buffer without bound. Drop the OLDEST non-request event —
      // a dropped permission request leaves the daemon waiting out its
      // timeout for an answer that never comes.
      if (this.#queue.length > 8192) {
        const i = this.#queue.findIndex((e) => e?.type !== "request");
        this.#queue.splice(i === -1 ? 0 : i, 1);
      }
    }
  }

  /** Wait for the link (reconnect in flight) up to 10s. Every outbound
   *  frame goes through this gate — sending on a closing socket throws
   *  and silently loses the message. */
  async #ready() {
    // Fail fast once closed: #connected never resolves post-close, so this call would sit out the full 10s below.
    if (this.#closed) throw new Error("connection closed");
    let timer;
    try {
      await Promise.race([
        this.#connected,
        new Promise((_, rej) => {
          timer = setTimeout(() => rej(new Error("timed out waiting for reconnect")), 10000);
        }),
      ]);
    } finally {
      // Race losers must not leave a live 10s timer behind on every call.
      clearTimeout(timer);
    }
  }

  async #call(method, params) {
    await this.#ready();
    const id = ++this.#nextId;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      // The socket can close between #ready() and send — a sync throw
      // would leave the pending entry registered forever.
      try {
        this.#ws.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
      } catch (e) {
        this.#pending.delete(id);
        reject(e);
      }
    });
  }

  /** Respond to a server-initiated request (permission prompts). */
  async respond(id, result) {
    await this.#ready();
    // Same close-window guard as #call — a sync throw on a dead socket
    // must surface as a clean error, not an unhandled TypeError.
    try {
      this.#ws.send(JSON.stringify({ jsonrpc: "2.0", id, result }));
    } catch {
      throw new Error("connection closed");
    }
  }

  /** Async iterator over daemon events: {type:"update"|"request"|"notification"|"promptDone"|"disconnected"|"reconnected"}.
   *  Survives reconnects; ends only after close(). */
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

  /** List sessions: [{sessionId, createdAt, model}].
   *  Pass {limit, offset} to page. */
  async listSessions({ limit, offset } = {}) {
    const r = await this.#call("session/list", { limit, offset });
    return r.sessions;
  }

  /** Session history: [{role, content, ...}]. Pass {limit, offset} to page. */
  async sessionMessages(sessionId, { limit, offset } = {}) {
    const r = await this.#call("session/messages", { sessionId, limit, offset });
    return r.messages;
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
    await this.#ready();
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
    // Same close-window guard as #call — a sync throw on a dead socket
    // must not leave the id registered in #pending forever.
    try {
      this.#ws.send(JSON.stringify({
        jsonrpc: "2.0", id, method: "session/prompt",
        params: { sessionId, model, prompt: [{ type: "text", text }] },
      }));
    } catch (e) {
      this.#pending.delete(id);
      throw e;
    }
    return done;
  }
  async cancel(sessionId) {
    await this.#ready();
    try {
      this.#ws.send(JSON.stringify({
        jsonrpc: "2.0", method: "session/cancel", params: { sessionId },
      }));
    } catch {
      throw new Error("connection closed");
    }
  }

  /** Force a context compaction on the session — the manual escape hatch
   *  when a session wedges against the real context window while the
   *  daemon's estimate still reads under the threshold.
   *  Returns { compacted, compactedThrough?, reason? }. */
  async compactSession(sessionId) {
    return this.#call("session/compact", { sessionId });
  }

  close() {
    this.#closed = true;
    // #ws is null inside a reconnect window — guard the deref, then end
    // events() iterators and reject pending calls ourselves: no live
    // socket means no close event will ever drive #onClose's cleanup.
    this.#ws?.close();
    const err = new Error("connection closed");
    for (const p of this.#pending.values()) p.reject(err);
    this.#pending.clear();
    this.#push(null);
  }
}

export class RpcError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

/** Exchange the bearer token for a single-use WS ticket and return the
 *  ?ticket= URL. ws(s)://host/ws → http(s)://host/v1/ws_ticket. */
async function ticketedUrl(wsUrl, token, fetchFn) {
  // Parse instead of regex surgery — a query string (`/ws?x=1`) would
  // defeat the `/ws` path check and pollute the ticket URL.
  const base = new URL(wsUrl);
  base.pathname = base.pathname.replace(/\/+$/, ""); // trailing slashes → /ws/v1/ws_ticket 404s
  if (!base.pathname.endsWith("/ws")) {
    throw new Error(`expected a ws(s) URL ending in /ws, got: ${wsUrl}`);
  }
  if (base.protocol === "wss:") base.protocol = "https:";
  else if (base.protocol === "ws:") base.protocol = "http:";
  // Preserve the path prefix before /ws (/sub/ws → /sub/v1/ws_ticket), matching Rust client.rs:411-416.
  const resp = await fetchFn(`${base.origin}${base.pathname.slice(0, -3)}/v1/ws_ticket`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}` },
  });
  if (!resp.ok) throw new Error(`ws_ticket rejected: ${resp.status}`);
  const { ticket } = await resp.json();
  const sep = base.search ? "&" : "?";
  return `${wsUrl}${sep}ticket=${ticket}`;
}
