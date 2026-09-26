// damon-agent — minimal Node.js client for the damond WS protocol v2.
// Zero dependencies. Requires Node >= 22 (global WebSocket), or pass a
// WHATWG-style WebSocket constructor via `webSocket` (e.g. the `ws`
// package's WebSocket works too).
//
// Direct (loopback, or ticketed with a bearer token):
//   import { DamonClient } from "damon-agent";
//   const client = await DamonClient.connect("ws://127.0.0.1:9470/ws");
//   await client.hello();
//   const sessionId = await client.newSession(process.cwd(), "claude");
//   const turn = client.prompt(sessionId, "hi"); // resolves at turn end
//   for await (const ev of client.events()) {
//     if (ev.type === "event" && ev.event?.kind === "assistant_message")
//       process.stdout.write(ev.event.text);
//     if (ev.type === "event" && ev.event?.type === "permission_requested")
//       await client.respondPermission(ev.sessionId, ev.event.id, { behavior: "allow" });
//     if (ev.type === "promptDone") break;
//   }
//   await turn;
//
// Through a damon-relay (E2E-encrypted; mirrors Rust connect_relay):
//   import { connectRelay } from "damon-agent";
//   const client = await connectRelay({
//     url: "wss://relay.example.com", name: "home", token: process.env.DAMON_TOKEN,
//   });
//   // identical surface from here on — callers cannot tell transports apart.

export class DamonClient {
  #io;                 // live transport: { send(text), close() } calling back onText/onDown
  #dial;               // async () => fresh transport (ticket fetch / relay handshake included)
  #nextId = 0;
  #pending = new Map(); // id -> {resolve, reject}
  #queue = [];          // buffered events for events()
  #waiters = [];        // pending event consumers
  #closed = false;      // close() called — stop reconnecting
  serverHello = null;   // latest {"hello": …} greeting (sent on every connect)
  #connected;           // Promise resolved when the link is up
  #markConnected;
  #touched = new Set(); // session ids this client created/resumed/prompted
  #autoResume;          // re-issue session.resume for #touched after a redial

  constructor(io, dial, { autoResume = true } = {}) {
    this.#dial = dial;
    this.#autoResume = autoResume;
    this.#connected = new Promise((r) => (this.#markConnected = r));
    this.#attach(io);
    // connect()/connectRelay() only resolve after the link (including
    // any relay handshake) is fully up.
    this.#markConnected();
  }

  /** Fire-and-forget session.resume for every touched session after a
   *  redial — the daemon re-attaches the backend and replays missed
   *  events (replay:true), so subscriptions survive daemon restarts.
   *  Failures (e.g. the session was deleted meanwhile) are swallowed:
   *  the next caller-facing call surfaces its own errors. */
  #autoResubscribe() {
    if (!this.#autoResume) return;
    for (const sessionId of this.#touched) {
      this.#call("session.resume", { sessionId }).catch(() => {});
    }
  }

  #attach(io) {
    this.#io = io;
    io.onText = (text) => this.#onMessage(text);
    // Guard on identity: a failed redial's death must not retrigger this.
    io.onDown = () => { if (this.#io === io) this.#onClose(); };
  }

  /** Connect to a damond WS endpoint. When `token` is set, a single-use
   *  ticket is fetched from POST /v1/ws_ticket (Bearer auth) and sent as
   *  ?ticket= — the token never appears in a URL. If the link drops the
   *  client redials with backoff (100ms → 5s, fresh ticket each attempt);
   *  calls made while down wait up to 10s for the link. */
  static async connect(url, { token, webSocket, fetchImpl, autoResume } = {}) {
    const WS = webSocket ?? globalThis.WebSocket;
    if (!WS) {
      throw new Error(
        "no WebSocket implementation — use Node >= 22 or pass { webSocket }"
      );
    }
    const fetchFn = fetchImpl ?? globalThis.fetch;
    // Redial: fresh ticket + WS → new transport.
    const dial = () => dialWs(url, WS, token, fetchFn);
    return new DamonClient(await dial(), dial, { autoResume });
  }

  /** Connect through a damon-relay — E2E-encrypted, identical surface.
   *  See the module-level `connectRelay` docs for the handshake. */
  static connectRelay(opts) {
    return connectRelay(opts);
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
    // Server push: {"event":"session.event","sessionId","data":<StreamEvent>,
    // "replay":true?}. The replay mark lets consumers separate
    // catch-up history from live frames instead of double-rendering
    // after a reconnect — same field the Rust/Python clients surface.
    if (typeof m.event === "string") {
      this.#push({
        type: "event",
        sessionId: m.sessionId,
        event: m.data,
        replay: m.replay === true,
      });
      return;
    }
    // {"hello": {protocol, daemon, version, …}} — sent on every connect.
    if (m.hello && typeof m.hello === "object") this.serverHello = m.hello;
  }

  #onClose() {
    // Detach FIRST: 'error' then 'close' both reach here, and without
    // this the identity guard in #attach would pass twice — two redial
    // loops, two live transports, duplicated events.
    this.#io = null;
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
        // Full redial — for a relay this re-runs the whole E2E handshake
        // with a fresh X25519 keypair.
        const io = await this.#dial();
        this.#attach(io);
        // close() may have run while this link was coming up — the
        // loop's #closed check already passed, so re-check here or the
        // fresh link leaks open on a closed client.
        if (this.#closed) { io.close(); return; }
        this.#markConnected();
        this.#push({ type: "reconnected" });
        this.#autoResubscribe();
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
      // this buffer without bound. Drop the OLDEST event that isn't a
      // permission ask — a dropped permission_requested leaves the
      // daemon waiting out its timeout for an answer that never comes.
      if (this.#queue.length > 8192) {
        const i = this.#queue.findIndex(
          (e) => !(e?.type === "event" && e?.event?.type === "permission_requested")
        );
        this.#queue.splice(i === -1 ? 0 : i, 1);
      }
    }
  }

  /** Wait for the link (reconnect in flight) up to 10s. Every outbound
   *  frame goes through this gate — sending on a dying link throws
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

  /** Send one plaintext JSON-RPC frame over the live transport. Async
   *  because the relay transport encrypts before it hits the wire; a
   *  dead link rejects. */
  async #send(text) {
    if (!this.#io) throw new Error("connection closed");
    await this.#io.send(text);
  }

  async #call(method, params) {
    await this.#ready();
    const id = ++this.#nextId;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      // The link can close between #ready() and the send — a failure
      // would leave the pending entry registered forever.
      this.#send(JSON.stringify({ id, method, params }))
        .catch((e) => {
          this.#pending.delete(id);
          reject(e);
        });
    });
  }

  /** Answer a permission_requested event. `response` is a
   *  PermissionResponse: {behavior:"allow", action_id?, updated_input?}
   *  or {behavior:"deny", action_id?, message?, interrupt?}. */
  async respondPermission(sessionId, requestId, response) {
    return this.#call("permission.respond", { sessionId, requestId, response });
  }

  /** Async iterator over daemon events: {type:"event"|"promptDone"|"disconnected"|"reconnected"}.
   *  "event" carries {sessionId, event} where event is a StreamEvent —
   *  {turn_id?, type, …}; timeline items ride as {type:"timeline",
   *  kind:"assistant_message"|"tool_call"|…}. Survives reconnects;
   *  ends only after close(). */

  async *events() {
    for (;;) {
      const ev = this.#queue.length
        ? this.#queue.shift()
        : await new Promise((r) => this.#waiters.push(r));
      if (ev === null) return;
      yield ev;
    }
  }

  /** Handshake → {protocol, daemon, version, backends[], methods[]}. The
   *  same payload also arrives unsolicited on every (re)connect and is
   *  kept on `serverHello`. */
  hello() {
    return this.#call("hello");
  }

  /** Create a session; returns sessionId. `backend` (optional) picks the
   *  agent backend — a catalog id like "claude"; omitted → the daemon's
   *  default backend. */
  async newSession(cwd, backend) {
    const r = await this.#call("session.create", { cwd, backend });
    this.#touched.add(r.sessionId);
    return r.sessionId;
  }

  /** List sessions: [{sessionId, createdAt, backend, title}].
   *  Pass {limit, offset} to page. */
  async listSessions({ limit, offset } = {}) {
    const r = await this.#call("session.list", { limit, offset });
    return r.sessions;
  }

  /** Session history: [{role, content, ...}]. Pass {limit, offset} to page. */
  async sessionMessages(sessionId, { limit, offset } = {}) {
    const r = await this.#call("session.messages", { sessionId, limit, offset });
    return r.messages;
  }

  /** Resume an existing session; returns sessionId. Throws if unknown. */
  async resumeSession(sessionId) {
    const r = await this.#call("session.resume", { sessionId });
    this.#touched.add(sessionId);
    return r.sessionId;
  }

  /** Resume a native session by its persistence handle — the import
   *  path for sessions the backend made outside the daemon. Returns
   *  the Damon sessionId (deduped: same handle → same session). */
  async resumeByHandle(handle, { title, cwd } = {}) {
    const r = await this.#call("session.resume", { handle, title, cwd });
    this.#touched.add(r.sessionId);
    return r.sessionId;
  }

  /** Sessions importable from a backend's native store. */
  async importSessions(backend, cwd) {
    const r = await this.#call("session.import", { backend, cwd });
    return r.sessions;
  }

  /** Fork a session: copy the session row and its messages into a new
   *  session, optionally only up to and including `upto` (an original
   *  message id). The agent's own session id is NOT copied — the fork
   *  attaches a fresh agent session on first prompt. Returns the new
   *  sessionId. */
  async forkSession(sessionId, uptoMessageId) {
    const r = await this.#call("session.fork", { sessionId, upto: uptoMessageId });
    return r.sessionId;
  }

  async deleteSession(sessionId) {
    return this.#call("session.delete", { sessionId });
  }

  /** Rename a session (sets its display title). */
  async renameSession(sessionId, title) {
    return this.#call("session.rename", { sessionId, title });
  }

  /** Full-text search over all history: [{sessionId, messageId, snippet}]. */
  async search(query, limit = 10) {
    const r = await this.#call("session.search", { query, limit });
    return r.results;
  }

  /** Usage: {sessions} rollup without sessionId, or one session's
   *  totals ({contextUsed, contextSize, costUsd, turns}) with it. */
  async usage(sessionId) {
    return this.#call("session.usage", { sessionId });
  }

  /** Start a turn. Resolves with {turnId, stopReason, usage?} when the
   *  turn ends — events stream first, then this response. `prompt` is a
   *  string or an array of content blocks. The same outcome is also
   *  delivered as a {type:"promptDone"} event on the events() stream
   *  for consumers that only iterate events.
   *
   *  With `{ detach: true }` the call resolves immediately with
   *  {turnId, detached: true} and the turn keeps running on the daemon
   *  even if this client disconnects — the outcome arrives as stream
   *  events (or replay on the next connect) instead of this response. */
  async prompt(sessionId, text, timeoutSecs, { detach } = {}) {
    await this.#ready();
    this.#touched.add(sessionId);
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
    // Same close-window guard as #call — a failed send must not leave
    // the id registered in #pending forever.
    try {
      await this.#send(JSON.stringify({
        id, method: "turn.start",
        params: { sessionId, prompt: text, timeoutSecs, detach },
      }));
    } catch (e) {
      this.#pending.delete(id);
      throw e;
    }
    return done;
  }

  /** Cancel a live turn → {cancelled: true}. */
  async cancel(sessionId) {
    return this.#call("turn.cancel", { sessionId });
  }

  /** Steer a live turn with an additional prompt → {result}. */
  async steer(sessionId, prompt, expectedTurn) {
    return this.#call("turn.steer", { sessionId, prompt, expectedTurn });
  }

  /** Set the session's model. */
  async setModel(sessionId, model) {
    return this.#call("session.set_model", { sessionId, model });
  }

  /** Set the session's mode. */
  async setMode(sessionId, mode) {
    return this.#call("session.set_mode", { sessionId, mode });
  }

  /** List agent backends → [{id, available, capabilities}]. */
  async backends() {
    const r = await this.#call("backend.list");
    return r.backends;
  }

  /** Backend catalog → {models[], modes[], commands[]}. */
  async catalog(backend) {
    return this.#call("catalog.models", { backend });
  }

  close() {
    this.#closed = true;
    // #io is null inside a reconnect window — guard the deref, then end
    // events() iterators and reject pending calls ourselves: no live
    // link means no death callback will ever drive #onClose's cleanup.
    this.#io?.close();
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

// ---------------------------------------------------------------------------
// Transports — the seam shared by direct-WS and relay links
// ---------------------------------------------------------------------------

/** Wait for a WebSocket "open"; reject on error or after 10s. A socket
 *  still CONNECTING at the timeout is closed so it can't open unnoticed
 *  and leak. */
function openSocket(ws) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => {
      reject(new Error("ws connect timed out"));
      if (ws.readyState === 0 || ws.readyState === 2) { try { ws.close(); } catch {} }
    }, 10000);
    ws.addEventListener("open", () => { clearTimeout(t); resolve(ws); }, { once: true });
    ws.addEventListener("error", (e) => { clearTimeout(t); reject(e.error ?? new Error("ws connect failed")); }, { once: true });
  });
}

/** Adapt an open WebSocket into a transport carrying plaintext JSON-RPC
 *  text frames. DamonClient attaches onText/onDown right after creation —
 *  synchronously, before any event can fire. */
function wsIo(ws) {
  const io = {
    send: (text) => ws.send(text),
    close: () => ws.close(),
  };
  ws.addEventListener("message", (e) => io.onText?.(e.data));
  // 'error' then 'close' both fire on a dying link; the identity guard
  // DamonClient installs makes the second call a no-op.
  ws.addEventListener("close", () => io.onDown?.());
  ws.addEventListener("error", () => io.onDown?.());
  return io;
}

/** One direct-WS link: fresh ticket per attempt, dial, wait open. */
async function dialWs(url, WS, token, fetchFn) {
  // Fresh ticket per attempt — the last one was consumed or expired.
  const wsUrl = token ? await ticketedUrl(url, token, fetchFn) : url;
  return wsIo(await openSocket(new WS(wsUrl)));
}

// ---------------------------------------------------------------------------
// Relay transport — mirrors src/relay.rs (the protocol is normative there)
// ---------------------------------------------------------------------------

/** Connect through a damon-relay and return a DamonClient. The relay pipes
 *  WebSocket frames between the registered daemon and this client but
 *  never sees plaintext: after a 4-message E2E handshake (relay.rs
 *  client_handshake) every JSON-RPC frame is AES-256-GCM encrypted with
 *  direction-separated keys and strict sequence numbers.
 *
 *    1. client → {"e2e_pub": b64(X25519 public key), "kdf": "s256"}  (no proof yet)
 *    2. daemon → {"e2e_pub": b64(X25519 public key), "kdf": "s256"}  (bare; echoes kdf when understood)
 *    3. client → {"e2e_proof": b64(sha256(token ‖ client_pub ‖ daemon_pub))}
 *    4. daemon → {"e2e_proof": b64(sha256(token ‖ daemon_pub ‖ client_pub))}
 *               …or {"e2e_error": "auth"} so a wrong token fails fast.
 *
 *  When the daemon echoes `"kdf": "s256"` both sides stretch the proofs
 *  with 2^16 extra sha256 rounds (brute-force hardening for low-entropy
 *  tokens); older daemons ignore the marker and keep step 3/4 as-is.
 *
 *  All messages travel wrapped in the envelope the relay pipes:
 *  `{"data": "..."}`. Post-handshake frames are base64 of
 *  seq(8B big-endian) ‖ nonce(12B) ‖ ciphertext+tag, where the nonce is
 *  4 zero bytes ‖ seq and the keys are sha256(shared ‖ "c2d") (client →
 *  daemon) and sha256(shared ‖ "d2c") (daemon → client). Anything replayed,
 *  reordered, or tampered fails closed — the link dies and redials with a
 *  full fresh handshake, exactly like Rust connect_relay.
 *  Requires WebCrypto X25519 (Node >= 22, Chrome 133+, Safari 17+). */
export async function connectRelay({ url, name, token, webSocket, handshakeTimeoutMs, autoResume } = {}) {
  if (!url || typeof url !== "string") {
    throw new Error("connectRelay: url (the relay's ws://host:port) is required");
  }
  if (!name || typeof name !== "string") {
    throw new Error("connectRelay: name (the daemon's relay name) is required");
  }
  if (typeof token !== "string" || token.length === 0) {
    throw new Error("connectRelay: token (the daemon's auth_token) is required");
  }
  // Rust bounds the handshake at 15s (relay.rs HANDSHAKE_TIMEOUT).
  const dial = () => dialRelay(url, name, token, webSocket, handshakeTimeoutMs ?? 15000);
  return new DamonClient(await dial(), dial, { autoResume });
}

async function dialRelay(url, name, token, webSocket, timeoutMs) {
  const WS = webSocket ?? globalThis.WebSocket;
  if (!WS) {
    throw new Error(
      "no WebSocket implementation — use Node >= 22 or pass { webSocket }"
    );
  }
  // Same URL shape as relay.rs client_connect: trim trailing '/', then
  // /connect?name=<percent-encoded>. The token never appears in the URL —
  // it only ever travels inside the E2E proof.
  const connectUrl = `${url.replace(/\/+$/, "")}/connect?name=${relayEncode(name)}`;
  const ws = await openSocket(new WS(connectUrl));
  return relayIo(ws, token, timeoutMs);
}

/** Percent-encode a query value exactly like relay.rs `urlencoding`:
 *  RFC 3986 unreserved chars pass through, every other byte is %XX. */
function relayEncode(s) {
  return encodeURIComponent(s).replace(
    /[!'()*]/g,
    (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`
  );
}

/** Build the relay transport on an open socket: run the E2E handshake,
 *  then encrypt outbound / decrypt inbound JSON-RPC frames. Returns an
 *  io { send, close } that calls back onText(plaintext) / onDown(). */
async function relayIo(ws, token, timeoutMs) {
  const io = { send: null, close: null };
  let down = false;
  let pingTimer = null;
  const markDown = () => {
    if (down) return;
    down = true;
    clearInterval(pingTimer);
    try { ws.close(); } catch {}
    io.onDown?.();
  };

  // One message listener for the socket's life, phased: the handshake
  // queue consumes frames until the keys exist, the decrypt pipeline
  // after. Frames queued during the swap are drained in order.
  let handshakeQueue = [];
  let handshakeWaiter = null;
  let onCipher = null;
  ws.addEventListener("message", (e) => {
    if (down) return;
    let v;
    try {
      v = JSON.parse(typeof e.data === "string" ? e.data : e.data.toString());
    } catch {
      return;
    }
    // The relay pipes `{"data": ...}` envelopes; anything else (client-id
    // notices, relay errors) has no string data — ignore it, the socket
    // dying is what signals link death, same as Rust's read pump.
    if (typeof v?.data !== "string") return;
    if (onCipher) onCipher(v.data);
    else if (handshakeWaiter) { const w = handshakeWaiter; handshakeWaiter = null; w(v.data); }
    else handshakeQueue.push(v.data);
  });
  ws.addEventListener("close", markDown);
  ws.addEventListener("error", markDown);

  // Keepalive (mirrors relay.rs CLIENT_PING_EVERY): ping the relay
  // every 30s so NATs keep the flow open and the relay's own idle
  // check sees liveness. Node's ws exposes ping(); browsers don't —
  // there the relay's 120s idle drop triggers markDown and the
  pingTimer = setInterval(() => {
    if (down) return;
    try { ws.ping?.(); } catch {}
  }, 30_000);
  // The keepalive must never keep a process alive on its own (a failed
  // test that never closes the client would otherwise hang the runner).
  pingTimer.unref?.();

  // Every outbound message is wrapped in the envelope the relay pipes.
  const sendRaw = (text) => ws.send(JSON.stringify({ data: text }));

  const deadline = Date.now() + timeoutMs;
  const nextFrame = () => new Promise((resolve, reject) => {
    if (handshakeQueue.length) return resolve(handshakeQueue.shift());
    const left = deadline - Date.now();
    if (left <= 0) return reject(new Error("E2E handshake timed out"));
    const t = setTimeout(() => {
      handshakeWaiter = null;
      reject(new Error("E2E handshake timed out"));
    }, left);
    handshakeWaiter = (d) => { clearTimeout(t); resolve(d); };
  });

  let sendKey, recvKey;
  try {
    ({ sendKey, recvKey } = await e2eHandshake(sendRaw, nextFrame, token));
  } catch (e) {
    // Kill the socket — a live read half would pin the relay's per-IP
    // session slot after every failed attempt (mirrors Rust aborting the
    // read pump on handshake failure).
    markDown();
    throw e;
  }

  // Post-handshake. Sends are chained so wire order always matches
  // sequence order — a swap would fail the daemon's strict recv check
  // and kill the session. Receives decrypt strictly in arrival order and
  // fail closed on any tamper/replay/reorder.
  let sendSeq = 0, recvSeq = 0;
  let sendChain = Promise.resolve();
  let recvChain = Promise.resolve();
  const deliver = (b64Frame) => {
    recvChain = recvChain.then(async () => {
      try {
        const { text } = await openFrame(recvKey, recvSeq, b64Frame);
        recvSeq++;
        io.onText?.(text);
      } catch {
        // Tampered / replayed / reordered frame: fail closed (relay.rs
        // E2e) — drop the link; the supervisor redials and re-handshakes.
        markDown();
      }
    });
  };
  for (const q of handshakeQueue.splice(0)) deliver(q); // raced the swap
  onCipher = deliver;

  io.send = async (text) => {
    if (down) throw new Error("connection closed");
    const seq = sendSeq++;
    const p = sendChain.then(async () => {
      if (down) throw new Error("connection closed");
      sendRaw(await sealFrame(sendKey, seq, text));
    });
    sendChain = p.then(() => {}, () => {});
    await p;
  };
  io.close = markDown;
  return io;
}

/** Client side of the 4-message E2E handshake (relay.rs E2e). A fresh
 *  X25519 keypair per call — reconnect attempts never reuse keys. The
 *  first frame advertises the stretched-proof KDF (`kdf: "s256"`); a
 *  daemon that echoes it uses the stretched proof, anything older
 *  ignores the unknown field and both sides fall back to the legacy
 *  single-hash proof. Returns the direction-separated session keys
 *  (client sends with c2d, receives with d2c; the daemon mirrors). */
async function e2eHandshake(sendRaw, nextFrame, token) {
  // 1. Our ephemeral public key + KDF offer.
  const kp = await x25519Keypair();
  const mine = new Uint8Array(await subtle.exportKey("raw", kp.publicKey));
  sendRaw(JSON.stringify({ e2e_pub: b64(mine), kdf: PROOF_KDF }));

  // 2. Daemon's bare {e2e_pub} — no proof until we authenticate. An
  //    echoed kdf marker means stretched proofs both ways.
  const hs = JSON.parse(await nextFrame());
  const theirPubB64 = hs.e2e_pub;
  if (typeof theirPubB64 !== "string") {
    throw new Error("missing e2e_pub in daemon handshake");
  }
  const theirs = unb64(theirPubB64);
  if (theirs.length !== 32) throw new Error("bad pubkey len");
  const stretch = hs.kdf === PROOF_KDF;

  // 3. Our proof over (client_pub, daemon_pub) — the client proves FIRST,
  //    so the daemon never reveals token-derived material to a peer that
  //    hasn't authenticated (and a relay can't farm proofs per pubkey).
  sendRaw(JSON.stringify({ e2e_proof: await proof(token, mine, theirs, stretch) }));

  // 4. Daemon's proof — or an explicit auth rejection so a wrong token
  //    fails fast instead of timing out.
  const reply = JSON.parse(await nextFrame());
  if (reply.e2e_error === "auth") {
    throw new Error("daemon rejected our proof — wrong auth_token?");
  }
  if (
    typeof reply.e2e_proof !== "string" ||
    !constantTimeEq(reply.e2e_proof, await proof(token, theirs, mine, stretch))
  ) {
    throw new Error("daemon failed E2E proof — wrong auth_token?");
  }

  const shared = await x25519Shared(kp.privateKey, await importX25519Public(theirs));
  return {
    sendKey: await importAesKey(await deriveKeys(shared, "c2d"), ["encrypt"]),
    recvKey: await importAesKey(await deriveKeys(shared, "d2c"), ["decrypt"]),
  };
}

// --- WebCrypto X25519 ------------------------------------------------------

const subtle = globalThis.crypto?.subtle;

/** Detect the X25519 algorithm shape once. Prefers the Secure Curves
 *  "{name:'X25519'}" (Node >= 22, Chrome 133+, Safari 17+); falls back to
 *  ECDH-with-namedCurve for runtimes that only expose it that way. A full
 *  generate+derive self-test must pass before a shape is committed. */
let x25519Shape = null;
async function detectX25519() {
  if (x25519Shape) return x25519Shape;
  if (!subtle) {
    throw new Error(
      "WebCrypto (crypto.subtle) unavailable — the relay transport needs Node >= 22 or a secure browser context"
    );
  }
  const shapes = [
    { gen: { name: "X25519" }, derive: (pub) => ({ name: "X25519", public: pub }) },
    { gen: { name: "ECDH", namedCurve: "X25519" }, derive: (pub) => ({ name: "ECDH", public: pub }) },
  ];
  for (const shape of shapes) {
    try {
      const a = await subtle.generateKey(shape.gen, false, ["deriveBits"]);
      const b = await subtle.generateKey(shape.gen, false, ["deriveBits"]);
      const bits = new Uint8Array(
        await subtle.deriveBits(shape.derive(b.publicKey), a.privateKey, 256)
      );
      if (bits.length === 32) {
        x25519Shape = shape;
        return shape;
      }
    } catch {
      /* try the next shape */
    }
  }
  throw new Error(
    "X25519 is unavailable in this runtime's WebCrypto — the relay transport needs Node >= 22 or a Secure Curves browser (Chrome 133+ / Safari 17+)"
  );
}

async function x25519Keypair() {
  const { gen } = await detectX25519();
  return subtle.generateKey(gen, false, ["deriveBits"]);
}

async function importX25519Public(raw) {
  const { gen } = await detectX25519();
  return subtle.importKey("raw", raw, gen, false, []);
}

/** Raw X25519 shared secret. Rejects the all-zero (non-contributory)
 *  share a low-order public key yields — WebCrypto may or may not reject
 *  it itself, so check either way (mirrors x25519_dalek's
 *  was_contributory check in relay.rs). */
async function x25519Shared(privateKey, publicKey) {
  const { derive } = await detectX25519();
  const shared = new Uint8Array(await subtle.deriveBits(derive(publicKey), privateKey, 256));
  if (shared.length !== 32 || shared.every((b) => b === 0)) {
    throw new Error("non-contributory DH share");
  }
  return shared;
}

// --- Frame codec (pure; mirrors relay.rs proof/derive_key/seq_nonce/E2e) ---

/** Stretched-proof KDF marker — mirrors relay.rs PROOF_KDF. Opt-in via
 *  the first handshake frame; a daemon that echoes it runs the same
 *  stretched proof, older peers keep the single-hash proof below. */
const PROOF_KDF = "s256";

/** Extra sha256 rounds when `stretch` is set — mirrors relay.rs
 *  PROOF_ITERATIONS (2^16). Cheap per handshake, expensive per offline
 *  guess at a low-entropy token. */
const PROOF_ITERATIONS = 1 << 16;

/** sha256(token ‖ mine ‖ theirs), base64 — binds the proof to BOTH public
 *  keys, so a relay cannot replay a captured proof for other keys. With
 *  `stretch`, iterate sha256 PROOF_ITERATIONS more times over a
 *  domain-separated seed (exactly relay.rs proof_stretched). */
async function proof(token, mine, theirs, stretch = false) {
  let h = await sha256(te(token), mine, theirs);
  if (stretch) {
    h = await sha256(te("damon-relay-proof-s256"), te(token), mine, theirs);
    for (let i = 0; i < PROOF_ITERATIONS; i++) {
      h = await sha256(h);
    }
  }
  return b64(h);
}

/** sha256(shared ‖ label) — directional session key material
 *  (label "d2c" = daemon→client, "c2d" = client→daemon). */
async function deriveKeys(shared, label) {
  return sha256(shared, te(label));
}

async function importAesKey(raw32, usages) {
  return subtle.importKey("raw", raw32, { name: "AES-GCM" }, false, usages);
}

/** 12-byte AEAD nonce: 4 zero bytes ‖ seq (big-endian). */
function seqNonce(seq) {
  const n = new Uint8Array(12);
  new DataView(n.buffer).setBigUint64(4, BigInt(seq));
  return n;
}

/** Encrypt a JSON text frame → base64 of seq(8B big-endian) ‖ nonce(12B) ‖
 *  ct+tag. AES-256-GCM appends the 16-byte tag — the exact layout of the
 *  aes-gcm crate's encrypt in relay.rs. */
async function sealFrame(key, seq, plaintext) {
  const nonce = seqNonce(seq);
  const ct = new Uint8Array(
    await subtle.encrypt({ name: "AES-GCM", iv: nonce, tagLength: 128 }, key, te(plaintext))
  );
  const frame = new Uint8Array(20 + ct.length);
  new DataView(frame.buffer).setBigUint64(0, BigInt(seq));
  frame.set(nonce, 8);
  frame.set(ct, 20);
  return b64(frame);
}

/** Decrypt a base64 frame → { seq, text }. The frame's sequence number
 *  must equal `expected` — a replayed, reordered, or injected frame throws
 *  (callers fail closed), and so does a tampered ciphertext or bad base64. */
async function openFrame(key, expected, b64Frame) {
  const frame = unb64(b64Frame);
  if (frame.length < 20) throw new Error("frame too short");
  const seq = Number(new DataView(frame.buffer, frame.byteOffset, 8).getBigUint64(0));
  if (seq !== expected) {
    throw new Error(`frame seq ${seq} != expected ${expected} — replay or reorder`);
  }
  let pt;
  try {
    pt = await subtle.decrypt(
      { name: "AES-GCM", iv: frame.subarray(8, 20), tagLength: 128 },
      key,
      frame.subarray(20)
    );
  } catch {
    throw new Error("decrypt failed — wrong key or tampered");
  }
  return { seq, text: td(pt) };
}

/** Constant-time string equality — the proof comparison must not leak by
 *  early exit (mirrors config::constant_time_eq on the Rust side). */
function constantTimeEq(a, b) {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

// --- Encoding helpers (browser + Node compatible, zero deps) ---------------

const TE = new TextEncoder();
const TD = new TextDecoder();
const te = (s) => TE.encode(s);
const td = (b) => TD.decode(b);

async function sha256(...chunks) {
  const parts = chunks.map((c) => (typeof c === "string" ? te(c) : c));
  let len = 0;
  for (const p of parts) len += p.length;
  const all = new Uint8Array(len);
  let o = 0;
  for (const p of parts) { all.set(p, o); o += p.length; }
  return new Uint8Array(await subtle.digest("SHA-256", all));
}

/** STANDARD base64 (padded) — the same engine relay.rs uses (B64 = base64
 *  general_purpose::STANDARD). */
function b64(bytes) {
  let s = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    s += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(s);
}

function unb64(s) {
  const bin = atob(s); // throws on invalid input — callers treat it as fatal
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// ---------------------------------------------------------------------------

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
    // 15s cap matches the Rust client's ticket fetch — a hung POST must
    // not stall connect() (or a redial, which shares this path) forever.
    signal: AbortSignal.timeout(15000),
  });
  if (!resp.ok) throw new Error(`ws_ticket rejected: ${resp.status}`);
  const { ticket } = await resp.json();
  const sep = base.search ? "&" : "?";
  return `${wsUrl}${sep}ticket=${ticket}`;
}

/** Pure internals for tests — NOT a public or stable API. */
export const __test = {
  x25519Keypair, x25519Shared, importX25519Public,
  proof, deriveKeys, importAesKey, sealFrame, openFrame, seqNonce,
  b64, unb64, constantTimeEq, relayEncode,
};
