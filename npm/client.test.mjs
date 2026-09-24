// Tests for the damon-agent client — node:test + node:assert, zero deps.
// The frame codec is tested as a pure unit against vectors recomputed with
// node:crypto (the same algorithms src/relay.rs uses), and the full relay
// path (handshake order, envelopes, reconnect re-handshake) is exercised
// against an in-process mock relay socket speaking the daemon side.
//
// Heavier E2E against a real `damon-relay` + `damond` pair is opt-in:
//   DAMON_E2E=ws://127.0.0.1:9471 DAMON_E2E_NAME=default DAMON_E2E_TOKEN=… npm test

import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { DamonClient, connectRelay, RpcError, __test } from "./client.mjs";

// --- pure-unit vectors ------------------------------------------------------

test("relayEncode: unreserved pass through, every other byte is %XX", () => {
  assert.equal(__test.relayEncode("home"), "home");
  assert.equal(__test.relayEncode("a b/c"), "a%20b%2Fc");
  assert.equal(__test.relayEncode("a-b_c.d~e"), "a-b_c.d~e");
  // '!' is NOT unreserved in relay.rs urlencoding — must be %21.
  assert.equal(__test.relayEncode("hi!"), "hi%21");
  assert.equal(__test.relayEncode("hi()*"), "hi%28%29%2A");
  // Non-ASCII encodes as UTF-8 bytes, byte-wise.
  assert.equal(__test.relayEncode("한"), "%ED%95%9C");
});

test("seqNonce: 4 zero bytes then the seq, big-endian", () => {
  assert.deepEqual(__test.seqNonce(0), new Uint8Array(12));
  assert.deepEqual(__test.seqNonce(1), new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]));
  assert.deepEqual(__test.seqNonce(0x0102), new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2]));
  assert.deepEqual(__test.seqNonce(2 ** 32), new Uint8Array([0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]));
});

test("proof: b64(sha256(token‖mine‖theirs)) — matches a node:crypto vector", async () => {
  const token = "sekrit-token";
  const mine = new Uint8Array(32).map((_, i) => i + 1);
  const theirs = new Uint8Array(32).map((_, i) => 255 - i);
  const expect = createHash("sha256").update(token).update(mine).update(theirs).digest("base64");
  assert.equal(await __test.proof(token, mine, theirs), expect);
  // The proof binds BOTH keys in order — swapping the inputs changes it.
  assert.notEqual(await __test.proof(token, theirs, mine), expect);
});

test("deriveKeys: sha256(shared‖label) — matches a node:crypto vector", async () => {
  const shared = new Uint8Array(32).map((_, i) => (i * 7) % 256);
  for (const label of ["d2c", "c2d"]) {
    const expect = new Uint8Array(createHash("sha256").update(shared).update(label).digest());
    assert.deepEqual(await __test.deriveKeys(shared, label), expect);
  }
  // Direction separation: the two labels must yield different keys.
  assert.notDeepEqual(await __test.deriveKeys(shared, "d2c"), await __test.deriveKeys(shared, "c2d"));
});

// --- codec round-trip + fail-closed behavior -------------------------------

test("E2E codec: both directions round-trip; tamper/replay/gap fail closed", async () => {
  // Two self-generated peers (the "daemon" is just the other keypair).
  const cKp = await __test.x25519Keypair();
  const dKp = await __test.x25519Keypair();
  const sharedC = await __test.x25519Shared(cKp.privateKey, dKp.publicKey);
  const sharedD = await __test.x25519Shared(dKp.privateKey, cKp.publicKey);
  assert.deepEqual(sharedC, sharedD); // identical DH output on both ends

  const c2d = await __test.deriveKeys(sharedC, "c2d");
  const d2c = await __test.deriveKeys(sharedC, "d2c");
  const cSend = await __test.importAesKey(c2d, ["encrypt"]);
  const dRecv = await __test.importAesKey(c2d, ["decrypt"]);
  const dSend = await __test.importAesKey(d2c, ["encrypt"]);
  const cRecv = await __test.importAesKey(d2c, ["decrypt"]);

  const req = JSON.stringify({ id: 1, method: "hello", params: {} });
  const f0 = await __test.sealFrame(cSend, 0, req);
  assert.equal((await __test.openFrame(dRecv, 0, f0)).text, req);

  const note = JSON.stringify({ event: "session.event", sessionId: "s1", data: { type: "timeline" } });
  const r0 = await __test.sealFrame(dSend, 0, note);
  assert.equal((await __test.openFrame(cRecv, 0, r0)).text, note);

  // Frame layout: seq(8B BE) ‖ nonce(12B) ‖ ct+tag(16B).
  const raw = __test.unb64(f0);
  assert.equal(raw.length, 20 + req.length + 16); // ascii plaintext → 1 byte/char
  assert.deepEqual(raw.subarray(0, 8), new Uint8Array(8));
  assert.deepEqual(raw.subarray(8, 20), __test.seqNonce(0));

  // Reflection: a frame sent back must not decrypt under the other direction's key.
  await assert.rejects(openFrameWrong(cRecv, f0), /decrypt failed/);

  // Tamper: a flipped ciphertext byte must fail the GCM tag.
  const tampered = __test.unb64(f0);
  tampered[25] ^= 0xff;
  await assert.rejects(__test.openFrame(dRecv, 0, __test.b64(tampered)), /decrypt failed/);

  // Replay: the same frame against an advanced expectation.
  await assert.rejects(__test.openFrame(dRecv, 1, f0), /frame seq 0 != expected 1/);
  // Gap: seq 2 arriving when 1 is expected.
  const f2 = await __test.sealFrame(cSend, 2, req);
  await assert.rejects(__test.openFrame(dRecv, 1, f2), /frame seq 2 != expected 1/);
  // Truncated and non-base64 frames.
  await assert.rejects(__test.openFrame(dRecv, 0, __test.b64(new Uint8Array(10))), /frame too short/);
  await assert.rejects(__test.openFrame(dRecv, 0, "!!!"));

  // Low-order public key (all-zero point): Node's WebCrypto rejects the
  // derivation itself; on runtimes that don't, the explicit all-zero check
  // in x25519Shared does. Either layer failing closed is the contract.
  await assert.rejects(
    __test.x25519Shared(cKp.privateKey, await __test.importX25519Public(new Uint8Array(32)))
  );
});

async function openFrameWrong(key, frame) {
  // expected seq 0 matches the frame's own seq, so only the key can fail.
  return __test.openFrame(key, 0, frame);
}

// --- full relay path against an in-process mock relay ------------------------

/** Minimal WHATWG-ish WebSocket double: instant-open, buffers sends until a
 *  test attaches a handler, lets the test fire events and drop the link. */
function mockRelayClass(sink) {
  return class MockRelaySocket {
    constructor(url) {
      this.url = url;
      this.readyState = 1;
      this.listeners = {};
      this.sent = []; // frames the client wrote (wire order)
      this.onsend = null;
      sink.push(this);
      queueMicrotask(() => this.fire("open"));
    }
    addEventListener(type, cb) { (this.listeners[type] ??= []).push(cb); }
    removeEventListener() {}
    fire(type, ev) { for (const cb of [...(this.listeners[type] ?? [])]) cb(ev ?? {}); }
    send(s) {
      if (this.readyState !== 1) throw new Error("WebSocket is already in CLOSING or CLOSED state.");
      this.sent.push(s);
      this.onsend?.(s);
    }
    close() {
      if (this.readyState === 3) return;
      this.readyState = 3;
      this.fire("close");
    }
    /** Test-side: a frame arriving from the relay/daemon. */
    serverSend(str) { this.fire("message", { data: str }); }
  };
}

/** The daemon side of the relay protocol (relay.rs daemon_handshake):
 *  answers the client's pub, verifies the client's proof FIRST, then sends
 *  its own; afterwards decrypts requests and answers with sealed frames. */
function makeDaemon(token, log) {
  return async function attach(ws) {
    const st = { clientPub: null, sendKey: null, recvKey: null, sendSeq: 0, recvSeq: 0 };
    log.states.push(st);
    const kp = await __test.x25519Keypair();
    const dPub = new Uint8Array(await globalThis.crypto.subtle.exportKey("raw", kp.publicKey));
    const reply = (obj) => ws.serverSend(JSON.stringify({ data: JSON.stringify(obj) }));
    const send = async (text) => {
      const seq = st.sendSeq++;
      ws.serverSend(JSON.stringify({ data: await __test.sealFrame(st.sendKey, seq, text) }));
    };
    const handle = async (wire) => {
      let env;
      try { env = JSON.parse(wire); } catch { return; }
      const inner = env?.data;
      if (typeof inner !== "string") return;
      let m = null;
      try { m = JSON.parse(inner); } catch { /* ciphertext */ }
      if (m && typeof m.e2e_pub === "string") {
        log.frames.push("pub");
        st.clientPub = __test.unb64(m.e2e_pub);
        log.clientPubs.push(Buffer.from(st.clientPub).toString("base64"));
        reply({ e2e_pub: __test.b64(dPub) }); // bare pub — no proof yet
        return;
      }
      if (m && typeof m.e2e_proof === "string") {
        log.frames.push("proof");
        const expected = await __test.proof(token, st.clientPub, dPub);
        if (!__test.constantTimeEq(m.e2e_proof, expected)) {
          log.proofRejected = true;
          reply({ e2e_error: "auth" });
          return;
        }
        const shared = await __test.x25519Shared(
          kp.privateKey, await __test.importX25519Public(st.clientPub)
        );
        st.sendKey = await __test.importAesKey(await __test.deriveKeys(shared, "d2c"), ["encrypt"]);
        st.recvKey = await __test.importAesKey(await __test.deriveKeys(shared, "c2d"), ["decrypt"]);
        log.handshakes += 1;
        reply({ e2e_proof: await __test.proof(token, dPub, st.clientPub) });
        // Daemon pushes its hello + one session.event right after the
        // handshake, before any request — exercises the decrypt
        // pipeline's queued handover.
        await send(JSON.stringify({
          hello: { protocol: 2, daemon: "damond", version: "0.0.0-test" },
        }));
        await send(JSON.stringify({
          event: "session.event", sessionId: "s1",
          data: { type: "timeline", kind: "assistant_message", text: "hello" },
        }));
        return;
      }
      log.frames.push("ct");
      // Encrypted JSON-RPC frame: strict-sequence decrypt, then answer.
      const { text } = await __test.openFrame(st.recvKey, st.recvSeq++, inner);
      log.plaintext.push(text);
      const req = JSON.parse(text);
      await send(JSON.stringify({
        id: req.id, result: { protocol: 2, daemon: "damond", version: "0.0.0-test" },
      }));
    };
    const onFrame = (wire) => {
      st.chain = (st.chain ?? Promise.resolve()).then(() => handle(wire)).catch((e) => {
        log.errors.push(String(e));
      });
    };
    ws.onsend = onFrame;
    for (const f of ws.sent.splice(0)) onFrame(f); // frames buffered pre-attach
  };
}

/** Drain a client's events() into an array with waiter support. */
function collect(client) {
  const seen = [];
  const waiters = [];
  const done = (async () => {
    for await (const ev of client.events()) {
      seen.push(ev);
      for (let i = waiters.length - 1; i >= 0; i--) {
        if (waiters[i].pred(ev)) {
          waiters[i].resolve(ev);
          waiters.splice(i, 1);
        }
      }
    }
  })();
  return {
    seen,
    done,
    waitFor: (pred, ms = 5000) => {
      const hit = seen.find(pred);
      if (hit) return Promise.resolve(hit);
      return new Promise((resolve, reject) => {
        const t = setTimeout(
          () => reject(new Error(`timeout waiting for event; seen: ${seen.map((e) => e?.type).join(",")}`)),
          ms
        );
        waiters.push({ pred, resolve: (ev) => { clearTimeout(t); resolve(ev); } });
      });
    },
  };
}

async function until(cond, what, ms = 5000) {
  const deadline = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`timeout: ${what}`);
    await new Promise((r) => setTimeout(r, 10));
  }
}

function freshLog() {
  return { handshakes: 0, clientPubs: [], plaintext: [], frames: [], states: [], errors: [] };
}

test("connectRelay: E2E handshake + encrypted RPC over a mock relay", async () => {
  const TOKEN = "test-token-0123456789abcdef";
  const sockets = [];
  const log = freshLog();
  const attach = makeDaemon(TOKEN, log);
  const RelayWs = mockRelayClass(sockets);

  const p = connectRelay({ url: "ws://relay.example:9471/", name: "home", token: TOKEN, webSocket: RelayWs });
  assert.equal(sockets.length, 1); // dialed synchronously
  assert.equal(sockets[0].url, "ws://relay.example:9471/connect?name=home"); // trailing '/' trimmed
  attach(sockets[0]);
  const client = await p;

  // Message order: pub, proof, then ciphertext — client-proves-first.
  assert.deepEqual(log.frames.slice(0, 2), ["pub", "proof"]);
  assert.equal(log.handshakes, 1);

  // The wire carried ONLY {"data": "<base64 ct>"} envelopes — no plaintext.
  const c = collect(client);
  const init = await client.hello();
  assert.equal(init.protocol, 2);
  assert.ok(log.plaintext.some((t) => t.includes('"hello"'))); // daemon did decrypt it
  const inner = sockets[0].sent.map((f) => JSON.parse(f).data);
  for (const f of sockets[0].sent) {
    assert.deepEqual(Object.keys(JSON.parse(f)), ["data"]); // the relay envelope only
  }
  // Handshake frames: bare JSON with the exact field names relay.rs uses.
  assert.deepEqual(Object.keys(JSON.parse(inner[0])), ["e2e_pub"]);
  assert.deepEqual(Object.keys(JSON.parse(inner[1])), ["e2e_proof"]);
  // Everything after: sealed frames, never plaintext.
  for (const d of inner.slice(2)) {
    assert.match(d, /^[A-Za-z0-9+/]+={0,2}$/);
    assert.ok(!d.includes('"method":"hello"'), "plaintext method leaked onto the wire");
    assert.ok(__test.unb64(d).length >= 20 + 16); // seq+nonce+ct+tag
  }
  // Exactly the handshake frames plus encrypted ones after — no stragglers.
  assert.deepEqual(log.frames, ["pub", "proof", "ct"]);

  // Daemon-pushed session.event arrives decrypted and decoded through events().
  const ev = await c.waitFor((e) => e?.type === "event");
  assert.equal(ev.sessionId, "s1");
  assert.equal(ev.event.kind, "assistant_message");
  assert.equal(ev.event.text, "hello");
  assert.equal(client.serverHello?.protocol, 2);

  client.close();
  await c.done; // events() iterator ends after close()
});

test("connectRelay: drop → redial re-runs the handshake with a fresh keypair", async () => {
  const TOKEN = "test-token-0123456789abcdef";
  const sockets = [];
  const log = freshLog();
  const attach = makeDaemon(TOKEN, log);
  const RelayWs = mockRelayClass(sockets);

  const p = connectRelay({ url: "ws://relay.example:9471", name: "home", token: TOKEN, webSocket: RelayWs });
  attach(sockets[0]);
  const client = await p;
  const c = collect(client);
  await client.hello();

  sockets[0].close(); // relay side drops us
  await c.waitFor((e) => e?.type === "disconnected");
  await until(() => sockets.length === 2, "second dial");
  attach(sockets[1]); // daemon behind the new pipe
  await c.waitFor((e) => e?.type === "reconnected");

  assert.equal(log.handshakes, 2);
  assert.notEqual(log.clientPubs[0], log.clientPubs[1]); // fresh X25519 keypair per attempt

  const r = await client.hello(); // same API over the new link
  assert.equal(r.protocol, 2);

  client.close();
  await c.done;
});

test("connectRelay: wrong token fails fast on the daemon's auth rejection", async () => {
  const sockets = [];
  const log = freshLog();
  const attach = makeDaemon("right-token", log);
  const RelayWs = mockRelayClass(sockets);

  const p = connectRelay({ url: "ws://relay.example:9471", name: "home", token: "wrong-token", webSocket: RelayWs });
  attach(sockets[0]);
  await assert.rejects(p, /wrong auth_token/);
  assert.equal(log.proofRejected, true);
  assert.equal(sockets[0].readyState, 3); // failed dial closed the socket — no slot leak
});

test("connectRelay: a silent relay fails at the handshake timeout", async () => {
  const sockets = [];
  const RelayWs = mockRelayClass(sockets); // nothing attached → no daemon answers

  await assert.rejects(
    connectRelay({
      url: "ws://relay.example:9471", name: "home", token: "t",
      webSocket: RelayWs, handshakeTimeoutMs: 80,
    }),
    /E2E handshake timed out/
  );
  assert.equal(sockets[0].readyState, 3);
});

test("DamonClient.connect: direct-WS path still round-trips plaintext and reconnects", async () => {
  // Guards the transport-seam refactor: identical mock socket, but the
  // direct path carries bare JSON-RPC (no envelopes, no crypto).
  const sockets = [];
  const Ws = mockRelayClass(sockets);
  const attach = (ws) => {
    ws.onsend = (s) => {
      const m = JSON.parse(s);
      ws.serverSend(JSON.stringify({ id: m.id, result: { pong: true } }));
    };
    for (const f of ws.sent.splice(0)) ws.onsend(f);
  };

  const p = DamonClient.connect("ws://127.0.0.1:9/ws", { webSocket: Ws });
  attach(sockets[0]);
  const client = await p;
  assert.equal(sockets[0].url, "ws://127.0.0.1:9/ws");
  assert.deepEqual(await client.hello(), { pong: true }); // plaintext in, plaintext out

  const c = collect(client);
  sockets[0].close();
  await c.waitFor((e) => e?.type === "disconnected");
  await until(() => sockets.length === 2, "redial");
  attach(sockets[1]);
  await c.waitFor((e) => e?.type === "reconnected");
  assert.deepEqual(await client.hello(), { pong: true });

  client.close();
  await c.done;
});

test("DamonClient.connect: token path fetches a single-use ticket into the dial URL", async () => {
  const sockets = [];
  const Ws = mockRelayClass(sockets);
  const fetchImpl = async (u, opts) => {
    assert.equal(u, "http://127.0.0.1:9/v1/ws_ticket");
    assert.equal(opts.method, "POST");
    assert.equal(opts.headers.authorization, "Bearer tk");
    return { ok: true, json: async () => ({ ticket: "T123" }) };
  };
  const p = DamonClient.connect("ws://127.0.0.1:9/ws", { token: "tk", webSocket: Ws, fetchImpl });
  await until(() => sockets.length === 1, "ticketed dial"); // the ticket fetch precedes the dial
  sockets[0].onsend = (s) => {
    const m = JSON.parse(s);
    sockets[0].serverSend(JSON.stringify({ id: m.id, result: {} }));
  };
  for (const f of sockets[0].sent.splice(0)) sockets[0].onsend(f);
  const client = await p;
  assert.equal(sockets[0].url, "ws://127.0.0.1:9/ws?ticket=T123");
  await client.hello();
  client.close();
});

test("public surface: connect/connectRelay attached + exported, RpcError intact", () => {
  assert.equal(typeof DamonClient.connect, "function");
  assert.equal(typeof DamonClient.connectRelay, "function");
  assert.equal(typeof connectRelay, "function");
  assert.equal(typeof RpcError, "function");
  const e = new RpcError(-32601, "method not found");
  assert.ok(e instanceof Error);
  assert.equal(e.code, -32601);
  assert.equal(e.message, "method not found");
});

test(
  "E2E: real damon-relay + daemon (opt-in: DAMON_E2E=ws://host:port [DAMON_E2E_NAME] [DAMON_E2E_TOKEN])",
  { skip: !process.env.DAMON_E2E },
  async () => {
    const client = await connectRelay({
      url: process.env.DAMON_E2E,
      name: process.env.DAMON_E2E_NAME ?? "default",
      token: process.env.DAMON_E2E_TOKEN ?? "",
    });
    try {
      const r = await client.hello();
      assert.ok(r);
    } finally {
      client.close();
    }
  }
);
