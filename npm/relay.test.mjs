// Relay-client tests: the browser's DamonRelay handshake must agree
// with the Rust daemon byte-for-byte. Crypto pieces are cross-checked
// against Node's native implementations; the E2E spawns a real
// `damon-relay` + `damond` pair and drives the full handshake + RPC
// round trip (skipped when the binaries aren't built).
import test from "node:test";
import assert from "node:assert";
import { readFileSync, existsSync, mkdtempSync, writeFileSync } from "node:fs";
import {
  createCipheriv,
  createDecipheriv,
  createPrivateKey,
  createPublicKey,
  diffieHellman,
  randomBytes,
} from "node:crypto";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

// The relay client is a classic script exposing globalThis.DamonRelay —
// load it the same way a browser would.
(0, eval)(readFileSync(join(ROOT, "src", "relay-client.js"), "utf8"));
const DamonRelay = globalThis.DamonRelay;
const I = DamonRelay._internals;

test("sha256 matches the known vector", () => {
  assert.equal(
    I.sha256Hex(Buffer.from("abc")),
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
  );
});

test("proofs match the vectors asserted against the Rust implementation", () => {
  const mine = new Uint8Array(32).fill(1);
  const theirs = new Uint8Array(32).fill(3);
  assert.equal(
    I.proofLegacy("tok", mine, theirs),
    "S8mawcxH7YVZq1N0D+PC7Q48QoULbq/LSh/T0S+p2mI=",
  );
  assert.equal(
    I.proofStretched("tok", mine, theirs),
    "pEQzR8r3Ev4sJ+cFta1ZJEpgGRynd3Ix9EFoElBMBqQ=",
  );
});

test("x25519 agrees with Node's native implementation", () => {
  for (let i = 0; i < 3; i++) {
    const sk = randomBytes(32);
    const priv = createPrivateKey({
      key: Buffer.concat([X25519_PKCS8, sk]),
      format: "der",
      type: "pkcs8",
    });
    const pub = createPublicKey(priv).export({ type: "spki", format: "der" }).slice(-32);
    assert.deepEqual(
      Buffer.from(I.x25519(sk, I.BASE9)),
      pub,
      "public key derivation differs from OpenSSL",
    );
    const other = randomBytes(32);
    const otherPriv = createPrivateKey({
      key: Buffer.concat([X25519_PKCS8, other]),
      format: "der",
      type: "pkcs8",
    });
    const otherPub = createPublicKey(otherPriv).export({ type: "spki", format: "der" }).slice(-32);
    assert.deepEqual(
      Buffer.from(I.x25519(sk, otherPub)),
      diffieHellman({ privateKey: priv, publicKey: createPublicKey(otherPriv) }),
      "DH differs from OpenSSL",
    );
  }
});
const X25519_PKCS8 = Buffer.from("302e020100300506032b656e04220420", "hex");

test("aes-256-gcm agrees with Node's native implementation (both directions)", () => {
  for (const size of [0, 1, 15, 16, 17, 100, 1237, 65536]) {
    const key = randomBytes(32), nonce = randomBytes(12), pt = randomBytes(size);
    const sealed = Buffer.from(I.aesGcmSeal(key, nonce, pt));
    const d = createDecipheriv("aes-256-gcm", key, nonce);
    d.setAuthTag(sealed.subarray(sealed.length - 16));
    const out = Buffer.concat([d.update(sealed.subarray(0, sealed.length - 16)), d.final()]);
    assert.deepEqual(out, Buffer.from(pt), `seal mismatch at ${size} bytes`);
    const c = createCipheriv("aes-256-gcm", key, nonce);
    const ct = Buffer.concat([c.update(pt), c.final()]);
    const opened = I.aesGcmOpen(key, nonce, Buffer.concat([ct, c.getAuthTag()]));
    assert.deepEqual(Buffer.from(opened), Buffer.from(pt), `open mismatch at ${size} bytes`);
  }
  // A flipped ciphertext bit must fail the tag.
  const sealed = Buffer.from(I.aesGcmSeal(randomBytes(32), randomBytes(12), randomBytes(64)));
  sealed[5] ^= 1;
  assert.throws(() => I.aesGcmOpen(randomBytes(32), randomBytes(12), sealed));
});

// --- E2E against the real binaries -----------------------------------------

const RELAY_BIN = [join(ROOT, "target", "debug", "damon-relay"), join(ROOT, "target", "release", "damon-relay")].find(existsSync);
const DAMOND_BIN = [join(ROOT, "target", "debug", "damond"), join(ROOT, "target", "release", "damond")].find(existsSync);

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const port = srv.address().port;
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

function waitClose(child, timeoutMs) {
  return new Promise((resolve) => {
    const t = setTimeout(() => resolve("timeout"), timeoutMs);
    child.once("exit", (code) => { clearTimeout(t); resolve(code); });
  });
}

/** Connect and resolve on open; rejects with the close reason on failure. */
function relayConnect(opts, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("connect timed out")), timeoutMs);
    const shim = DamonRelay.connect(opts);
    shim.onopen = () => { clearTimeout(timer); resolve(shim); };
    shim.onclose = (ev) => { clearTimeout(timer); reject(new Error(ev?.reason || "closed")); };
  });
}

test("E2E: browser relay client ↔ real damond + damon-relay", { skip: !RELAY_BIN || !DAMOND_BIN }, async () => {
  const relayPort = await freePort();
  const daemonPort = await freePort();
  const dir = mkdtempSync(join(tmpdir(), "damon-relay-e2e-"));
  const relay = spawn(RELAY_BIN, [], {
    env: { ...process.env, DAMON_RELAY_BIND: `127.0.0.1:${relayPort}`, RUST_LOG: "warn" },
    stdio: "ignore",
  });
  const config = [
    `bind = "127.0.0.1:${daemonPort}"`,
    `auth_token = "e2e-relay-token"`,
    `data_dir = ${JSON.stringify(join(dir, "data"))}`,
    `[relay]`,
    `url = "ws://127.0.0.1:${relayPort}"`,
    `name = "js-e2e"`,
    "",
  ].join("\n");
  const cfgPath = join(dir, "config.toml");
  writeFileSync(cfgPath, config);
  const daemon = spawn(DAMOND_BIN, ["--config", cfgPath], { stdio: "ignore" });
  try {
    // Wait for the daemon's dial-out registration to land (5s retry cadence).
    let shim = null;
    for (let i = 0; i < 20 && !shim; i++) {
      try {
        shim = await relayConnect({ url: `ws://127.0.0.1:${relayPort}`, name: "js-e2e", token: "e2e-relay-token" }, 3000);
      } catch { await new Promise(r => setTimeout(r, 700)); }
    }
    assert.ok(shim, "handshake never succeeded — daemon not registered?");

    // Full RPC round trip over the encrypted tunnel.
    const reply = await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("no hello reply")), 10000);
      shim.onmessage = (ev) => {
        const v = JSON.parse(ev.data);
        if (v.id === 1) { clearTimeout(t); resolve(v); }
      };
      shim.send(JSON.stringify({ id: 1, method: "hello", params: {} }));
    });
    assert.equal(reply.result.protocol, 2);
    assert.ok(reply.result.daemon === "damond");

    // A second round trip — seqs advance correctly past the first frame.
    const reply2 = await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("no backend.list reply")), 10000);
      shim.onmessage = (ev) => {
        const v = JSON.parse(ev.data);
        if (v.id === 2) { clearTimeout(t); resolve(v); }
      };
      shim.send(JSON.stringify({ id: 2, method: "backend.list", params: {} }));
    });
    assert.ok(Array.isArray(reply2.result.backends));

    // A wrong token must be rejected at the handshake — no retry loop.
    await assert.rejects(
      () => relayConnect({ url: `ws://127.0.0.1:${relayPort}`, name: "js-e2e", token: "wrong-token" }, 15000),
      /auth|proof/i,
    );

    shim.close();
  } finally {
    daemon.kill();
    relay.kill();
    await waitClose(daemon, 3000);
    await waitClose(relay, 3000);
  }
});
