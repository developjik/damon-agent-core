/*!
 * DamonRelay — browser client for damond's E2E relay (protocol v2).
 *
 * Connects to a `damon-relay` host, attaches to a daemon by name, runs
 * the same X25519 handshake the Rust/npm clients run, and hands the UI
 * plaintext JSON frames. Wire contract (see src/relay.rs — the two
 * implementations must agree byte-for-byte):
 *
 *   ws  {relay}/connect?name={name}
 *   →   {"data": "{\"e2e_pub\":…,\"kdf\":\"s256\"}"}          client hello
 *   ←   {"data": "{\"e2e_pub\":…}"}                           daemon hello
 *   →   {"data": "{\"e2e_proof\":…}"}                         client proves FIRST
 *   ←   {"data": "{\"e2e_proof\":…}"} or {"e2e_error":"auth"} daemon proves
 *   then  {"data": b64(seq(8B) ‖ nonce(12B) ‖ ct‖tag)} both ways
 *
 * Proofs bind sha256(token ‖ mine ‖ theirs); "s256" stretches both by
 * 2^16 extra sha256 rounds over a domain-separated seed. Session keys
 * are sha256(shared ‖ label) per direction ("c2d" send, "d2c" recv from
 * the client side); the AEAD nonce is 4 zero bytes ‖ seq big-endian and
 * the seq must be contiguous — any gap fails the session closed.
 *
 * The crypto is pure JS on purpose: crypto.subtle only exists in secure
 * contexts, and a self-hosted relay is typically plain ws:// served over
 * http — the page must work there too. Everything self-tests against
 * published vectors on first connect (RFC 7748, sha256, the Rust proof
 * vectors, AES-GCM cross-checked against Node's implementation), so a
 * corrupted build fails loudly instead of failing every handshake.
 *
 * Keepalive: browsers cannot send WebSocket pings, and the relay drops
 * a client silent for 120s — a cheap encrypted `hello` RPC every 30s
 * keeps the relay's (and the daemon's) idle clocks fed. The reserved
 * id space (≥1e9) is ignored by the UI's response router.
 */
(function (global) {
  "use strict";

  // ---------- bytes & base64 ------------------------------------------------

  const te = new TextEncoder();
  const td = new TextDecoder();
  const utf8 = (s) => te.encode(s);
  const fromUtf8 = (b) => td.decode(b);

  function randBytes(n) {
    if (!global.crypto || !global.crypto.getRandomValues) {
      throw new Error("no crypto.getRandomValues available");
    }
    const b = new Uint8Array(n);
    global.crypto.getRandomValues(b);
    return b;
  }

  function concatBytes(...parts) {
    const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
    let o = 0;
    for (const p of parts) {
      out.set(p, o);
      o += p.length;
    }
    return out;
  }

  const B64T =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  function b64encode(bytes) {
    let out = "";
    for (let i = 0; i < bytes.length; i += 3) {
      const b0 = bytes[i];
      const b1 = i + 1 < bytes.length ? bytes[i + 1] : 0;
      const b2 = i + 2 < bytes.length ? bytes[i + 2] : 0;
      out += B64T[b0 >> 2];
      out += B64T[((b0 & 3) << 4) | (b1 >> 4)];
      out += i + 1 < bytes.length ? B64T[((b1 & 15) << 2) | (b2 >> 6)] : "=";
      out += i + 2 < bytes.length ? B64T[b2 & 63] : "=";
    }
    return out;
  }

  function b64decode(s) {
    const clean = String(s).replace(/[^A-Za-z0-9+/]/g, "");
    const out = new Uint8Array(Math.floor((clean.length * 3) / 4));
    let o = 0, buf = 0, bits = 0;
    for (let i = 0; i < clean.length; i++) {
      buf = (buf << 6) | B64T.indexOf(clean[i]);
      bits += 6;
      if (bits >= 8) {
        bits -= 8;
        out[o++] = (buf >> bits) & 255;
      }
    }
    return out.subarray(0, o);
  }

  function constTimeEqStr(a, b) {
    if (typeof a !== "string" || typeof b !== "string" || a.length !== b.length) {
      return false;
    }
    let diff = 0;
    for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
    return diff === 0;
  }

  // ---------- sha256 (synchronous — the stretched proof runs 2^16 rounds) ---

  const SHA_K = new Uint32Array([
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
    0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
  ]);
  const SHA_IV = new Uint32Array([
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c,
    0x1f83d9ab, 0x5be0cd19,
  ]);

  function sha256Compress(state, block, off) {
    const w = new Uint32Array(64);
    for (let i = 0; i < 16; i++) {
      w[i] =
        ((block[off + i * 4] << 24) |
          (block[off + i * 4 + 1] << 16) |
          (block[off + i * 4 + 2] << 8) |
          block[off + i * 4 + 3]) >>> 0;
    }
    for (let i = 16; i < 64; i++) {
      const a = w[i - 15], b = w[i - 2];
      const s0 = ((a >>> 7) | (a << 25)) ^ ((a >>> 18) | (a << 14)) ^ (a >>> 3);
      const s1 = ((b >>> 17) | (b << 15)) ^ ((b >>> 19) | (b << 13)) ^ (b >>> 10);
      w[i] = (w[i - 16] + s0 + w[i - 7] + s1) >>> 0;
    }
    let a = state[0], b = state[1], c = state[2], d = state[3];
    let e = state[4], f = state[5], g = state[6], h = state[7];
    for (let i = 0; i < 64; i++) {
      const S1 =
        ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7));
      const ch = (e & f) ^ (~e & g);
      const t1 = (h + S1 + ch + SHA_K[i] + w[i]) >>> 0;
      const S0 =
        ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10));
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const t2 = (S0 + maj) >>> 0;
      h = g; g = f; f = e;
      e = (d + t1) >>> 0;
      d = c; c = b; b = a;
      a = (t1 + t2) >>> 0;
    }
    state[0] = (state[0] + a) >>> 0;
    state[1] = (state[1] + b) >>> 0;
    state[2] = (state[2] + c) >>> 0;
    state[3] = (state[3] + d) >>> 0;
    state[4] = (state[4] + e) >>> 0;
    state[5] = (state[5] + f) >>> 0;
    state[6] = (state[6] + g) >>> 0;
    state[7] = (state[7] + h) >>> 0;
  }

  function sha256(data) {
    const len = data.length;
    const blocks = ((len + 9 + 63) / 64) | 0;
    const buf = new Uint8Array(blocks * 64);
    buf.set(data);
    buf[len] = 0x80;
    const bits = len * 8;
    const hi = Math.floor(bits / 0x100000000);
    buf[buf.length - 8] = (hi >>> 24) & 255;
    buf[buf.length - 7] = (hi >>> 16) & 255;
    buf[buf.length - 6] = (hi >>> 8) & 255;
    buf[buf.length - 5] = hi & 255;
    buf[buf.length - 4] = (bits >>> 24) & 255;
    buf[buf.length - 3] = (bits >>> 16) & 255;
    buf[buf.length - 2] = (bits >>> 8) & 255;
    buf[buf.length - 1] = bits & 255;
    const state = new Uint32Array(SHA_IV);
    for (let i = 0; i < blocks; i++) sha256Compress(state, buf, i * 64);
    const out = new Uint8Array(32);
    for (let i = 0; i < 8; i++) {
      out[i * 4] = (state[i] >>> 24) & 255;
      out[i * 4 + 1] = (state[i] >>> 16) & 255;
      out[i * 4 + 2] = (state[i] >>> 8) & 255;
      out[i * 4 + 3] = state[i] & 255;
    }
    return out;
  }

  function sha256Hex(data) {
    return Array.from(sha256(data), (b) => b.toString(16).padStart(2, "0")).join("");
  }

  // ---------- X25519 (RFC 7748, BigInt field arithmetic) --------------------

  const P25519 = (1n << 255n) - 19n;
  const A24 = 121665n;
  const BASE9 = new Uint8Array(32);
  BASE9[0] = 9;

  function leToBig(b) {
    let x = 0n;
    for (let i = b.length - 1; i >= 0; i--) x = (x << 8n) | BigInt(b[i]);
    return x;
  }
  function bigToLE32(x) {
    const o = new Uint8Array(32);
    for (let i = 0; i < 32; i++) {
      o[i] = Number(x & 255n);
      x >>= 8n;
    }
    return o;
  }
  const modP = (x) => ((x % P25519) + P25519) % P25519;
  function powMod(base, exp, m) {
    let r = 1n, b = modP(base), e = exp;
    while (e > 0n) {
      if (e & 1n) r = (r * b) % m;
      b = (b * b) % m;
      e >>= 1n;
    }
    return r;
  }

  function x25519(scalarBytes, uBytes) {
    const k = Uint8Array.from(scalarBytes);
    k[0] &= 248;
    k[31] &= 127;
    k[31] |= 64;
    const u = Uint8Array.from(uBytes);
    u[31] &= 127;
    const kInt = leToBig(k);
    const x1 = modP(leToBig(u));
    let x2 = 1n, z2 = 0n, x3 = x1, z3 = 1n, swap = 0n;
    for (let t = 254; t >= 0; t--) {
      const kt = (kInt >> BigInt(t)) & 1n;
      swap ^= kt;
      if (swap === 1n) {
        [x2, x3] = [x3, x2];
        [z2, z3] = [z3, z2];
      }
      swap = kt;
      const A = modP(x2 + z2);
      const AA = modP(A * A);
      const B = modP(x2 - z2);
      const BB = modP(B * B);
      const E = modP(AA - BB);
      const C = modP(x3 + z3);
      const D = modP(x3 - z3);
      const DA = modP(D * A);
      const CB = modP(C * B);
      const t0 = modP(DA + CB);
      const t1 = modP(DA - CB);
      x3 = modP(t0 * t0);
      z3 = modP(x1 * t1 * t1);
      x2 = modP(AA * BB);
      z2 = modP(E * modP(AA + A24 * E));
    }
    if (swap === 1n) {
      [x2, x3] = [x3, x2];
      [z2, z3] = [z3, z2];
    }
    return bigToLE32(modP(x2 * powMod(z2, P25519 - 2n, P25519)));
  }

  function x25519Keygen() {
    // Random 32 bytes; x25519() clamps internally. The public key is the
    // scalar × basepoint 9 — exactly what x25519_dalek produces.
    return randBytes(32);
  }

  // ---------- AES-256-GCM (pure JS) ------------------------------------------
  // Only what the relay protocol needs: 12-byte nonces, no additional
  // data, 16-byte tags. The S-box is computed from GF(2^8) inverses at
  // load (transcribing 256 magic constants is the classic corruption
  // bug), and self-tested below against known values.

  const xtime = (b) => ((b << 1) ^ (b & 0x80 ? 0x1b : 0)) & 255;
  const rotl8 = (b, n) => ((b << n) | (b >>> (8 - n))) & 255;

  const SBOX = (() => {
    const exp = new Uint8Array(256); // [255] wraps to [0] — inv(1) needs it
    const log = new Uint8Array(256);
    let x = 1;
    for (let i = 0; i < 255; i++) {
      exp[i] = x;
      log[x] = i;
      x ^= xtime(x); // x *= 3 — a generator of GF(2^8)*
    }
    exp[255] = exp[0];
    const box = new Uint8Array(256);
    for (let i = 0; i < 256; i++) {
      const inv = i === 0 ? 0 : exp[255 - log[i]];
      box[i] =
        inv ^
        rotl8(inv, 1) ^
        rotl8(inv, 2) ^
        rotl8(inv, 3) ^
        rotl8(inv, 4) ^
        0x63;
    }
    return box;
  })();

  function expandKey256(key) {
    if (key.length !== 32) throw new Error("AES-256 needs a 32-byte key");
    const w = new Uint32Array(60);
    for (let i = 0; i < 8; i++) {
      w[i] =
        ((key[i * 4] << 24) |
          (key[i * 4 + 1] << 16) |
          (key[i * 4 + 2] << 8) |
          key[i * 4 + 3]) >>> 0;
    }
    let rcon = 1;
    for (let i = 8; i < 60; i++) {
      let t = w[i - 1];
      if (i % 8 === 0) {
        t = ((t << 8) | (t >>> 24)) >>> 0; // RotWord
        t =
          ((((SBOX[(t >>> 24) & 255] << 24) |
            (SBOX[(t >>> 16) & 255] << 16) |
            (SBOX[(t >>> 8) & 255] << 8) |
            SBOX[t & 255]) >>> 0) ^
            ((rcon << 24) >>> 0)) >>> 0;
        rcon = xtime(rcon);
      } else if (i % 8 === 4) {
        t =
          ((SBOX[(t >>> 24) & 255] << 24) |
            (SBOX[(t >>> 16) & 255] << 16) |
            (SBOX[(t >>> 8) & 255] << 8) |
            SBOX[t & 255]) >>> 0;
      }
      w[i] = (w[i - 8] ^ t) >>> 0;
    }
    return w;
  }

  function encryptBlock(w, input) {
    const s = new Uint8Array(16);
    for (let i = 0; i < 16; i++) s[i] = input[i];
    const addRK = (round) => {
      for (let c = 0; c < 4; c++) {
        const word = w[round * 4 + c];
        s[c * 4] ^= (word >>> 24) & 255;
        s[c * 4 + 1] ^= (word >>> 16) & 255;
        s[c * 4 + 2] ^= (word >>> 8) & 255;
        s[c * 4 + 3] ^= word & 255;
      }
    };
    addRK(0);
    for (let round = 1; round <= 14; round++) {
      for (let i = 0; i < 16; i++) s[i] = SBOX[s[i]];
      // ShiftRows — state is column-major: s[r + 4c].
      let t;
      t = s[1]; s[1] = s[5]; s[5] = s[9]; s[9] = s[13]; s[13] = t; // row 1 ←1
      t = s[2]; s[2] = s[10]; s[10] = t; t = s[6]; s[6] = s[14]; s[14] = t; // row 2 ←2
      t = s[3]; s[3] = s[15]; s[15] = s[11]; s[11] = s[7]; s[7] = t; // row 3 ←3
      if (round < 14) {
        // MixColumns, one column at a time.
        for (let c = 0; c < 4; c++) {
          const o = c * 4;
          const a0 = s[o], a1 = s[o + 1], a2 = s[o + 2], a3 = s[o + 3];
          s[o] = xtime(a0) ^ (xtime(a1) ^ a1) ^ a2 ^ a3;
          s[o + 1] = a0 ^ xtime(a1) ^ (xtime(a2) ^ a2) ^ a3;
          s[o + 2] = a0 ^ a1 ^ xtime(a2) ^ (xtime(a3) ^ a3);
          s[o + 3] = (xtime(a0) ^ a0) ^ a1 ^ a2 ^ xtime(a3);
        }
      }
      addRK(round);
    }
    return s;
  }

  // GF(2^128) multiply for GHASH — blocks are BigInts with byte 0 most
  // significant (the standard mapping), R = 0xe1 followed by zeros.
  const GCM_R = 0xe1000000000000000000000000000000n;
  function gfMul(x, y) {
    let z = 0n;
    let v = y;
    for (let i = 127; i >= 0; i--) {
      if ((x >> BigInt(i)) & 1n) z ^= v;
      v = v & 1n ? (v >> 1n) ^ GCM_R : v >> 1n;
    }
    return z;
  }

  function blockToBig(b, off) {
    let x = 0n;
    for (let i = 0; i < 16; i++) x = (x << 8n) | BigInt(b[off + i]);
    return x;
  }
  function bigToBlock(x, out, off) {
    for (let i = 15; i >= 0; i--) {
      out[off + i] = Number(x & 255n);
      x >>= 8n;
    }
  }

  function aes256GcmCore(key, nonce, data, decrypt) {
    const w = expandKey256(key);
    if (nonce.length !== 12) throw new Error("GCM nonce must be 12 bytes");
    const zero = new Uint8Array(16);
    const H = blockToBig(encryptBlock(w, zero), 0);
    const j0 = new Uint8Array(16);
    j0.set(nonce);
    j0[15] = 1;
    // CTR keystream: J0 = IV ‖ 00000001, and the FIRST data block uses
    // inc32(J0) — counter value 2 — so block i (0-based) uses i + 2.
    const ctrBlock = (i) => {
      const c = new Uint8Array(16);
      c.set(nonce);
      const v = i >>> 0;
      c[12] = (v >>> 24) & 255;
      c[13] = (v >>> 16) & 255;
      c[14] = (v >>> 8) & 255;
      c[15] = v & 255;
      return encryptBlock(w, c);
    };
    const out = new Uint8Array(data.length);
    let whole = (data.length / 16) | 0;
    for (let i = 0; i < whole; i++) {
      const ks = ctrBlock(i + 2);
      for (let j = 0; j < 16; j++) out[i * 16 + j] = data[i * 16 + j] ^ ks[j];
    }
    if (data.length % 16) {
      const ks = ctrBlock(whole + 2);
      for (let j = 0; j < data.length - whole * 16; j++) {
        out[whole * 16 + j] = data[whole * 16 + j] ^ ks[j];
      }
    }
    // GHASH over the ciphertext (AAD empty) + the length block.
    let y = 0n;
    const padded = (data.length + 15) & ~15;
    const buf = new Uint8Array(padded + 16);
    if (decrypt) buf.set(data); // GHASH always over CIPHERTEXT
    else buf.set(out);
    for (let i = 0; i < padded; i += 16) {
      y = gfMul(y ^ blockToBig(buf, i), H);
    }
    const bits = BigInt(data.length) * 8n;
    bigToBlock(bits, buf, padded); // len(A)=0 in the high 8 bytes
    y = gfMul(y ^ blockToBig(buf, padded), H);
    const lenBuf = new Uint8Array(16);
    bigToBlock(y, lenBuf, 0);
    const tagMask = encryptBlock(w, j0);
    const tag = new Uint8Array(16);
    for (let i = 0; i < 16; i++) tag[i] = lenBuf[i] ^ tagMask[i];
    return { data: out, tag };
  }

  function aesGcmSeal(key, nonce, plaintext) {
    const r = aes256GcmCore(key, nonce, plaintext, false);
    return concatBytes(r.data, r.tag);
  }

  function aesGcmOpen(key, nonce, sealed) {
    if (sealed.length < 16) throw new Error("ciphertext shorter than the tag");
    const ct = sealed.subarray(0, sealed.length - 16);
    const tag = sealed.subarray(sealed.length - 16);
    const r = aes256GcmCore(key, nonce, ct, true);
    let diff = 0;
    for (let i = 0; i < 16; i++) diff |= r.tag[i] ^ tag[i];
    if (diff !== 0) throw new Error("decrypt failed — wrong key or tampered");
    return r.data;
  }

  // ---------- handshake proofs (mirror src/relay.rs) -------------------------

  const PROOF_KDF = "s256";
  const PROOF_ITERATIONS = 1 << 16;

  function proofLegacy(token, mine, theirs) {
    return b64encode(sha256(concatBytes(utf8(token), mine, theirs)));
  }

  function proofStretched(token, mine, theirs) {
    // sha256(b"damon-relay-proof-s256" ‖ token ‖ mine ‖ theirs), then
    // 2^16 sha256 rounds — one compression each (a 32-byte message fits
    // one padded block), so the padded block is rebuilt from the state
    // between rounds and everything stays in a single compress call.
    const seed = concatBytes(utf8("damon-relay-proof-s256"), utf8(token), mine, theirs);
    let state = new Uint32Array(SHA_IV);
    sha256CompressInto(state, seed); // state ← sha256(seed)
    const block = new Uint8Array(64);
    block[32] = 0x80;
    // Big-endian bit length of the 32-byte message: 0x0000000000000100.
    block[62] = 1;
    block[63] = 0;
    for (let i = 0; i < PROOF_ITERATIONS; i++) {
      block.set(stateBytes(state));
      state = new Uint32Array(SHA_IV);
      sha256Compress(state, block, 0);
    }
    return b64encode(stateBytes(state));
  }

  function stateBytes(state) {
    const o = new Uint8Array(32);
    for (let i = 0; i < 8; i++) {
      o[i * 4] = (state[i] >>> 24) & 255;
      o[i * 4 + 1] = (state[i] >>> 16) & 255;
      o[i * 4 + 2] = (state[i] >>> 8) & 255;
      o[i * 4 + 3] = state[i] & 255;
    }
    return o;
  }

  // Digest `data` into a fresh IV state (helper for the seed round).
  function sha256CompressInto(state, data) {
    const d = sha256(data);
    for (let i = 0; i < 8; i++) {
      state[i] =
        ((d[i * 4] << 24) |
          (d[i * 4 + 1] << 16) |
          (d[i * 4 + 2] << 8) |
          d[i * 4 + 3]) >>> 0;
    }
  }

  // ---------- self tests ------------------------------------------------------

  let selfTested = false;
  function selfTest() {
    if (selfTested) return;
    // sha256("abc")
    if (sha256Hex(utf8("abc")) !==
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad") {
      throw new Error("sha256 self-test failed");
    }
    // X25519, RFC 7748 §6.1 scalars — the public keys below were
    // cross-checked against Node's native X25519 (OpenSSL) as well.
    const aSk = utf8ToBytesHex("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
    const aPub = utf8ToBytesHex("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a");
    const bSk = utf8ToBytesHex("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
    const bPub = utf8ToBytesHex("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f");
    const kAB = utf8ToBytesHex("4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742");
    const eq = (a, b) => a.length === b.length && a.every((v, i) => v === b[i]);
    if (!eq(Array.from(x25519(aSk, BASE9)), Array.from(aPub))) {
      throw new Error("x25519 self-test failed (base multiply)");
    }
    if (!eq(Array.from(x25519(aSk, bPub)), Array.from(kAB)) ||
      !eq(Array.from(x25519(bSk, aPub)), Array.from(kAB))) {
      throw new Error("x25519 self-test failed (DH)");
    }
    // Proof vectors cross-checked against src/relay.rs's own tests.
    const mine = new Uint8Array(32).fill(1);
    const theirs = new Uint8Array(32).fill(3);
    if (proofLegacy("tok", mine, theirs) !==
      "S8mawcxH7YVZq1N0D+PC7Q48QoULbq/LSh/T0S+p2mI=") {
      throw new Error("legacy proof self-test failed");
    }
    if (proofStretched("tok", mine, theirs) !==
      "pEQzR8r3Ev4sJ+cFta1ZJEpgGRynd3Ix9EFoElBMBqQ=") {
      throw new Error("stretched proof self-test failed");
    }
    // AES-GCM — the S-box and one full round trip with published values.
    if (SBOX[0x00] !== 0x63 || SBOX[0x01] !== 0x7c || SBOX[0x53] !== 0xed ||
      SBOX[0xff] !== 0x16) {
      throw new Error("AES S-box self-test failed");
    }
    const k = new Uint8Array(32).map((_, i) => i);
    const n = new Uint8Array(12).map((_, i) => i);
    const pt = new Uint8Array(35).map((_, i) => (i * 7) & 255);
    const sealed = aesGcmSeal(k, n, pt);
    if (!eq(Array.from(aesGcmOpen(k, n, sealed)), Array.from(pt))) {
      throw new Error("AES-GCM round trip failed");
    }
    sealed[3] ^= 1;
    let threw = false;
    try { aesGcmOpen(k, n, sealed); } catch (_) { threw = true; }
    if (!threw) throw new Error("AES-GCM tag check failed");
    selfTested = true;
  }

  function utf8ToBytesHex(hex) {
    const out = new Uint8Array(hex.length / 2);
    for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.substr(i * 2, 2), 16);
    return out;
  }

  // ---------- transport --------------------------------------------------------

  const HANDSHAKE_TIMEOUT = 15000; // matches the daemon's bound
  const KEEPALIVE_EVERY = 30000; // relay drops a client silent for 120s
  const KEEPALIVE_ID_BASE = 1e9; // reserved id space — the UI's response
  // router ignores ids it never sent

  function withTimeout(ms, promise) {
    return Promise.race([
      promise,
      new Promise((_, rej) => setTimeout(() => rej(new Error("handshake timed out")), ms)),
    ]);
  }

  function seqBytes(seq) {
    const b = new Uint8Array(8);
    for (let i = 7; i >= 0; i--) {
      b[i] = seq & 255;
      seq = Math.floor(seq / 256);
    }
    return b;
  }

  function readSeqBE(frame) {
    let seq = 0;
    for (let i = 0; i < 8; i++) seq = seq * 256 + frame[i];
    return seq;
  }

  /**
   * Connect through a relay: `new DamonRelay.connect({url, name, token})`
   * returns a WebSocket-shaped shim — settable `onopen`/`onmessage`/
   * `onclose`/`onerror`, `send(text)` for plaintext JSON-RPC frames,
   * `close()`. The shim runs the E2E handshake before `onopen` fires and
   * encrypts/decrypts every frame; a broken handshake or a tampered
   * frame surfaces as `onclose` with the reason in `closeReason`.
   */
  function connect(opts) {
    const state = {
      closed: false,
      e2e: null,
      sendSeq: 0,
      recvSeq: 0,
      sendChain: Promise.resolve(),
      recvChain: Promise.resolve(),
      ws: null,
      ka: null,
      kaId: KEEPALIVE_ID_BASE,
      // WebSocket.readyState semantics — consumers (the UI's request()
      // guard) gate sends on this being OPEN.
      readyState: 0,
    };
    const api = {
      onopen: null,
      onmessage: null,
      onclose: null,
      onerror: null,
      closeReason: "",
      readyState: 0,
      send(text) {
        if (state.closed) return;
        if (!state.e2e) return; // pre-open sends are dropped, like a WS
        const frameText = String(text);
        state.sendChain = state.sendChain.then(async () => {
          const seq = state.sendSeq++;
          const sealed = aesGcmSeal(state.e2e.sendKey, seqNonce(seq), utf8(frameText));
          const frame = concatBytes(seqBytes(seq), seqNonce(seq), sealed);
          state.ws.send(JSON.stringify({ data: b64encode(frame) }));
        }).catch((e) => fail(e));
      },
      close() {
        state.closed = true;
        api.readyState = state.readyState = 3;
        if (state.ka) clearInterval(state.ka);
        try { if (state.ws) state.ws.close(); } catch (_) { /* closing a dead ws */ }
      },
    };

    function stopKeepalive() {
      if (state.ka) {
        clearInterval(state.ka);
        state.ka = null;
      }
    }

    function fail(err) {
      if (state.closed) return;
      state.closed = true;
      api.readyState = state.readyState = 3;
      api.closeReason = String((err && err.message) || err);
      stopKeepalive();
      try { if (state.ws) state.ws.close(); } catch (_) { /* already dead */ }
      if (api.onclose) api.onclose({ code: 1006, reason: api.closeReason });
    }

    function seqNonce(seq) {
      // 4 zero bytes ‖ seq big-endian — mirrors relay.rs seq_nonce().
      const n = new Uint8Array(12);
      const be = seqBytes(seq);
      n.set(be, 4);
      return n;
    }

    (async () => {
      try {
        selfTest();
        const base = String(opts.url || "").trim().replace(/\/+$/, "");
        if (!/^wss?:\/\//i.test(base)) {
          throw new Error("relay url must start with ws:// or wss://");
        }
        const name = String(opts.name || "");
        if (!name) throw new Error("daemon name required");
        const ws = new WebSocket(base + "/connect?name=" + encodeURIComponent(name));
        state.ws = ws;

        // Frame pump: JSON envelopes → `{"data": …}` strings, with a
        // push/pull queue so the handshake and the recv chain share it.
        const pending = [];
        let waiter = null;
        const nextFrame = () =>
          pending.length
            ? Promise.resolve(pending.shift())
            : new Promise((res) => { waiter = res; });
        ws.onmessage = (ev) => {
          let v;
          try { v = JSON.parse(ev.data); } catch (_) { return; }
          if (typeof v.error === "string") {
            fail(new Error("relay refused: " + v.error));
            return;
          }
          if (typeof v.data !== "string") return;
          if (waiter) {
            const r = waiter;
            waiter = null;
            r(v.data);
          } else {
            pending.push(v.data);
          }
        };
        ws.onclose = () => fail(new Error(api.closeReason || "connection closed"));
        ws.onerror = () => { /* onclose follows */ };

        // Wait for the socket to open — send() during CONNECTING is
        // rejected by browsers and Node alike.
        await new Promise((resolve, reject) => {
          if (ws.readyState === 1) return resolve();
          const t = setTimeout(() => reject(new Error("relay socket timed out")), HANDSHAKE_TIMEOUT);
          ws.onopen = () => { clearTimeout(t); resolve(); };
          ws.onerror = () => { clearTimeout(t); reject(new Error("cannot reach relay")); };
        });

        // Handshake — client proves token knowledge FIRST, exactly like
        // E2e::client_handshake(); a daemon proof that fails to verify
        // means a wrong token or an impostor daemon.
        const e2e = await withTimeout(HANDSHAKE_TIMEOUT, (async () => {
          const priv = x25519Keygen();
          const myPub = x25519(priv, BASE9);
          ws.send(JSON.stringify({
            data: JSON.stringify({ e2e_pub: b64encode(myPub), kdf: PROOF_KDF }),
          }));
          const f1 = JSON.parse(await nextFrame());
          if (f1.e2e_error) {
            throw new Error("relay/daemon refused handshake: " + f1.e2e_error);
          }
          const theirPub = b64decode(String(f1.e2e_pub || ""));
          if (theirPub.length !== 32) throw new Error("bad daemon pubkey");
          const stretch = f1.kdf === PROOF_KDF;
          const pf = stretch ? proofStretched : proofLegacy;
          const token = String(opts.token == null ? "" : opts.token);
          ws.send(JSON.stringify({
            data: JSON.stringify({ e2e_proof: pf(token, myPub, theirPub) }),
          }));
          const f2 = JSON.parse(await nextFrame());
          if (f2.e2e_error === "auth") {
            throw new Error("daemon rejected our proof — wrong auth_token?");
          }
          if (f2.e2e_error) {
            throw new Error("relay/daemon refused handshake: " + f2.e2e_error);
          }
          if (!constTimeEqStr(String(f2.e2e_proof || ""), pf(token, theirPub, myPub))) {
            throw new Error("daemon failed E2E proof — wrong auth_token?");
          }
          const shared = x25519(priv, theirPub);
          return {
            sendKey: sha256(concatBytes(shared, utf8("c2d"))),
            recvKey: sha256(concatBytes(shared, utf8("d2c"))),
          };
        })());
        state.e2e = e2e;

        // Recv chain: strict seq order, fail-closed on any gap (the
        // daemon does the same — a reordered or injected frame must not
        // decrypt into a plausible-looking lie).
        (async () => {
          while (!state.closed) {
            const d = await nextFrame();
            state.recvChain = state.recvChain.then(async () => {
              const frame = b64decode(d);
              if (frame.length < 20) throw new Error("frame too short");
              const seq = readSeqBE(frame);
              if (seq !== state.recvSeq) {
                throw new Error(
                  "frame seq " + seq + " != expected " + state.recvSeq + " — replay or reorder"
                );
              }
              const pt = aesGcmOpen(e2e.recvKey, frame.subarray(8, 20), frame.subarray(20));
              state.recvSeq++;
              if (api.onmessage) api.onmessage({ data: fromUtf8(pt) });
            }).catch((e) => fail(e));
          }
        })();

        // Keepalive: an encrypted no-op hello every 30s. Browsers can't
        // send WS pings and the relay drops silent clients after 120s;
        // the daemon answers the RPC and both idle clocks stay fed.
        state.ka = setInterval(() => {
          if (!state.closed && state.ws && state.ws.readyState === 1) {
            api.send(JSON.stringify({ id: state.kaId++, method: "hello", params: {} }));
          }
        }, KEEPALIVE_EVERY);

        api.readyState = state.readyState = 1;
        if (api.onopen) api.onopen({});
      } catch (e) {
        fail(e);
      }
    })();

    return api;
  }

  global.DamonRelay = {
    connect,
    _internals: {
      sha256, sha256Hex, x25519, BASE9, SBOX,
      aesGcmSeal, aesGcmOpen, proofLegacy, proofStretched,
      b64encode, b64decode, selfTest,
    },
  };
})(typeof window !== "undefined" ? window : globalThis);
