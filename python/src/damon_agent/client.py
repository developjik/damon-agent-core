"""Asyncio client for the damond WS protocol v2.

Mirrors the Node (``damon-agent`` npm package) and Rust
(``damon::client``) clients so all three SDKs feel identical:
``connect`` → ``hello`` → ``new_session``, ``prompt`` with streamed
``session.event`` pushes, permission asks answered through
``respond_permission()``. Relay links (``connect_relay``) are ported
from npm/client.mjs — E2E-encrypted via the handshake in
``src/relay.rs`` (the normative protocol).

A background supervisor task owns the WebSocket link. When the link
drops it fails pending calls, emits a ``disconnected`` event, and
redials with exponential backoff (100ms doubling to a 5s cap) until
the daemon returns — then emits ``reconnected``. Calls made while the
link is down wait up to 10s for it to come back.

Requires Python >= 3.11 and ``websockets`` >= 12. The relay transport
is pure stdlib on top (hashlib for proofs, X25519 and AES-256-GCM in
plain Python) — no ``cryptography`` dependency — and every primitive
is pinned by published vectors plus vectors generated from the npm
reference in tests/test_client.py.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import http.client
import json
import secrets
import urllib.parse
from typing import Any, AsyncIterator, Optional, Union

import websockets

__all__ = ["DamonClient", "RpcError"]

#: Inbound frame cap — the same wire budget the daemon and relay enforce
#: (a library default of ~64 MiB would let a hostile peer force huge
#: allocations).
_MAX_FRAME = 4 * 1024 * 1024
#: Event-queue capacity. A slow consumer makes room by dropping the
#: oldest non-permission event; every drop is counted on
#: ``dropped_events``.
_EVENT_CAPACITY = 64
#: First reconnect delay; doubles per failure up to ``_RECONNECT_MAX``.
_RECONNECT_MIN = 0.1
_RECONNECT_MAX = 5.0
#: How long a call parks on a reconnecting link before giving up.
_RECONNECT_WAIT = 10.0
#: Bound on the relay E2E handshake — a relay that accepts the socket
#: but never answers must not hang connect_relay() forever (relay.rs
#: HANDSHAKE_TIMEOUT).
_HANDSHAKE_TIMEOUT = 15.0
#: Relay plaintext frame budget — mirrors rpc.rs MAX_RESPONSE_BYTES:
#: base64 inflates ~4/3 and the relay wraps the ciphertext in a JSON
#: envelope, so a larger plaintext would produce a frame over the
#: 4 MiB socket cap that the peer then drops silently.
_MAX_RELAY_PLAINTEXT = 1 << 20
#: Stretched-proof KDF marker — mirrors relay.rs PROOF_KDF. Offered in
#: the first handshake frame; a daemon that echoes it runs the same
#: stretched proof, older peers keep the single-hash proof.
_PROOF_KDF = "s256"
#: Extra sha256 rounds for the stretched proof (relay.rs
#: PROOF_ITERATIONS = 2^16): cheap per handshake, expensive per
#: offline guess at a low-entropy token.
_PROOF_ITERATIONS = 1 << 16

# Sentinel pushed to the event queue to end events() iterators.
_CLOSE = object()


class RpcError(Exception):
    """A JSON-RPC error response from the daemon.

    Attributes:
        code: the JSON-RPC error code (e.g. ``-32601`` method not found).
        message: the daemon's error message.
    """

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class _Pending:
    """One in-flight client request.

    ``prompt_session`` marks prompt turns: their outcome is also
    delivered on the event stream as a ``prompt_done`` event — the only
    surface some consumers read.
    """

    __slots__ = ("future", "prompt_session")

    def __init__(self, future: asyncio.Future, prompt_session: Optional[str] = None) -> None:
        self.future = future
        self.prompt_session = prompt_session


def _reap(future: asyncio.Future) -> None:
    """Mark an exception retrieved so a future nobody awaits (e.g. a
    prompt() caller cancelled mid-turn) cannot warn at GC time. Awaiters
    still receive the exception."""
    if not future.cancelled():
        future.exception()


async def _quiet_close(conn: Any) -> None:
    if conn is None:
        return
    try:
        await conn.close()
    except Exception:
        pass


async def _ticketed_url(ws_url: str, token: str) -> str:
    """Exchange the bearer token for a single-use WS ticket and return
    the ``?ticket=`` URL — the token never appears in a URL.
    ``ws(s)://host/ws`` → ``http(s)://host/v1/ws_ticket``, preserving any
    path prefix before ``/ws``."""
    parts = urllib.parse.urlsplit(ws_url)
    path = parts.path.rstrip("/")
    if parts.scheme not in ("ws", "wss") or not path.endswith("/ws"):
        raise ValueError(f"expected a ws(s) URL ending in /ws, got: {ws_url!r}")
    if not parts.hostname or "@" in parts.netloc:
        # No credentials-in-URL: the token travels in the Authorization
        # header only, so an "user:pass@host" form is always a mistake.
        raise ValueError(f"expected a plain host[:port] ws(s) URL, got: {ws_url!r}")
    scheme = "https" if parts.scheme == "wss" else "http"
    conn_cls = http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
    ticket_path = path[:-3] + "/v1/ws_ticket"

    def fetch() -> bytes:
        # The endpoint is the caller's own daemon (the SDK's purpose) and
        # is constrained above: only ws(s) input survives validation, so
        # only http(s) against that same host is ever dialed. http.client
        # with the explicit validated host/port (no URL-string handling
        # at the socket boundary) keeps the guarantee mechanical.
        conn = conn_cls(parts.hostname, port=parts.port, timeout=15)
        try:
            conn.request(
                "POST", ticket_path, headers={"Authorization": f"Bearer {token}"}
            )
            resp = conn.getresponse()
            body = resp.read()
            code = resp.status
        finally:
            conn.close()
        if code != 200:
            raise ConnectionError(f"ws_ticket rejected: {code}")
        return body

    try:
        body = await asyncio.to_thread(fetch)
    except ConnectionError:
        raise
    data = json.loads(body)
    ticket = data.get("ticket") if isinstance(data, dict) else None
    if not ticket:
        raise ConnectionError("no ticket in ws_ticket response")
    sep = "&" if parts.query else "?"
    return f"{ws_url}{sep}ticket={ticket}"

# ---------------------------------------------------------------------------
# Relay E2E crypto — mirrors src/relay.rs (the normative protocol); the
# npm client (npm/client.mjs) is the reference port this follows.
# Pure stdlib: Python has no WebCrypto, so the primitives live here and
# are pinned by cross-implementation vectors in tests/test_client.py
# (RFC 7748 / NIST published vectors + vectors generated from the npm
# client itself).
# ---------------------------------------------------------------------------


def _relay_encode(name: str) -> str:
    """Percent-encode a query value exactly like relay.rs ``urlencoding``:
    RFC 3986 unreserved chars pass through, every other byte is %XX."""
    # urllib.quote never quotes letters, digits, and ``_.-~`` — exactly
    # the unreserved set — and uppercases the escapes for the rest.
    return urllib.parse.quote(name, safe="")


def _b64(data: bytes) -> str:
    """STANDARD base64 (padded) — the engine relay.rs uses."""
    return base64.b64encode(data).decode()


def _sha256(*chunks: bytes) -> bytes:
    return hashlib.sha256(b"".join(chunks)).digest()


def _proof(token: str, mine: bytes, theirs: bytes, stretch: bool = False) -> str:
    """sha256(token ‖ mine ‖ theirs), base64 — binds the proof to BOTH
    public keys, so a relay cannot replay a captured proof for other
    keys. With ``stretch``, iterate sha256 ``_PROOF_ITERATIONS`` more
    times over a domain-separated seed (exactly relay.rs
    ``proof_stretched``)."""
    h = _sha256(token.encode(), mine, theirs)
    if stretch:
        h = _sha256(b"damon-relay-proof-s256", token.encode(), mine, theirs)
        for _ in range(_PROOF_ITERATIONS):
            h = hashlib.sha256(h).digest()
    return _b64(h)


def _derive_key(shared: bytes, label: bytes) -> bytes:
    """sha256(shared ‖ label) — directional session key material
    (``"d2c"`` = daemon→client, ``"c2d"`` = client→daemon)."""
    return _sha256(shared, label)


def _seq_nonce(seq: int) -> bytes:
    """12-byte AEAD nonce: 4 zero bytes ‖ seq (big-endian)."""
    return b"\x00\x00\x00\x00" + seq.to_bytes(8, "big")


# --- X25519 (RFC 7748) ------------------------------------------------------

_X25519_P = 2**255 - 19
_X25519_A24 = 121665


def _x25519(scalar: bytes, point: bytes) -> bytes:
    """RFC 7748 X25519 over Curve25519 — plain integers, no deps.

    Not constant-time (CPython big ints cannot be), which is the same
    trade the npm client makes on runtimes without Secure Curves: the
    scalar is a per-handshake ephemeral, and the auth token travels in
    the proof, not in this multiplication. Pinned against the RFC 7748
    vectors in tests/test_client.py.
    """
    if len(scalar) != 32 or len(point) != 32:
        raise ValueError("x25519 expects 32-byte scalar and point")
    # decodeScalar25519 clamps in place, like x25519_dalek.
    k = int.from_bytes(scalar, "little")
    k &= ~7
    k &= (1 << 255) - 1
    k |= 1 << 254
    u = int.from_bytes(point, "little") & ((1 << 255) - 1)
    p = _X25519_P
    x1, x2, z2, x3, z3, swap = u, 1, 0, u, 1, 0
    for t in reversed(range(255)):
        swap ^= (k >> t) & 1
        if swap:
            x2, x3 = x3, x2
            z2, z3 = z3, z2
        swap = (k >> t) & 1
        a = (x2 + z2) % p
        aa = a * a % p
        b = (x2 - z2) % p
        bb = b * b % p
        e = (aa - bb) % p
        c = (x3 + z3) % p
        d = (x3 - z3) % p
        da = d * a % p
        cb = c * b % p
        x3 = pow(da + cb, 2, p)
        z3 = x1 * pow(da - cb, 2, p) % p
        x2 = aa * bb % p
        z2 = e * (aa + _X25519_A24 * e) % p
    if swap:
        x2, x3 = x3, x2
        z2, z3 = z3, z2
    return (x2 * pow(z2, p - 2, p) % p).to_bytes(32, "little")


def _x25519_keypair() -> tuple[bytes, bytes]:
    """Fresh ephemeral (private, public) — reconnect attempts never
    reuse keys."""
    priv = secrets.token_bytes(32)
    return priv, _x25519(priv, (9).to_bytes(32, "little"))


def _x25519_shared(priv: bytes, peer_pub: bytes) -> bytes:
    shared = _x25519(priv, peer_pub)
    if shared == bytes(32):
        # All-zero output = a low-order (non-contributory) peer key —
        # predictable session keys; refuse like x25519_dalek's
        # was_contributory check in relay.rs.
        raise ValueError("non-contributory DH share")
    return shared


# --- AES-256-GCM ------------------------------------------------------------
#
# Layout mirrors the aes-gcm crate in relay.rs exactly: 96-bit IV from
# _seq_nonce, 16-byte tag appended. Slow by native standards (~1 MB/s)
# but relay frames are JSON-RPC sized and _MAX_RELAY_PLAINTEXT bounds
# the worst case. The S-box is computed from its GF(2^8)-inverse +
# affine definition rather than transcribed — a single mistyped
# constant would silently break every frame.


def _build_aes_sbox() -> list[int]:
    """Rijndael S-box from its definition: GF(2^8) multiplicative
    inverse (via log/exp tables over generator 3) plus the affine
    transform."""
    exp = [0] * 255
    log = [0] * 256
    x = 1
    for i in range(255):
        exp[i] = x
        log[x] = i
        x ^= ((x << 1) ^ 0x11B) & 0xFF if x & 0x80 else x << 1  # x *= 3
    sbox = [0x63] * 256  # 0 maps to the affine of 0, 0x63
    for i in range(1, 256):
        inv = exp[(255 - log[i]) % 255]
        r = (
            inv
            ^ ((inv << 1) | (inv >> 7))
            ^ ((inv << 2) | (inv >> 6))
            ^ ((inv << 3) | (inv >> 5))
            ^ ((inv << 4) | (inv >> 4))
        )
        sbox[i] = (r & 0xFF) ^ 0x63
    return sbox


_SBOX = _build_aes_sbox()


def _aes256_key_schedule(key: bytes) -> list[int]:
    """Expand a 32-byte key into the 60 big-endian round-key words of
    AES-256 (FIPS-197: Nk=8, Nr=14)."""
    if len(key) != 32:
        raise ValueError("AES-256 expects a 32-byte key")
    w = [int.from_bytes(key[4 * i : 4 * i + 4], "big") for i in range(8)]
    rcon = 1
    for i in range(8, 60):
        t = w[i - 1]
        if i % 8 == 0:
            t = ((t << 8) | (t >> 24)) & 0xFFFFFFFF  # RotWord
            t = (
                ((_SBOX[(t >> 24) & 0xFF] << 24)
                | (_SBOX[(t >> 16) & 0xFF] << 16)
                | (_SBOX[(t >> 8) & 0xFF] << 8)
                | _SBOX[t & 0xFF])
                ^ (rcon << 24)
            )
            rcon = ((rcon << 1) ^ 0x11B) & 0xFF if rcon & 0x80 else rcon << 1
        elif i % 8 == 4:
            t = (
                (_SBOX[(t >> 24) & 0xFF] << 24)
                | (_SBOX[(t >> 16) & 0xFF] << 16)
                | (_SBOX[(t >> 8) & 0xFF] << 8)
                | _SBOX[t & 0xFF]
            )
        w.append(w[i - 8] ^ t)
    return w


def _aes_block(w: list[int], block: bytes) -> bytes:
    """Encrypt one 16-byte block. The byte string IS the column-major
    state (in[i] → s[4*(i//4) + i%4] = in[i]), so no reindexing."""
    s = list(block)
    for c in range(4):
        k = w[c]
        for r in range(4):
            s[4 * c + r] ^= (k >> (24 - 8 * r)) & 0xFF
    for rnd in range(1, 15):
        t = [_SBOX[b] for b in s]  # SubBytes
        o = [0] * 16
        for c in range(4):  # ShiftRows: row r ← rotated left by r
            for r in range(4):
                o[4 * c + r] = t[4 * ((c + r) % 4) + r]
        if rnd < 14:  # MixColumns (skipped on the final round)
            m = [0] * 16
            for c in range(4):
                b = o[4 * c : 4 * c + 4]
                for r in range(4):
                    # Row r: 2·b_r ^ 3·b_{r+1} ^ b_{r+2} ^ b_{r+3}, with
                    # GF doubling via the AES polynomial 0x11B.
                    t0, t1 = b[r], b[(r + 1) % 4]
                    x0 = ((t0 << 1) ^ 0x11B) & 0xFF if t0 & 0x80 else t0 << 1
                    x1 = ((t1 << 1) ^ 0x11B) & 0xFF if t1 & 0x80 else t1 << 1
                    m[4 * c + r] = x0 ^ x1 ^ t1 ^ b[(r + 2) % 4] ^ b[(r + 3) % 4]
            o = m
        for c in range(4):
            k = w[4 * rnd + c]
            for r in range(4):
                o[4 * c + r] ^= (k >> (24 - 8 * r)) & 0xFF
        s = o
    return bytes(s)


def _gf_mult(x: int, y: int) -> int:
    """GCM's GF(2^128) multiply (SP 800-38D): MSB-first over ``x``,
    right-shift reduction by R = 0xE1 ‖ 0^120."""
    z, v = 0, y
    for i in range(127, -1, -1):
        if (x >> i) & 1:
            z ^= v
        v = (v >> 1) ^ (0xE1 << 120) if v & 1 else v >> 1
    return z


def _ghash(h: int, data: bytes) -> int:
    """GHASH over zero-padded 16-byte blocks, closed by the bit-length
    block (no AAD on this wire)."""
    y = 0
    for i in range(0, len(data), 16):
        block = data[i : i + 16].ljust(16, b"\x00")
        y = _gf_mult(y ^ int.from_bytes(block, "big"), h)
    length = (8 * len(data)).to_bytes(16, "big")
    return _gf_mult(y ^ int.from_bytes(length, "big"), h)


def _aes_ctr(w: list[int], iv: bytes, src: bytes) -> bytes:
    """CTR mode with the GCM counter chain: block i uses J0 + i + 1
    (J0 itself is reserved for the tag mask)."""
    j0 = int.from_bytes(iv + b"\x00\x00\x00\x01", "big")
    out = bytearray()
    for i in range(0, len(src), 16):
        counter = (j0 + i // 16 + 1) % (1 << 128)
        ks = _aes_block(w, counter.to_bytes(16, "big"))
        out.extend(b ^ k for b, k in zip(src[i : i + 16], ks))
    return bytes(out)


def _gcm_seal(key: bytes, iv: bytes, plaintext: bytes) -> tuple[bytes, bytes]:
    """AES-256-GCM encrypt → (ciphertext, tag)."""
    w = _aes256_key_schedule(key)
    h = int.from_bytes(_aes_block(w, bytes(16)), "big")
    ct = _aes_ctr(w, iv, plaintext)
    tag_input = _ghash(h, ct)
    mask = _aes_block(w, (int.from_bytes(iv + b"\x00\x00\x00\x01", "big")).to_bytes(16, "big"))
    tag = bytes(a ^ b for a, b in zip(tag_input.to_bytes(16, "big"), mask))
    return ct, tag


def _gcm_open(key: bytes, iv: bytes, ct: bytes, tag: bytes) -> bytes:
    """AES-256-GCM decrypt — the tag is checked constant-time before
    any plaintext is released."""
    w = _aes256_key_schedule(key)
    h = int.from_bytes(_aes_block(w, bytes(16)), "big")
    tag_input = _ghash(h, ct)
    mask = _aes_block(w, (int.from_bytes(iv + b"\x00\x00\x00\x01", "big")).to_bytes(16, "big"))
    expect = bytes(a ^ b for a, b in zip(tag_input.to_bytes(16, "big"), mask))
    if not hmac.compare_digest(expect, tag):
        raise ValueError("decrypt failed — wrong key or tampered")
    return _aes_ctr(w, iv, ct)


def _seal_frame(key: bytes, seq: int, plaintext: str) -> str:
    """Encrypt a JSON text frame → base64 of seq(8B big-endian) ‖
    nonce(12B) ‖ ct+tag — the exact layout of relay.rs E2e::encrypt."""
    data = plaintext.encode()
    if len(data) > _MAX_RELAY_PLAINTEXT:
        raise ValueError(
            f"plaintext {len(data)} bytes exceeds the "
            f"{_MAX_RELAY_PLAINTEXT}-byte relay frame budget"
        )
    nonce = _seq_nonce(seq)
    ct, tag = _gcm_seal(key, nonce, data)
    return _b64(seq.to_bytes(8, "big") + nonce + ct + tag)


def _open_frame(key: bytes, expected: int, frame_b64: str) -> tuple[int, str]:
    """Decrypt a base64 frame → ``(seq, text)``. The frame's sequence
    number must equal ``expected`` — a replayed, reordered, or injected
    frame raises, and so does a tampered ciphertext or bad base64
    (callers fail closed)."""
    try:
        frame = base64.b64decode(frame_b64, validate=True)
    except ValueError:
        raise ValueError("bad base64 frame") from None
    if len(frame) < 20:
        raise ValueError("frame too short")
    seq = int.from_bytes(frame[:8], "big")
    if seq != expected:
        raise ValueError(f"frame seq {seq} != expected {expected} — replay or reorder")
    pt = _gcm_open(key, frame[8:20], frame[20:-16], frame[-16:])
    return seq, pt.decode()


# --- The relay link ---------------------------------------------------------


class _RelayLink:
    """One E2E-encrypted relay link, shaped like a websockets connection
    so the supervisor can own it exactly like a direct link:
    ``recv()`` yields decrypted plaintext frames, ``send()`` seals them.

    Mirrors npm/client.mjs ``relayIo`` + src/relay.rs ``client_connect``:
    the 4-message X25519 handshake (client proves first), then strict-
    sequence AES-256-GCM frames wrapped in the relay's ``{"data": …}``
    envelopes. Keepalive and half-open detection ride on websockets'
    own ping_interval/ping_timeout (20s) rather than a hand-rolled 30s
    ping timer — the relay counts any frame as liveness, so the
    observable contract (NAT warmth + dead-link teardown) holds.
    """

    _DOWN = object()  # sentinel: inbox drained, link is dead

    def __init__(self, ws: Any) -> None:
        self._ws = ws
        self._inbox: asyncio.Queue = asyncio.Queue()
        self._down = False
        self._send_key = b""
        self._recv_key = b""
        self._send_seq = 0
        self._recv_seq = 0
        self._reader: Optional[asyncio.Task[None]] = None

    @classmethod
    async def start(cls, ws: Any, token: str, timeout: float) -> "_RelayLink":
        """Run the client side of the 4-message E2E handshake
        (relay.rs ``client_handshake``) and start the decrypt pump.
        Raises ConnectionError on refusal, bad proof, or timeout —
        the caller closes the socket so a failed attempt cannot pin
        the relay's per-IP session slot."""
        self = cls(ws)
        # 1. Our ephemeral public key, advertising the stretched-proof
        #    KDF — a daemon that echoes it computes the stretched proof
        #    too; anything older ignores the unknown field and we fall
        #    back to the legacy single-hash proof.
        priv, pub = _x25519_keypair()
        await self._send_raw(json.dumps({"e2e_pub": _b64(pub), "kdf": _PROOF_KDF}))
        # 2. Daemon's bare {e2e_pub} — no proof until we authenticate.
        try:
            hs = json.loads(await self._handshake_frame(timeout))
        except ValueError:
            raise ConnectionError("malformed daemon handshake") from None
        if not isinstance(hs, dict) or not isinstance(hs.get("e2e_pub"), str):
            raise ConnectionError("missing e2e_pub in daemon handshake")
        try:
            theirs = base64.b64decode(hs["e2e_pub"], validate=True)
        except ValueError:
            raise ConnectionError("bad e2e_pub in daemon handshake") from None
        if len(theirs) != 32:
            raise ConnectionError("bad pubkey len")
        stretch = hs.get("kdf") == _PROOF_KDF
        # 3. Our proof over (client_pub, daemon_pub) — the client proves
        #    FIRST, so the daemon never reveals token-derived material
        #    to a peer that hasn't authenticated (and a relay can't farm
        #    proofs per pubkey).
        await self._send_raw(json.dumps({"e2e_proof": _proof(token, pub, theirs, stretch)}))
        # 4. Daemon's proof, only sent to an authenticated client — or
        #    an explicit auth rejection so a wrong token fails fast
        #    instead of timing out.
        try:
            reply = json.loads(await self._handshake_frame(timeout))
        except ValueError:
            raise ConnectionError("malformed daemon proof") from None
        if isinstance(reply, dict) and reply.get("e2e_error") == "auth":
            raise ConnectionError("daemon rejected our proof — wrong auth_token?")
        if isinstance(reply, dict) and isinstance(reply.get("e2e_error"), str):
            raise ConnectionError(
                f"relay/daemon refused handshake: {reply['e2e_error']}"
            )
        got = reply.get("e2e_proof") if isinstance(reply, dict) else None
        expect = _proof(token, theirs, pub, stretch)
        # Constant-time comparison — the proof check must not leak by
        # early exit (mirrors config::constant_time_eq / npm
        # constantTimeEq).
        if not isinstance(got, str) or not hmac.compare_digest(
            got.encode(), expect.encode()
        ):
            raise ConnectionError("daemon failed E2E proof — wrong auth_token?")
        try:
            shared = _x25519_shared(priv, theirs)
        except ValueError:
            raise ConnectionError("non-contributory DH share") from None
        # Client sends with c2d, receives with d2c; the daemon mirrors.
        self._send_key = _derive_key(shared, b"c2d")
        self._recv_key = _derive_key(shared, b"d2c")
        self._reader = asyncio.create_task(self._read_pump(), name="damon-relay-read")
        return self

    async def _handshake_frame(self, timeout: float) -> str:
        """Next ``{"data": …}`` payload within ``timeout`` — non-data
        frames (client-id notices, relay chatter) are ignored, and a
        relay-level ``{"error": …}`` refusal fails fast instead of
        stalling (mirrors the Rust read pump)."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            left = deadline - loop.time()
            if left <= 0:
                raise ConnectionError("E2E handshake timed out")
            try:
                raw = await asyncio.wait_for(self._ws.recv(), left)
            except TimeoutError:
                raise ConnectionError("E2E handshake timed out") from None
            try:
                v = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(v, dict):
                continue
            if isinstance(v.get("error"), str):
                raise ConnectionError(f"relay/daemon refused handshake: {v['error']}")
            if isinstance(v.get("data"), str):
                return v["data"]

    async def _read_pump(self) -> None:
        """Decrypt inbound frames in strict arrival order; anything
        replayed, reordered, or tampered fails closed (relay.rs E2e) —
        the supervisor redials and re-handshakes with fresh keys."""
        try:
            async for raw in self._ws:
                try:
                    v = json.loads(raw)
                except ValueError:
                    continue
                if not isinstance(v, dict):
                    continue
                if isinstance(v.get("error"), str):
                    break  # relay-level error on a live link is fatal
                d = v.get("data")
                if not isinstance(d, str):
                    continue
                try:
                    _, text = _open_frame(self._recv_key, self._recv_seq, d)
                except ValueError:
                    break  # fail closed
                self._recv_seq += 1
                await self._inbox.put(text)
        except asyncio.CancelledError:
            raise  # the finally below marks the link down
        except Exception:
            pass  # recv errors = dead link
        finally:
            self._down = True
            self._inbox.put_nowait(_RelayLink._DOWN)

    async def _send_raw(self, inner: str) -> None:
        # Every outbound message travels wrapped in the envelope the
        # relay pipes.
        await self._ws.send(json.dumps({"data": inner}))

    async def recv(self) -> str:
        item = await self._inbox.get()
        if item is _RelayLink._DOWN:
            raise ConnectionError("relay link closed")
        return item

    async def send(self, text: str) -> None:
        if self._down:
            raise ConnectionError("relay link closed")
        # DamonClient._send holds its send lock across this call, so
        # seq order always matches wire order — a swap would fail the
        # daemon's strict recv check and kill the session.
        seq = self._send_seq
        self._send_seq += 1
        await self._ws.send(json.dumps({"data": _seal_frame(self._send_key, seq, text)}))

    def close_transport(self) -> None:
        """Sync teardown — DamonClient.close() looks for exactly this
        name (websockets >= 13 convention)."""
        self._down = True
        reader, self._reader = self._reader, None
        if reader is not None:
            reader.cancel()
        closer = getattr(self._ws, "close_transport", None)
        if closer is not None:
            try:
                closer()
                return
            except Exception:
                pass
        try:
            asyncio.get_running_loop().create_task(_quiet_close(self._ws))
        except RuntimeError:
            pass

    async def close(self) -> None:
        self.close_transport()


#: Pure internals for tests — NOT a public or stable API (npm parity:
#: the mjs client's ``__test`` export).
_TEST: dict[str, Any] = {
    "relay_encode": _relay_encode,
    "seq_nonce": _seq_nonce,
    "proof": _proof,
    "derive_keys": _derive_key,
    "x25519": _x25519,
    "x25519_keypair": _x25519_keypair,
    "x25519_shared": _x25519_shared,
    "seal_frame": _seal_frame,
    "open_frame": _open_frame,
}


class DamonClient:
    """A connected damon client.

    Build with :meth:`connect` (direct WS) or :meth:`connect_relay`
    (E2E-encrypted relay link); never call the constructor directly.
    Every request method is a coroutine; streamed ``session.event``
    pushes arrive on :meth:`events`. Cheap to share within one event
    loop.
    """

    def __init__(self, conn: Any, dial: Any, *, auto_resume: bool = True) -> None:
        self._dial = dial  # async () -> fresh connection (ticket included)
        self._conn: Any = conn
        self._task: Optional[asyncio.Task[None]] = None
        self._next_id = 0
        self._pending: dict[int, _Pending] = {}
        # Sessions this client touched (created/resumed/prompted) — the
        # auto-resume set; None disables resubscription entirely.
        self._touched: Optional[dict[str, None]] = {} if auto_resume else None
        self._events: asyncio.Queue[Any] = asyncio.Queue(maxsize=_EVENT_CAPACITY)
        self._dropped = 0
        self._closed = False
        self._link_up = asyncio.Event()
        self._link_up.set()
        self._send_lock = asyncio.Lock()
        self.server_hello: Optional[dict[str, Any]] = None
        self._push({"type": "connected"})

    # --- Construction --------------------------------------------------

    @classmethod
    async def connect(
        cls, url: str, token: Optional[str] = None, *, auto_resume: bool = True
    ) -> DamonClient:
        """Connect to a damond WS endpoint (``ws://127.0.0.1:9470/ws``).

        When ``token`` is set, a single-use ticket is fetched from
        ``POST /v1/ws_ticket`` (Bearer auth) and sent as ``?ticket=``.
        If the link drops the client redials with backoff (100ms → 5s,
        fresh ticket each attempt); calls made while down wait up to
        10s for the link. Raises if the first dial fails.
        """

        async def dial() -> Any:
            uri = await _ticketed_url(url, token) if token else url
            return await websockets.connect(uri, max_size=_MAX_FRAME)

        self = cls(await dial(), dial, auto_resume=auto_resume)
        self._task = asyncio.create_task(self._supervise(self._conn), name="damon-supervise")
        return self

    @classmethod
    async def connect_relay(
        cls,
        url: str,
        name: str,
        token: str,
        *,
        handshake_timeout: float = _HANDSHAKE_TIMEOUT,
        auto_resume: bool = True,
    ) -> DamonClient:
        """Connect through a damon-relay — E2E-encrypted, identical
        surface from here on (mirrors Rust ``connect_relay`` / npm
        ``connectRelay``).

        The relay pipes WebSocket frames between the registered daemon
        and this client but never sees plaintext: after a 4-message
        X25519 handshake (client proves the auth token first) every
        JSON-RPC frame is AES-256-GCM encrypted with direction-
        separated keys and strict sequence numbers. Both sides
        negotiate the stretched proof (``"kdf": "s256"``, 2^16 extra
        sha256 rounds) when the daemon understands it, and fall back to
        the legacy single-hash proof otherwise. A dropped link redials
        with a full fresh handshake; calls made while down wait up to
        10s for the link. Raises if the first dial or handshake fails.
        """
        if not isinstance(url, str) or not url:
            raise ValueError("connect_relay: url (the relay's ws://host:port) is required")
        if not isinstance(name, str) or not name:
            raise ValueError("connect_relay: name (the daemon's relay name) is required")
        if not isinstance(token, str) or not token:
            raise ValueError("connect_relay: token (the daemon's auth_token) is required")
        # Same URL shape as relay.rs client_connect: trim trailing '/',
        # then /connect?name=<percent-encoded>. The token never appears
        # in the URL — it only ever travels inside the E2E proof.
        connect_url = f"{url.rstrip('/')}/connect?name={_relay_encode(name)}"

        async def dial() -> Any:
            ws = await websockets.connect(connect_url, max_size=_MAX_FRAME)
            try:
                return await _RelayLink.start(ws, token, handshake_timeout)
            except BaseException:
                # Kill the socket — a live read half would pin the
                # relay's per-IP session slot after every failed
                # attempt (mirrors Rust aborting the read pump on
                # handshake failure).
                try:
                    await ws.close()
                except Exception:
                    pass
                raise

        self = cls(await dial(), dial, auto_resume=auto_resume)
        self._task = asyncio.create_task(self._supervise(self._conn), name="damon-supervise")
        return self

    async def __aenter__(self) -> DamonClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.close()

    # --- Supervisor ------------------------------------------------------

    async def _supervise(self, conn: Any) -> None:
        """Own the link: route inbound frames; on death fail pending
        calls, announce ``disconnected``, redial with backoff until the
        daemon returns, then announce ``reconnected``."""
        try:
            while True:
                try:
                    while True:
                        self._on_message(await conn.recv())
                except Exception:
                    pass  # ConnectionClosed or any recv failure = dead link
                if self._closed:
                    return
                self._on_link_down()
                delay = _RECONNECT_MIN
                while not self._closed:
                    await asyncio.sleep(delay)
                    if self._closed:
                        return
                    try:
                        conn = await self._dial()
                        break
                    except Exception:
                        delay = min(delay * 2, _RECONNECT_MAX)
                if self._closed:
                    # close() ran while this link was coming up — it must
                    # not leak open on a closed client.
                    await _quiet_close(conn)
                    return
                self._conn = conn
                self._link_up.set()
                self._push({"type": "reconnected"})
                self._auto_resubscribe()
        except asyncio.CancelledError:
            raise
        finally:
            await _quiet_close(conn)

    def _auto_resubscribe(self) -> None:
        """Re-issue ``session.resume`` for every touched session after a
        redial — the daemon re-attaches the backend and replays missed
        events (``replay: true``), so subscriptions survive daemon
        restarts. Fire-and-forget: failures surface on the caller's
        next real call, never here."""
        if self._touched is None:
            return
        for session_id in list(self._touched):
            try:
                asyncio.create_task(self._resume_fire_and_forget(session_id))
            except RuntimeError:  # no running loop (tests) — skip quietly
                pass

    async def _resume_fire_and_forget(self, session_id: str) -> None:
        try:
            await self._call("session.resume", {"sessionId": session_id})
        except Exception:
            pass  # deleted meanwhile / daemon refused — not our caller's problem

    def _touch(self, session_id: str) -> None:
        if self._touched is not None:
            self._touched[session_id] = None  # ordered-set semantics

    def _on_link_down(self) -> None:
        self._conn = None
        self._link_up.clear()
        # Fail pending calls — their responses can never arrive. Prompt
        # turns also report the failure as a prompt_done event, ordered
        # before the disconnected event (JS-client parity).
        pending, self._pending = self._pending, {}
        err = ConnectionError("connection lost; reconnecting")
        for p in pending.values():
            self._settle(p, error=err)
        self._push({"type": "disconnected"})

    # --- Frame routing ---------------------------------------------------

    def _on_message(self, raw: Any) -> None:
        # A malformed frame must not kill the supervisor.
        try:
            m = json.loads(raw)
        except (TypeError, ValueError):
            return
        if not isinstance(m, dict):
            return
        # Server push: {"event": "session.event", "sessionId", "data":
        # <StreamEvent>} — the only push frame v2 defines besides hello.
        if isinstance(m.get("event"), str):
            self._push(
                {
                    "type": "event",
                    "sessionId": m.get("sessionId"),
                    "event": m.get("data"),
                    # True for catch-up frames the daemon replays to a
                    # late subscriber — consumers that already rendered
                    # the history skip them.
                    "replay": m.get("replay") is True,
                }
            )
            return
        # {"hello": {protocol, daemon, version, …}} — sent on every
        # (re)connect; also the result of a "hello" request.
        hello = m.get("hello")
        if isinstance(hello, dict):
            self.server_hello = hello
            return
        if m.get("method") is not None:
            return  # v2 has no server-initiated requests/notifications
        # Response to one of our requests — or to a request we already
        # answered; unknown ids are ignored.
        rid = m.get("id")
        if isinstance(rid, int) and not isinstance(rid, bool):
            p = self._pending.pop(rid, None)
            if p is not None:
                err = m.get("error")
                if isinstance(err, dict) and err:
                    self._settle(
                        p, error=RpcError(err.get("code"), err.get("message", "rpc error"))
                    )
                else:
                    self._settle(p, result=m.get("result"))

    def _settle(self, p: _Pending, result: Any = None, error: Optional[BaseException] = None) -> None:
        if p.prompt_session is not None:
            if error is not None:
                ev: dict[str, Any] = {
                    "type": "prompt_done",
                    "sessionId": p.prompt_session,
                    "error": error,
                }
            else:
                ev = {"type": "prompt_done", "sessionId": p.prompt_session, "result": result}
            self._push(ev)
        if p.future.done():
            return
        if error is not None:
            p.future.set_exception(error)
        else:
            p.future.set_result(result)

    def _push(self, ev: Any) -> None:
        q = self._events
        if q.full():
            # A slow consumer must never wedge the supervisor. Drop the
            # OLDEST event that isn't a permission ask — a dropped
            # permission_requested would leave the daemon waiting out
            # its timeout for an answer that never comes. Every drop is
            # counted.
            stash: list[Any] = []
            victim: Any = None
            while not q.empty():
                e = q.get_nowait()
                if not (
                    isinstance(e, dict)
                    and e.get("type") == "event"
                    and isinstance(e.get("event"), dict)
                    and e["event"].get("type") == "permission_requested"
                ):
                    victim = e
                    break
                stash.append(e)
            else:
                victim = stash.pop(0) if stash else None  # only asks queued
            for e in stash:
                q.put_nowait(e)
            self._dropped += 1
        q.put_nowait(ev)

    # --- Send path ---------------------------------------------------------

    async def _ready(self) -> None:
        """Gate every outbound frame on a live link. While reconnecting,
        wait up to ``_RECONNECT_WAIT``; fail fast once closed — except a
        call already parked here, which sits out the wait (JS parity)."""
        if self._closed:
            raise ConnectionError("connection closed")
        if not self._link_up.is_set():
            try:
                await asyncio.wait_for(self._link_up.wait(), _RECONNECT_WAIT)
            except asyncio.TimeoutError:
                raise ConnectionError("timed out waiting for reconnect") from None

    async def _send(self, text: str) -> None:
        conn = self._conn
        if conn is None:
            raise ConnectionError("connection closed")
        async with self._send_lock:
            await conn.send(text)

    async def _call(
        self, method: str, params: Optional[dict[str, Any]], prompt_session: Optional[str] = None
    ) -> Any:
        await self._ready()
        rid = self._next_id = self._next_id + 1
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        fut.add_done_callback(_reap)
        self._pending[rid] = _Pending(fut, prompt_session)
        frame: dict[str, Any] = {"id": rid, "method": method}
        if params is not None:
            frame["params"] = params
        try:
            await self._send(json.dumps(frame))
        except BaseException:
            # A failed send must not leave the id registered forever.
            self._pending.pop(rid, None)
            raise
        return await fut

    # --- Public API --------------------------------------------------------

    def events(self) -> AsyncIterator[dict[str, Any]]:
        """Async iterator over daemon events — dicts with a ``type`` of:

        - ``"connected"``     — the initial link came up
        - ``"event"``         — ``{"sessionId", "event"}`` where ``event``
                                is a StreamEvent (``{turn_id?, type, …}``;
                                timeline items ride as
                                ``{type:"timeline", kind:…}``)
        - ``"prompt_done"``   — ``{"sessionId", "result" | "error"}`` turn end
        - ``"disconnected"``  — link dropped, redialing
        - ``"reconnected"``   — link is back (the daemon restarted)

        Survives reconnects; ends after :meth:`close`. Single consumer —
        the underlying queue is consumed, not broadcast (Rust parity).
        """
        return self._events_iter()

    async def _events_iter(self) -> AsyncIterator[dict[str, Any]]:
        while True:
            ev = await self._events.get()
            if ev is _CLOSE:
                return
            yield ev

    async def request(self, method: str, params: Optional[dict[str, Any]] = None) -> Any:
        """One generic JSON-RPC request → the ``result`` object; raises
        :class:`RpcError` on an error response. While the link is down
        this waits (up to 10s) for the reconnect supervisor."""
        return await self._call(method, params)

    async def respond_permission(
        self, session_id: str, request_id: str, response: dict[str, Any]
    ) -> None:
        """Answer a ``permission_requested`` event. ``response`` is a
        PermissionResponse: ``{"behavior": "allow", "action_id"?, …}`` or
        ``{"behavior": "deny", "action_id"?, "message"?, "interrupt"?}``."""
        await self._call(
            "permission.respond",
            {"sessionId": session_id, "requestId": request_id, "response": response},
        )

    def dropped_events(self) -> int:
        """Events dropped because the queue filled — nonzero means a slow
        consumer lost streamed chunks. Consume faster."""
        return self._dropped

    def close(self) -> None:
        """Stop reconnecting, drop the link, fail pending calls with
        ``ConnectionError`` (prompts also get a ``prompt_done`` error
        event), and end ``events()`` iterators."""
        if self._closed:
            return
        self._closed = True
        conn, self._conn = self._conn, None
        if conn is not None:
            closer = getattr(conn, "close_transport", None)
            if closer is not None:  # websockets >= 13: sync transport close
                try:
                    closer()
                except Exception:
                    pass
            else:  # legacy: close asynchronously, best effort
                try:
                    asyncio.get_running_loop().create_task(_quiet_close(conn))
                except RuntimeError:
                    pass
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
        pending, self._pending = self._pending, {}
        err = ConnectionError("connection closed")
        for p in pending.values():
            self._settle(p, error=err)
        self._push(_CLOSE)

    # --- Convenience wrappers (wire methods, JS/Rust-parity names) --------

    async def hello(self) -> dict[str, Any]:
        """Handshake → ``{protocol, daemon, version, backends, methods}``.
        The same payload also arrives unsolicited on every (re)connect and
        is kept on :attr:`server_hello`."""
        return await self._call("hello", None)

    async def new_session(self, cwd: str, backend: Optional[str] = None) -> str:
        """Create a session → ``sessionId``. ``backend`` picks the agent
        backend (a catalog id like ``"claude"``); omitted → the daemon's
        default backend."""
        params: dict[str, Any] = {"cwd": cwd}
        if backend is not None:
            params["backend"] = backend
        r = await self._call("session.create", params)
        self._touch(r["sessionId"])
        return r["sessionId"]

    async def list_sessions(
        self, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> list[dict[str, Any]]:
        """All sessions as ``[{sessionId, createdAt, backend, title}]``."""
        params: dict[str, Any] = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        r = await self._call("session.list", params)
        return r.get("sessions", [])

    async def session_messages(
        self, session_id: str, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> list[dict[str, Any]]:
        """Session history as stored messages; page with ``limit``/``offset``."""
        params: dict[str, Any] = {"sessionId": session_id}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        r = await self._call("session.messages", params)
        return r.get("messages", [])

    async def resume_session(self, session_id: str) -> str:
        """Reattach after a daemon restart → ``sessionId``. Raises if unknown."""
        r = await self._call("session.resume", {"sessionId": session_id})
        self._touch(session_id)
        return r["sessionId"]

    async def resume_by_handle(
        self,
        handle: dict[str, Any],
        title: Optional[str] = None,
        cwd: Optional[str] = None,
    ) -> str:
        """Resume a native session by its persistence handle — the import
        path for sessions the backend made outside the daemon. Returns
        the Damon ``sessionId`` (deduped: same handle → same session)."""
        params: dict[str, Any] = {"handle": handle}
        if title is not None:
            params["title"] = title
        if cwd is not None:
            params["cwd"] = cwd
        r = await self._call("session.resume", params)
        self._touch(r["sessionId"])
        return r["sessionId"]

    async def import_sessions(
        self, backend: str, cwd: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """Sessions importable from a backend's native store."""
        params: dict[str, Any] = {"backend": backend}
        if cwd is not None:
            params["cwd"] = cwd
        r = await self._call("session.import", params)
        return r.get("sessions", [])

    async def fork_session(self, session_id: str, upto: Optional[int] = None) -> str:
        """Fork a session → new ``sessionId``; ``upto`` (an original
        message id) copies history only up to and including it."""
        r = await self._call("session.fork", {"sessionId": session_id, "upto": upto})
        return r["sessionId"]

    async def delete_session(self, session_id: str) -> None:
        """Delete a session and its history."""
        await self._call("session.delete", {"sessionId": session_id})

    async def rename_session(self, session_id: str, title: str) -> None:
        """Set a session's display title."""
        await self._call("session.rename", {"sessionId": session_id, "title": title})

    async def search(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Full-text search over all history → ``[{sessionId, messageId, snippet}]``."""
        r = await self._call("session.search", {"query": query, "limit": limit})
        return r.get("results", [])

    async def usage(self, session_id: Optional[str] = None) -> dict[str, Any]:
        """One session's totals with ``session_id``, or the ``{sessions}``
        rollup without it."""
        params: dict[str, Any] = {} if session_id is None else {"sessionId": session_id}
        return await self._call("session.usage", params)

    async def prompt(
        self,
        session_id: str,
        text: Union[str, list[dict[str, Any]]],
        timeout_secs: Optional[int] = None,
        detach: bool = False,
    ) -> dict[str, Any]:
        """Start a turn and resolve with ``{turnId, stopReason, usage?}``
        when it ends — events stream first, then this response. ``text``
        is a string or raw content blocks (image/resource alongside
        text). The same outcome is also delivered as a ``prompt_done``
        event for consumers that only iterate :meth:`events`.

        With ``detach=True`` the call resolves immediately with
        ``{turnId, detached: true}`` and the turn keeps running on the
        daemon even if this client disconnects — the outcome arrives as
        stream events (or replay on the next connect) instead of this
        response."""
        params: dict[str, Any] = {"sessionId": session_id, "prompt": text}
        if timeout_secs is not None:
            params["timeoutSecs"] = timeout_secs
        if detach:
            params["detach"] = True
        self._touch(session_id)
        return await self._call("turn.start", params, prompt_session=session_id)

    async def steer(
        self,
        session_id: str,
        text: Union[str, list[dict[str, Any]]],
        expected_turn: Optional[str] = None,
    ) -> dict[str, Any]:
        """Steer a live turn with an additional prompt → ``{result}``."""
        params: dict[str, Any] = {"sessionId": session_id, "prompt": text}
        if expected_turn is not None:
            params["expectedTurn"] = expected_turn
        return await self._call("turn.steer", params)

    async def cancel(self, session_id: str) -> dict[str, Any]:
        """Cancel a live turn → ``{cancelled: true}``."""
        return await self._call("turn.cancel", {"sessionId": session_id})

    async def set_model(self, session_id: str, model: str) -> None:
        """Set the session's model."""
        await self._call("session.set_model", {"sessionId": session_id, "model": model})

    async def set_mode(self, session_id: str, mode: str) -> None:
        """Set the session's mode."""
        await self._call("session.set_mode", {"sessionId": session_id, "mode": mode})

    async def backends(self) -> list[dict[str, Any]]:
        """List agent backends → ``[{id, available, capabilities}]``."""
        r = await self._call("backend.list", None)
        return r.get("backends", [])

    async def catalog(self, backend: str) -> dict[str, Any]:
        """Backend catalog → ``{models, modes, commands}``."""
        return await self._call("catalog.models", {"backend": backend})

