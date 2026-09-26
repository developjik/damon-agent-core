"""Integration tests for DamonClient against a REAL local WS server.

The mock daemon below implements the minimal wire surface from
docs/protocol-v2.md: hello echoes the backend list, session.create mints
an id, turn.start streams one session.event push (after an optional
permission_requested ask that must be answered via permission.respond)
and resolves with stopReason "end_turn"; everything else gets -32601.
"""

import asyncio
import time
import json
import uuid
from typing import Any, AsyncIterator, Optional

import pytest
import websockets

from damon_agent import DamonClient, RpcError

ALLOW = {"behavior": "allow"}


def _ok(rid: int, result: Any) -> str:
    return json.dumps({"id": rid, "result": result})


def _err(rid: int, code: int, message: str) -> str:
    return json.dumps({"id": rid, "error": {"code": code, "message": message}})


class MockDamon:
    """Minimal damon built on the websockets server."""

    def __init__(self, ask_permission: bool = False) -> None:
        self.ask_permission = ask_permission
        self.backends = [{"id": "mock", "available": True, "capabilities": {}}]
        # Every request any incarnation answered, oldest first — the
        # auto-resume tests assert session.resume hit the wire.
        self.requests: list = []
        self.port: Optional[int] = None
        self._server: Any = None
        self._conns: set = set()

    @property
    def url(self) -> str:
        assert self.port is not None
        return f"ws://127.0.0.1:{self.port}/ws"

    async def start(self) -> "MockDamon":
        self._server = await websockets.serve(self._handle, "127.0.0.1", self.port or 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def stop(self) -> None:
        if self._server is None:
            return
        server, self._server = self._server, None
        conns, self._conns = list(self._conns), set()
        server.close()  # stop accepting FIRST so redials get refused, not adopted
        for c in conns:
            try:
                await c.close()
            except Exception:
                pass
        await server.wait_closed()

    async def _handle(self, ws: Any, path: Optional[str] = None) -> None:
        self._conns.add(ws)
        try:
            # v2 pushes a hello greeting on every connect.
            await ws.send(
                json.dumps(
                    {"hello": {"protocol": 2, "daemon": "damond", "version": "0.0.0-test"}}
                )
            )
            async for raw in ws:
                try:
                    m = json.loads(raw)
                except ValueError:
                    continue
                method = m.get("method")
                if method is None:
                    continue
                self.requests.append((method, m.get("params")))
                if method == "hello":
                    await ws.send(
                        _ok(
                            m["id"],
                            {
                                "protocol": 2,
                                "daemon": "damond",
                                "version": "0.0.0-test",
                                "backends": self.backends,
                                "methods": [],
                            },
                        )
                    )
                elif method == "session.create":
                    await ws.send(
                        _ok(
                            m["id"],
                            {
                                "sessionId": f"s-{uuid.uuid4().hex[:12]}",
                                "backend": m["params"].get("backend") or "mock",
                            },
                        )
                    )
                elif method == "session.resume":
                    await ws.send(
                        _ok(
                            m["id"],
                            {"sessionId": m["params"].get("sessionId", ""), "backend": "mock"},
                        )
                    )
                elif method == "turn.start":
                    session_id = m["params"]["sessionId"]
                    if self.ask_permission:
                        request_id = f"perm-{uuid.uuid4().hex[:8]}"
                        await ws.send(
                            json.dumps(
                                {
                                    "event": "session.event",
                                    "sessionId": session_id,
                                    "data": {
                                        "type": "permission_requested",
                                        "id": request_id,
                                        "kind": "tool",
                                        "name": "Bash",
                                        "title": "rm -rf",
                                        "actions": [
                                            {"id": "allow", "label": "Allow", "behavior": "allow"},
                                            {"id": "deny", "label": "Deny", "behavior": "deny"},
                                        ],
                                    },
                                }
                            )
                        )
                        answer = await self._await_request(ws, "permission.respond")
                        await ws.send(_ok(answer["id"], {}))
                        if answer["params"]["response"].get("behavior") != "allow":
                            await ws.send(_err(m["id"], -32000, "permission denied"))
                            continue
                    await ws.send(
                        json.dumps(
                            {
                                "event": "session.event",
                                "sessionId": session_id,
                                "data": {
                                    "turn_id": "t-1",
                                    "type": "timeline",
                                    "kind": "assistant_message",
                                    "text": "hi there",
                                },
                            }
                        )
                    )
                    await ws.send(
                        _ok(m["id"], {"turnId": "t-1", "stopReason": "end_turn"})
                    )
                else:
                    await ws.send(_err(m["id"], -32601, "Method not found"))
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self._conns.discard(ws)

    @staticmethod
    async def _await_request(ws: Any, want_method: str) -> dict:
        while True:
            r = json.loads(await ws.recv())
            if r.get("method") == want_method:
                return r


@pytest.fixture
async def mock() -> AsyncIterator[MockDamon]:
    m = await MockDamon().start()
    yield m
    await m.stop()


async def next_event(events: AsyncIterator[dict], timeout: float = 10.0) -> dict:
    return await asyncio.wait_for(events.__anext__(), timeout)


# (a) connect + hello round-trip; first event reports the link-up.


async def test_connect_and_hello(mock: MockDamon) -> None:
    client = await DamonClient.connect(mock.url)
    events = client.events()
    try:
        result = await client.hello()
        assert result["protocol"] == 2
        assert result["backends"] == [{"id": "mock", "available": True, "capabilities": {}}]
        assert client.server_hello["protocol"] == 2  # pushed on connect
        assert await next_event(events) == {"type": "connected"}
    finally:
        client.close()
        await events.aclose()


# (b) prompt resolves with the turn result; the event stream saw the
#     streamed session.event and the same result as a prompt_done event.


async def test_prompt_streams_event_and_returns_result(mock: MockDamon) -> None:
    client = await DamonClient.connect(mock.url)
    events = client.events()
    try:
        await client.hello()
        session_id = await client.new_session("/tmp", backend="mock")
        assert session_id

        turn = asyncio.create_task(client.prompt(session_id, "hello"))
        saw_event = False
        done: Optional[dict] = None
        while done is None:
            ev = await next_event(events)
            if ev["type"] == "event":
                assert ev["sessionId"] == session_id
                assert ev["event"]["type"] == "timeline"
                assert ev["event"]["kind"] == "assistant_message"
                assert ev["event"]["text"] == "hi there"
                saw_event = True
            elif ev["type"] == "prompt_done":
                done = ev

        result = await turn
        assert result == {"turnId": "t-1", "stopReason": "end_turn"}
        assert saw_event
        assert done is not None
        assert done["sessionId"] == session_id
        assert done["result"] == result
    finally:
        client.close()
        await events.aclose()


# (c) a permission ask surfaces as a permission_requested event;
#     respond_permission() completes it and the turn then finishes.


async def test_permission_request_round_trip(mock: MockDamon) -> None:
    mock.ask_permission = True
    client = await DamonClient.connect(mock.url)
    events = client.events()
    try:
        await client.hello()
        session_id = await client.new_session("/tmp")
        turn = asyncio.create_task(client.prompt(session_id, "do a thing"))
        assert (await next_event(events))["type"] == "connected"  # initial link-up, buffered
        ev = await next_event(events)
        assert ev["type"] == "event"
        assert ev["sessionId"] == session_id
        assert ev["event"]["type"] == "permission_requested"
        assert ev["event"]["title"] == "rm -rf"

        await client.respond_permission(session_id, ev["event"]["id"], ALLOW)

        assert (await next_event(events))["type"] == "event"
        result = await turn
        assert result == {"turnId": "t-1", "stopReason": "end_turn"}
    finally:
        client.close()
        await events.aclose()


# (d) server death → disconnected event; restart on the same port →
#     reconnected event and a subsequent request succeeds.


async def test_reconnect_after_server_restart(mock: MockDamon) -> None:
    client = await DamonClient.connect(mock.url)
    events = client.events()
    try:
        assert (await next_event(events))["type"] == "connected"
        assert (await client.hello())["protocol"] == 2

        await mock.stop()  # the daemon dies under us
        assert await next_event(events, timeout=5) == {"type": "disconnected"}

        await mock.start()  # daemon comes back on the same port
        assert (await next_event(events))["type"] == "reconnected"

        assert (await client.hello())["protocol"] == 2
    finally:
        client.close()
        await events.aclose()


# (d2) the reconnect re-subscribes: sessions the client touched are
#      resumed on the new link (fire-and-forget), so event streams
#      survive daemon restarts without the caller re-issuing anything.


async def test_reconnect_auto_resumes_touched_sessions(mock: MockDamon) -> None:
    client = await DamonClient.connect(mock.url)
    events = client.events()
    try:
        assert (await next_event(events))["type"] == "connected"
        await client.prompt("sid-py", "hi")  # touches the session
        # Drain the turn's stream artifacts — the next lifecycle event
        # we care about is the disconnect.
        while (ev := await next_event(events))["type"] != "prompt_done":
            pass

        await mock.stop()
        assert await next_event(events, timeout=5) == {"type": "disconnected"}
        await mock.start()
        assert await next_event(events) == {"type": "reconnected"}

        async def resumed() -> bool:
            return any(
                m == "session.resume" and (p or {}).get("sessionId") == "sid-py"
                for m, p in mock.requests
            )

        deadline = time.monotonic() + 5.0
        while not await resumed():
            assert time.monotonic() < deadline, f"auto-resume never hit: {mock.requests}"
            await asyncio.sleep(0.02)
    finally:
        client.close()
        await events.aclose()


async def test_auto_resume_can_be_disabled(mock: MockDamon) -> None:
    client = await DamonClient.connect(mock.url, auto_resume=False)
    events = client.events()
    try:
        assert (await next_event(events))["type"] == "connected"
        await client.prompt("sid-off", "hi")
        while (ev := await next_event(events))["type"] != "prompt_done":
            pass

        await mock.stop()
        assert await next_event(events, timeout=5) == {"type": "disconnected"}
        await mock.start()
        assert await next_event(events) == {"type": "reconnected"
        } and await client.hello()

        await asyncio.sleep(0.2)  # any unwanted resume had its chance
        assert not [m for m, _ in mock.requests if m == "session.resume"], mock.requests
    finally:
        client.close()
        await events.aclose()


# (e) an error response raises RpcError carrying code and message.


async def test_rpc_error_carries_code_and_message(mock: MockDamon) -> None:
    client = await DamonClient.connect(mock.url)
    events = client.events()
    try:
        await client.hello()
        with pytest.raises(RpcError) as ei:
            await client.request("no.such.method", {})
        assert ei.value.code == -32601
        assert ei.value.message == "Method not found"
        assert "Method not found" in str(ei.value)
    finally:
        client.close()
        await events.aclose()

# (f) relay transport — pure-protocol vectors first (the cross-
#     implementation contract: relay.rs goldens, RFC 7748, NIST AES-GCM,
#     and vectors generated from npm/client.mjs — the reference port),
#     then the full path against a mock daemon behind a real WS server,
#     mirroring the npm client's relay tests.

import base64
import hashlib
import hmac
import re

from damon_agent.client import _TEST as T

RELAY_TOKEN = "test-token-0123456789abcdef"


def test_relay_encode_vectors() -> None:
    assert T["relay_encode"]("home") == "home"
    assert T["relay_encode"]("a b/c") == "a%20b%2Fc"
    assert T["relay_encode"]("a-b_c.d~e") == "a-b_c.d~e"
    # '!' is NOT unreserved in relay.rs urlencoding — must be %21.
    assert T["relay_encode"]("hi!") == "hi%21"
    assert T["relay_encode"]("hi()*") == "hi%28%29%2A"
    # Non-ASCII encodes as UTF-8 bytes, byte-wise.
    assert T["relay_encode"]("한") == "%ED%95%9C"


def test_seq_nonce_vectors() -> None:
    assert T["seq_nonce"](0) == bytes(12)
    assert T["seq_nonce"](1) == bytes([0] * 11 + [1])
    assert T["seq_nonce"](0x0102) == bytes([0] * 10 + [1, 2])
    assert T["seq_nonce"](2**32) == bytes([0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0])


def test_proof_vectors() -> None:
    # sha256 vector recomputed with hashlib — mirrors npm's node:crypto
    # cross-check; the proof binds BOTH keys in order.
    token = "sekrit-token"
    mine = bytes(range(1, 33))
    theirs = bytes(range(255, 223, -1))
    expect = base64.b64encode(
        hashlib.sha256(token.encode() + mine + theirs).digest()
    ).decode()
    assert T["proof"](token, mine, theirs) == expect
    assert T["proof"](token, theirs, mine) != expect

    # relay.rs golden vectors — the implementations must agree
    # byte-for-byte or Rust↔npm↔python relay handshakes fail auth.
    legacy = T["proof"]("tok", b"\x01" * 32, b"\x03" * 32)
    assert legacy == "S8mawcxH7YVZq1N0D+PC7Q48QoULbq/LSh/T0S+p2mI="
    stretched = T["proof"]("tok", b"\x01" * 32, b"\x03" * 32, True)
    assert stretched == "pEQzR8r3Ev4sJ+cFta1ZJEpgGRynd3Ix9EFoElBMBqQ="
    # Stretch differs from legacy, is deterministic, and still binds
    # the token and both pubkeys.
    assert stretched != legacy
    assert stretched == T["proof"]("tok", b"\x01" * 32, b"\x03" * 32, True)
    assert stretched != T["proof"]("tok2", b"\x01" * 32, b"\x03" * 32, True)
    assert stretched != T["proof"]("tok", b"\x02" * 32, b"\x03" * 32, True)
    assert stretched != T["proof"]("tok", b"\x01" * 32, b"\x02" * 32, True)


def test_derive_keys_vectors() -> None:
    shared = bytes((i * 7) % 256 for i in range(32))
    for label in (b"d2c", b"c2d"):
        assert T["derive_keys"](shared, label) == hashlib.sha256(
            shared + label
        ).digest()
    # Direction separation: the two labels must yield different keys.
    assert T["derive_keys"](shared, b"d2c") != T["derive_keys"](shared, b"c2d")


def test_x25519_rfc7748_vectors() -> None:
    # §5.2 scalar-mult vector.
    k = bytes.fromhex(
        "a546e36bf0527c9d3b16154b82465edd62144c0ac1fc5a18506a2244ba449ac4"
    )
    u = bytes.fromhex(
        "e6db6867583030db3594c1a424b15f7c726624ec26b3353b10a903a6d0ab1c4c"
    )
    assert T["x25519"](k, u) == bytes.fromhex(
        "c3da55379de9c6908e94ea4df28d084f32eccf03491c71f754b4075577a28552"
    )
    # §6.1 Diffie-Hellman: public keys and the shared secret both ways.
    a = bytes.fromhex(
        "77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a"
    )
    b = bytes.fromhex(
        "5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb"
    )
    base = (9).to_bytes(32, "little")
    a_pub = T["x25519"](a, base)
    b_pub = T["x25519"](b, base)
    assert a_pub == bytes.fromhex(
        "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a"
    )
    assert b_pub == bytes.fromhex(
        "de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f"
    )
    shared = T["x25519"](a, b_pub)
    assert shared == bytes.fromhex(
        "4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742"
    )
    assert shared == T["x25519"](b, a_pub)
    # A keypair's public key is the scalar mult of the base point.
    priv, pub = T["x25519_keypair"]()
    assert pub == T["x25519"](priv, base)
    # Low-order (all-zero) public key → non-contributory share rejected.
    with pytest.raises(ValueError, match="non-contributory"):
        T["x25519_shared"](a, bytes(32))


def test_frame_codec_nist_and_npm_vectors() -> None:
    # NIST AES-256-GCM test cases 13/14 ride through the frame codec:
    # seq 0 ⇒ IV = 0^12, exactly the vectors' IV.
    k0 = bytes(32)
    f = base64.b64decode(T["seal_frame"](k0, 0, ""))
    assert f == bytes(20) + bytes.fromhex("530f8afbc74536b9a963b4f1c4cb738b")
    f = base64.b64decode(T["seal_frame"](k0, 0, "\x00" * 16))
    assert f == bytes(20) + bytes.fromhex(
        "cea7403d4d606b6e074ec5d3baf39d18d0d1c8a799996bf0265b98b5d48ab919"
    )

    # Cross-implementation frame vectors generated by npm/client.mjs's
    # sealFrame (Node WebCrypto AES-256-GCM) over the RFC 7748 §6.1
    # shared secret — the reference port must agree byte-for-byte.
    shared = bytes.fromhex(
        "4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742"
    )
    c2d = T["derive_keys"](shared, b"c2d")
    d2c = T["derive_keys"](shared, b"d2c")
    text = '{"id": 1, "method": "hello", "params": {}}'
    s0 = T["seal_frame"](c2d, 0, text)
    s1 = T["seal_frame"](c2d, 1, text)
    assert s0 == (
        "AAAAAAAAAAAAAAAAAAAAAAAAAAC39bGVowEcM8iEqU/G8+/9G99fehRBqfqgC7KexqaDl"
        "LOIvfPQ1SUR8brFHKvCDTcPD9QjT2H7zv79"
    )
    assert s1 == (
        "AAAAAAAAAAEAAAAAAAAAAAAAAAH9kxB3GECBDYO+hSxp1NmgX20fnBSOe94b/LM6Mc+GjW"
        "VFZ2y1nmz6DkYRxbnrCJt7jn6aeJw3Ksmn"
    )
    assert T["open_frame"](c2d, 0, s0) == (0, text)
    assert T["open_frame"](c2d, 1, s1) == (1, text)

    # Frame layout: seq(8B BE) ‖ nonce(12B) ‖ ct+tag(16B) — ascii
    # plaintext is 1 byte/char.
    raw = base64.b64decode(s0)
    assert len(raw) == 20 + len(text) + 16
    assert raw[:8] == bytes(8)
    assert raw[8:20] == T["seq_nonce"](0)

    # Reflection: a frame sent back must not decrypt under the other
    # direction's key.
    r0 = T["seal_frame"](d2c, 0, text)
    assert T["open_frame"](d2c, 0, r0)[1] == text
    with pytest.raises(ValueError, match="decrypt failed"):
        T["open_frame"](c2d, 0, r0)

    # Tamper: a flipped ciphertext byte fails the GCM tag.
    tampered = bytearray(base64.b64decode(s0))
    tampered[25] ^= 0xFF
    with pytest.raises(ValueError, match="decrypt failed"):
        T["open_frame"](c2d, 0, base64.b64encode(bytes(tampered)))
    # Replay / gap: the strict sequence check fires before any crypto.
    with pytest.raises(ValueError, match="replay or reorder"):
        T["open_frame"](c2d, 1, s0)
    s2 = T["seal_frame"](c2d, 2, text)
    with pytest.raises(ValueError, match="replay or reorder"):
        T["open_frame"](c2d, 1, s2)
    # Truncated and non-base64 frames.
    with pytest.raises(ValueError, match="frame too short"):
        T["open_frame"](c2d, 0, base64.b64encode(bytes(10)))
    with pytest.raises(ValueError, match="bad base64"):
        T["open_frame"](c2d, 0, "!!!")
    # Oversized plaintext is refused — rpc.rs MAX_RESPONSE_BYTES parity:
    # the sealed frame would blow the relay's 4 MiB socket cap.
    with pytest.raises(ValueError, match="frame budget"):
        T["seal_frame"](c2d, 0, "x" * ((1 << 20) + 1))


def _envelope(obj: Any) -> str:
    return json.dumps({"data": json.dumps(obj)})


class MockRelayDaemon:
    """The daemon side of the relay protocol (relay.rs
    daemon_handshake) behind a plain WS server: answers the client's
    pub, verifies the client's proof FIRST, then sends its own;
    afterwards decrypts requests and answers with sealed frames.

    ``legacy`` pretends to be an older daemon — no kdf echo, always the
    single-hash proof. ``silent`` accepts the socket and never answers.
    Reuses the client's own (vector-pinned) primitives, exactly like
    npm's makeDaemon reuses __test.
    """

    def __init__(self, token: str, legacy: bool = False, silent: bool = False) -> None:
        self.token = token
        self.legacy = legacy
        self.silent = silent
        self.port: Optional[int] = None
        self.paths: list[str] = []
        self.client_pubs: list[str] = []  # b64 pubkeys, per handshake
        self.client_wire: list[str] = []  # inner payloads the client sent
        self.sent: list[str] = []  # raw frames the daemon sent
        self.plaintext: list[str] = []  # decrypted client requests
        self.handshakes = 0
        self.proof_rejected = False
        self._server: Any = None
        self._conns: list = []

    @property
    def url(self) -> str:
        assert self.port is not None
        return f"ws://127.0.0.1:{self.port}"

    async def start(self) -> "MockRelayDaemon":
        self._server = await websockets.serve(self._handle, "127.0.0.1", self.port or 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def stop(self) -> None:
        if self._server is None:
            return
        server, self._server = self._server, None
        conns, self._conns = list(self._conns), []
        server.close()
        for c in conns:
            try:
                await c.close()
            except Exception:
                pass
        await server.wait_closed()

    async def drop_last(self) -> None:
        """The relay side drops the most recent client connection."""
        assert self._conns
        await self._conns[-1].close()

    async def _handle(self, ws: Any, path: Optional[str] = None) -> None:
        request = getattr(ws, "request", None)
        self.paths.append(getattr(request, "path", None) or path or "")
        self._conns.append(ws)
        try:
            if self.silent:
                async for _raw in ws:  # swallow frames, never answer
                    pass
                return
            priv, pub = T["x25519_keypair"]()
            keys: dict[str, Any] = {}
            async for raw in ws:
                try:
                    env = json.loads(raw)
                except ValueError:
                    continue
                if not isinstance(env, dict):
                    continue
                inner = env.get("data")
                if not isinstance(inner, str):
                    continue
                self.client_wire.append(inner)
                try:
                    m = json.loads(inner)
                except ValueError:
                    m = None
                if isinstance(m, dict) and isinstance(m.get("e2e_pub"), str):
                    # Mirror relay.rs daemon_handshake: echo the
                    # stretched-proof KDF only when the client offered it.
                    stretch = (not self.legacy) and m.get("kdf") == "s256"
                    hello = {"e2e_pub": base64.b64encode(pub).decode()}
                    if stretch:
                        hello["kdf"] = "s256"
                    keys.update(theirs=base64.b64decode(m["e2e_pub"]), stretch=stretch)
                    self.client_pubs.append(m["e2e_pub"])
                    await ws.send(_envelope(hello))  # bare pub — no proof yet
                    continue
                if isinstance(m, dict) and isinstance(m.get("e2e_proof"), str):
                    expected = T["proof"](self.token, keys["theirs"], pub, keys["stretch"])
                    if not hmac.compare_digest(m["e2e_proof"], expected):
                        self.proof_rejected = True
                        await ws.send(_envelope({"e2e_error": "auth"}))
                        return
                    shared = T["x25519_shared"](priv, keys["theirs"])
                    keys.update(
                        send=T["derive_keys"](shared, b"d2c"),
                        recv=T["derive_keys"](shared, b"c2d"),
                        send_seq=0,
                        recv_seq=0,
                    )
                    self.handshakes += 1
                    # The daemon's proof binds the keys in ITS order:
                    # proof(token, daemon_pub, client_pub).
                    await ws.send(_envelope({
                        "e2e_proof": T["proof"](self.token, pub, keys["theirs"], keys["stretch"])
                    }))
                    # Daemon pushes its hello + one session.event right
                    # after the handshake, before any request —
                    # exercises the decrypt pipeline's queued handover.
                    await self._send_sealed(ws, keys, json.dumps({"hello": {"protocol": 2, "daemon": "damond", "version": "0.0.0-test"}}))
                    await self._send_sealed(ws, keys, json.dumps({
                        "event": "session.event", "sessionId": "s1",
                        "data": {"type": "timeline", "kind": "assistant_message", "text": "hello"},
                    }))
                    continue
                # Encrypted JSON-RPC frame: strict-sequence decrypt, answer.
                try:
                    _, text = T["open_frame"](keys["recv"], keys["recv_seq"], inner)
                except ValueError:
                    return  # fail closed, like relay.rs
                keys["recv_seq"] += 1
                self.plaintext.append(text)
                req = json.loads(text)
                await self._send_sealed(ws, keys, json.dumps({
                    "id": req["id"],
                    "result": {"protocol": 2, "daemon": "damond", "version": "0.0.0-test"},
                }))
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            if ws in self._conns:
                self._conns.remove(ws)

    async def _send_sealed(self, ws: Any, keys: dict, text: str) -> None:
        seq = keys["send_seq"]
        keys["send_seq"] += 1
        raw = json.dumps({"data": T["seal_frame"](keys["send"], seq, text)})
        self.sent.append(raw)
        await ws.send(raw)


async def next_of(events: AsyncIterator[dict], want: str, timeout: float = 10.0) -> dict:
    """Next event of the wanted type — connection-state mirrors pass."""
    while True:
        ev = await next_event(events, timeout)
        if ev.get("type") == want:
            return ev


async def until(cond: Any, what: str, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not cond():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"timeout: {what}")
        await asyncio.sleep(0.01)


async def test_connect_relay_handshake_and_encrypted_rpc() -> None:
    daemon = await MockRelayDaemon(RELAY_TOKEN).start()
    # Trailing '/' is trimmed, exactly like relay.rs client_connect.
    client = await DamonClient.connect_relay(daemon.url + "/", "home", RELAY_TOKEN)
    events = client.events()
    try:
        assert daemon.paths == ["/connect?name=home"]

        init = await client.hello()
        assert init["protocol"] == 2
        assert daemon.handshakes == 1
        assert any('"hello"' in t for t in daemon.plaintext)  # daemon did decrypt it

        # Handshake frames: bare JSON with the exact field names relay.rs
        # uses — the pub frame carries the opt-in kdf marker, echoed by
        # the daemon; the proof frame is proof-only.
        wire = [json.loads(w) for w in daemon.client_wire[:2]]
        assert list(wire[0]) == ["e2e_pub", "kdf"]
        assert wire[0]["kdf"] == "s256"
        assert list(wire[1]) == ["e2e_proof"]
        # Message order: pub, proof, then ciphertext — client-proves-first.
        for d in daemon.client_wire[2:]:
            assert re.fullmatch(r"[A-Za-z0-9+/]+={0,2}", d), f"not sealed: {d!r}"
            assert '"method":"hello"' not in d, "plaintext method leaked onto the wire"
            assert len(base64.b64decode(d)) >= 20 + 16  # seq+nonce+ct+tag

        # Daemon-pushed session.event arrives decrypted through events().
        ev = await next_of(events, "event")
        assert ev["sessionId"] == "s1"
        assert ev["event"]["kind"] == "assistant_message"
        assert ev["event"]["text"] == "hello"
        assert client.server_hello is not None
        assert client.server_hello["protocol"] == 2
    finally:
        client.close()
        await events.aclose()
        await daemon.stop()


async def test_connect_relay_reconnects_with_fresh_keypair() -> None:
    daemon = await MockRelayDaemon(RELAY_TOKEN).start()
    client = await DamonClient.connect_relay(daemon.url, "home", RELAY_TOKEN)
    events = client.events()
    try:
        await client.hello()

        await daemon.drop_last()  # relay side drops us
        await next_of(events, "disconnected")
        await until(lambda: len(daemon.paths) == 2, "second dial")
        await next_of(events, "reconnected")

        assert daemon.handshakes == 2
        # Fresh X25519 keypair per attempt.
        assert daemon.client_pubs[0] != daemon.client_pubs[1]

        r = await client.hello()  # same API over the new link
        assert r["protocol"] == 2
    finally:
        client.close()
        await events.aclose()
        await daemon.stop()


async def test_connect_relay_wrong_token_fails_fast() -> None:
    daemon = await MockRelayDaemon("right-token").start()
    try:
        with pytest.raises(ConnectionError, match="auth_token"):
            await DamonClient.connect_relay(daemon.url, "home", "wrong-token")
        assert daemon.proof_rejected
        # The failed dial closed the socket — no slot leak.
        await until(lambda: not daemon._conns, "socket closed after refusal")
    finally:
        await daemon.stop()


async def test_connect_relay_silent_relay_times_out() -> None:
    daemon = await MockRelayDaemon(RELAY_TOKEN, silent=True).start()
    try:
        with pytest.raises(ConnectionError, match="E2E handshake timed out"):
            await DamonClient.connect_relay(
                daemon.url, "home", RELAY_TOKEN, handshake_timeout=0.25
            )
        await until(lambda: not daemon._conns, "socket closed after timeout")
    finally:
        await daemon.stop()


async def test_connect_relay_legacy_daemon_interoperates() -> None:
    """Older daemons ignore the kdf offer — the handshake must fall
    back to the legacy single-hash proof and still authenticate
    (relay.rs legacy_client_without_marker_interoperates)."""
    daemon = await MockRelayDaemon(RELAY_TOKEN, legacy=True).start()
    client = await DamonClient.connect_relay(daemon.url, "home", RELAY_TOKEN)
    events = client.events()
    try:
        assert daemon.handshakes == 1  # legacy proof verified
        assert (await client.hello())["protocol"] == 2
        # We still offered s256; the daemon did not echo it.
        assert json.loads(daemon.client_wire[0]).get("kdf") == "s256"
        assert "kdf" not in json.loads(daemon.sent[0])["data"]
    finally:
        client.close()
        await events.aclose()
        await daemon.stop()


async def test_connect_relay_requires_args() -> None:
    for url, name, token in (
        ("", "home", "t"),
        ("ws://relay:1", "", "t"),
        ("ws://relay:1", "home", ""),
    ):
        with pytest.raises(ValueError, match="connect_relay:"):
            await DamonClient.connect_relay(url, name, token)
