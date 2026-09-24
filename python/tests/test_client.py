"""Integration tests for DamonClient against a REAL local WS server.

The mock daemon below implements the minimal wire surface from
docs/protocol-v2.md: hello echoes the backend list, session.create mints
an id, turn.start streams one session.event push (after an optional
permission_requested ask that must be answered via permission.respond)
and resolves with stopReason "end_turn"; everything else gets -32601.
"""

import asyncio
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
