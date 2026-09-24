"""Asyncio client for the damond WS/JSON-RPC (ACP-shaped) API.

Mirrors the Node (``damon-agent`` npm package) and Rust
(``damon::client``) clients so all three SDKs feel identical:
``connect`` → ``initialize`` → ``new_session``, ``prompt`` with streamed
``session/update`` events, permission prompts answered through
``respond()``.

A background supervisor task owns the WebSocket link. When the link
drops it fails pending calls, emits a ``disconnected`` event, and
redials with exponential backoff (100ms doubling to a 5s cap) until the
daemon returns — then emits ``reconnected``. Calls made while the link
is down wait up to 10s for it to come back.

Requires Python >= 3.11 and ``websockets`` >= 12. Relay links
(``connect_relay`` in the Node/Rust SDKs) are not part of this
milestone.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, AsyncIterator, Optional, Union

import websockets

__all__ = ["DamonClient", "RpcError"]

#: Inbound frame cap — the same wire budget the daemon and relay enforce
#: (a library default of ~64 MiB would let a hostile peer force huge
#: allocations).
_MAX_FRAME = 4 * 1024 * 1024
#: Event-queue capacity. A slow consumer makes room by dropping the
#: oldest non-request event; every drop is counted on ``dropped_events``.
_EVENT_CAPACITY = 64
#: First reconnect delay; doubles per failure up to ``_RECONNECT_MAX``.
_RECONNECT_MIN = 0.1
_RECONNECT_MAX = 5.0
#: How long a call parks on a reconnecting link before giving up.
_RECONNECT_WAIT = 10.0

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
    scheme = "https" if parts.scheme == "wss" else "http"
    ticket_url = urllib.parse.urlunsplit(
        (scheme, parts.netloc, path[:-3] + "/v1/ws_ticket", "", "")
    )

    def fetch() -> bytes:
        req = urllib.request.Request(
            ticket_url, method="POST", headers={"Authorization": f"Bearer {token}"}
        )
        with urllib.request.urlopen(req) as resp:
            return resp.read()

    try:
        body = await asyncio.to_thread(fetch)
    except urllib.error.HTTPError as e:
        raise ConnectionError(f"ws_ticket rejected: {e.code}") from None
    data = json.loads(body)
    ticket = data.get("ticket") if isinstance(data, dict) else None
    if not ticket:
        raise ConnectionError("no ticket in ws_ticket response")
    sep = "&" if parts.query else "?"
    return f"{ws_url}{sep}ticket={ticket}"


class DamonClient:
    """A connected damon client.

    Build with :meth:`connect`; never call the constructor directly.
    Every request method is a coroutine; streamed updates and
    server-initiated requests (permission prompts) arrive on
    :meth:`events`. Cheap to share within one event loop.
    """

    def __init__(self, conn: Any, dial: Any) -> None:
        self._dial = dial  # async () -> fresh connection (ticket included)
        self._conn: Any = conn
        self._task: Optional[asyncio.Task[None]] = None
        self._next_id = 0
        self._pending: dict[int, _Pending] = {}
        self._events: asyncio.Queue[Any] = asyncio.Queue(maxsize=_EVENT_CAPACITY)
        self._dropped = 0
        self._closed = False
        self._link_up = asyncio.Event()
        self._link_up.set()
        self._send_lock = asyncio.Lock()
        self._push({"type": "connected"})

    # --- Construction --------------------------------------------------

    @classmethod
    async def connect(cls, url: str, token: Optional[str] = None) -> DamonClient:
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

        self = cls(await dial(), dial)
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
        except asyncio.CancelledError:
            raise
        finally:
            await _quiet_close(conn)

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
        method = m.get("method")
        if method is None:
            # Response to one of our requests — or to a server request we
            # already answered; unknown ids are ignored.
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
            return
        params = m.get("params")
        if method == "session/update":
            pd = params if isinstance(params, dict) else {}
            ev: dict[str, Any] = {
                "type": "update",
                "sessionId": pd.get("sessionId"),
                "update": pd.get("update"),
            }
        elif "id" in m:
            # Server-initiated request (e.g. session/request_permission).
            ev = {"type": "request", "id": m["id"], "method": method, "params": params}
        else:
            ev = {"type": "notification", "method": method, "params": params}
        self._push(ev)

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
            # OLDEST non-request event — a dropped permission request
            # would leave the daemon waiting out its timeout for an
            # answer that never comes. Every drop is counted.
            stash: list[Any] = []
            victim: Any = None
            while not q.empty():
                e = q.get_nowait()
                if not (isinstance(e, dict) and e.get("type") == "request"):
                    victim = e
                    break
                stash.append(e)
            else:
                victim = stash.pop(0) if stash else None  # only requests queued
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
        frame: dict[str, Any] = {"jsonrpc": "2.0", "id": rid, "method": method}
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
        - ``"update"``        — ``{"sessionId", "update"}`` ACP session update
        - ``"request"``       — ``{"id", "method", "params"}`` server-initiated
                                request; answer with :meth:`respond`
        - ``"notification"``  — ``{"method", "params"}``
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

    async def respond(self, id: Any, outcome: dict[str, Any]) -> None:
        """Answer a server-initiated request (permission prompts).
        ``outcome`` is the JSON-RPC result object, e.g.
        ``{"outcome": {"outcome": "selected", "optionId": "allow-once"}}``."""
        await self._ready()
        try:
            await self._send(json.dumps({"jsonrpc": "2.0", "id": id, "result": outcome}))
        except Exception:
            raise ConnectionError("connection closed") from None

    async def respond_error(self, id: Any, code: int, message: str) -> None:
        """Answer a server-initiated request with a JSON-RPC error — used
        for methods the client doesn't implement, so the daemon isn't
        left waiting on a response that never comes."""
        await self._ready()
        try:
            await self._send(
                json.dumps({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
            )
        except Exception:
            raise ConnectionError("connection closed") from None

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

    async def initialize(self) -> dict[str, Any]:
        """Handshake → ``{protocolVersion, agents, agentInfo}``."""
        return await self._call(
            "initialize", {"protocolVersion": 1, "clientCapabilities": {}}
        )

    async def new_session(self, cwd: str, agent: Optional[str] = None) -> str:
        """Create a session → ``sessionId``. ``agent`` picks the agent per
        session; omitted → the daemon's ``default_agent``."""
        params: dict[str, Any] = {"cwd": cwd, "mcpServers": []}
        if agent is not None:
            params["agent"] = agent
        r = await self._call("session/new", params)
        return r["sessionId"]

    async def list_sessions(
        self, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> list[dict[str, Any]]:
        """All sessions as ``[{sessionId, createdAt, agent, title}]``."""
        params: dict[str, Any] = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        r = await self._call("session/list", params)
        return r.get("sessions", [])

    async def session_messages(
        self, session_id: str, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> list[dict[str, Any]]:
        """Session history as ACP messages; page with ``limit``/``offset``."""
        params: dict[str, Any] = {"sessionId": session_id}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        r = await self._call("session/messages", params)
        return r.get("messages", [])

    async def resume_session(self, session_id: str) -> str:
        """Reattach after a daemon restart → ``sessionId``. Raises if unknown."""
        r = await self._call("session/resume", {"sessionId": session_id})
        return r["sessionId"]

    async def load_session(self, session_id: str) -> dict[str, Any]:
        """ACP ``session/load``: replay stored history as ``update`` events
        on :meth:`events` — start iterating before calling."""
        return await self._call("session/load", {"sessionId": session_id})

    async def delete_session(self, session_id: str) -> None:
        """Delete a session and its history."""
        await self._call("session/delete", {"sessionId": session_id})

    async def rename_session(self, session_id: str, title: str) -> None:
        """Set a session's display title."""
        await self._call("session/rename", {"sessionId": session_id, "title": title})

    async def search(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Full-text search over all history → ``[{sessionId, messageId, snippet}]``."""
        r = await self._call("session/search", {"query": query, "limit": limit})
        return r.get("results", [])

    async def usage(self, session_id: Optional[str] = None) -> dict[str, Any]:
        """One session's totals with ``session_id``, or the per-agent
        rollup (``{models: [...]}``) without it."""
        params: dict[str, Any] = {} if session_id is None else {"sessionId": session_id}
        return await self._call("session/usage", params)

    async def prompt(
        self,
        session_id: str,
        text: Union[str, list[dict[str, Any]]],
        model: Optional[str] = None,
    ) -> dict[str, Any]:
        """Send a prompt and resolve with the turn result
        (``{stopReason, agent}``) when the turn ends. ``text`` is a string
        or raw ACP content blocks (image/resource alongside text);
        ``model`` overrides the session default for this turn. The same
        outcome is also delivered as a ``prompt_done`` event for
        consumers that only iterate :meth:`events`."""
        blocks: list[dict[str, Any]] = (
            text if isinstance(text, list) else [{"type": "text", "text": text}]
        )
        params: dict[str, Any] = {"sessionId": session_id, "prompt": blocks}
        if model is not None:
            params["model"] = model
        return await self._call("session/prompt", params, prompt_session=session_id)

    async def cancel(self, session_id: str) -> None:
        """Cancel a live turn (notification — no response comes back)."""
        await self._ready()
        try:
            await self._send(
                json.dumps(
                    {"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": session_id}}
                )
            )
        except Exception:
            raise ConnectionError("connection closed") from None
