#!/usr/bin/env python3
"""Minimal MCP stdio server for tests. Tools: ping → echoes args; sleep → sleeps, then echoes args."""
import json
import sys
import time


def send(obj):
    try:
        sys.stdout.write(json.dumps(obj) + "\n")
        sys.stdout.flush()
    except BrokenPipeError:
        # Parent tore down the server mid-reply (test cleanup) — exit quietly.
        sys.exit(0)


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        msg = json.loads(line)
    except json.JSONDecodeError:
        continue
    method = msg.get("method")
    mid = msg.get("id")
    if method == "initialize":
        send({
            "jsonrpc": "2.0", "id": mid,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "test-mcp", "version": "0.0.1"},
            },
        })
    elif method == "ping":
        send({"jsonrpc": "2.0", "id": mid, "result": {}})
    elif method == "tools/list":
        send({
            "jsonrpc": "2.0", "id": mid,
            "result": {"tools": [
                {
                    "name": "ping",
                    "description": "echo arguments back",
                    "inputSchema": {"type": "object", "properties": {}},
                },
                {
                    "name": "sleep",
                    "description": "sleep, then echo arguments back",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"secs": {"type": "number"}},
                    },
                },
            ]},
        })
    elif method == "tools/call":
        params = msg.get("params", {})
        arguments = params.get("arguments", {})
        if params.get("name") == "sleep":
            time.sleep(arguments.get("secs", 5))
        send({
            "jsonrpc": "2.0", "id": mid,
            "result": {
                "content": [{"type": "text", "text": json.dumps(arguments)}],
                "isError": False,
            },
        })
    elif mid is not None:
        send({"jsonrpc": "2.0", "id": mid,
              "error": {"code": -32601, "message": "method not found"}})
