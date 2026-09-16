#!/usr/bin/env python3
"""Minimal MCP stdio server for tests. One tool: echo.ping → echoes args."""
import json
import sys


def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


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
    elif method == "tools/list":
        send({
            "jsonrpc": "2.0", "id": mid,
            "result": {"tools": [{
                "name": "ping",
                "description": "echo arguments back",
                "inputSchema": {"type": "object", "properties": {}},
            }]},
        })
    elif method == "tools/call":
        args = msg.get("params", {}).get("arguments", {})
        send({
            "jsonrpc": "2.0", "id": mid,
            "result": {
                "content": [{"type": "text", "text": json.dumps(args)}],
                "isError": False,
            },
        })
    elif mid is not None:
        send({"jsonrpc": "2.0", "id": mid,
              "error": {"code": -32601, "message": "method not found"}})
