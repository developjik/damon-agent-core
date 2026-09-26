"""damon-agent — asyncio client for the damond WS protocol v2 agent API."""

from .client import DamonClient, RpcError

__all__ = ["DamonClient", "RpcError"]
__version__ = "0.5.0"
