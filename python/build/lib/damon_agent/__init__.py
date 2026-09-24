"""damon-agent — asyncio client for the damon WS/JSON-RPC agent API."""

from .client import DamonClient, RpcError

__all__ = ["DamonClient", "RpcError"]
__version__ = "0.2.0"
