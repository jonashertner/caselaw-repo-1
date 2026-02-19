"""MCP Bridge — manages mcp_server.py as a stdio subprocess.

Sends JSON-RPC 2.0 messages over stdin/stdout to call MCP tools.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger("web_api.mcp_bridge")

MCP_SERVER_PATH = os.environ.get(
    "MCP_SERVER_PATH",
    str(Path(__file__).parent.parent / "mcp_server.py"),
)
PYTHON = os.environ.get("MCP_PYTHON", sys.executable)
TOOL_TIMEOUT = float(os.environ.get("MCP_TOOL_TIMEOUT", "120"))


class MCPBridge:
    """Manages a single mcp_server.py subprocess via JSON-RPC over stdio."""

    def __init__(self):
        self._proc: asyncio.subprocess.Process | None = None
        self._request_id = 0
        self._lock = asyncio.Lock()
        self._initialized = False

    async def start(self) -> None:
        if self._proc is not None and self._proc.returncode is None:
            return

        logger.info("Starting MCP subprocess: %s %s", PYTHON, MCP_SERVER_PATH)
        self._proc = await asyncio.create_subprocess_exec(
            PYTHON, MCP_SERVER_PATH,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._initialized = False

        # MCP handshake: send initialize, then initialized notification
        init_resp = await self._send_request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "web_api", "version": "1.0.0"},
        })
        logger.info("MCP initialized: %s", json.dumps(init_resp.get("result", {}))[:200])

        await self._send_notification("notifications/initialized", {})
        self._initialized = True

    async def stop(self) -> None:
        if self._proc and self._proc.returncode is None:
            self._proc.stdin.close()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                self._proc.kill()
            self._proc = None
        self._initialized = False

    async def call_tool(self, name: str, arguments: dict) -> str:
        """Call an MCP tool and return the text result."""
        if not self._initialized:
            await self.start()

        resp = await self._send_request("tools/call", {
            "name": name,
            "arguments": arguments,
        })

        if "error" in resp:
            raise RuntimeError(f"MCP error: {resp['error']}")

        result = resp.get("result", {})
        content = result.get("content", [])
        texts = [c.get("text", "") for c in content if c.get("type") == "text"]
        return "\n".join(texts)

    async def _send_request(self, method: str, params: dict) -> dict:
        async with self._lock:
            self._request_id += 1
            msg = {
                "jsonrpc": "2.0",
                "id": self._request_id,
                "method": method,
                "params": params,
            }
            return await self._send_and_receive(msg)

    async def _send_notification(self, method: str, params: dict) -> None:
        async with self._lock:
            msg = {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            }
            await self._write(msg)

    async def _send_and_receive(self, msg: dict) -> dict:
        await self._write(msg)
        target_id = msg.get("id")

        while True:
            resp = await self._read()
            if resp is None:
                stderr_out = ""
                if self._proc and self._proc.stderr:
                    try:
                        stderr_out = (await asyncio.wait_for(
                            self._proc.stderr.read(4096), timeout=1
                        )).decode(errors="replace")
                    except (asyncio.TimeoutError, Exception):
                        pass
                raise RuntimeError(f"MCP subprocess closed. stderr: {stderr_out}")

            # Skip notifications (no id)
            if "id" not in resp:
                continue
            if resp.get("id") == target_id:
                return resp

    async def _write(self, msg: dict) -> None:
        """Write a JSON-RPC message as a single newline-delimited JSON line."""
        if not self._proc or self._proc.stdin is None:
            raise RuntimeError("MCP subprocess not running")
        line = json.dumps(msg, separators=(",", ":")) + "\n"
        self._proc.stdin.write(line.encode())
        await self._proc.stdin.drain()

    async def _read(self) -> dict | None:
        """Read one newline-delimited JSON line from the MCP subprocess."""
        if not self._proc or self._proc.stdout is None:
            return None

        try:
            line = await asyncio.wait_for(
                self._proc.stdout.readline(), timeout=TOOL_TIMEOUT,
            )
            if not line:
                return None
            return json.loads(line.decode())

        except asyncio.TimeoutError:
            logger.error("MCP read timeout after %ss — killing subprocess", TOOL_TIMEOUT)
            await self._kill_proc()
            return None
        except Exception as e:
            logger.error("MCP read error: %s", e)
            return None

    async def _kill_proc(self) -> None:
        """Kill the MCP subprocess so the singleton getter restarts it."""
        if self._proc and self._proc.returncode is None:
            self._proc.kill()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=3)
            except asyncio.TimeoutError:
                pass
        self._proc = None
        self._initialized = False

    @property
    def is_running(self) -> bool:
        return self._proc is not None and self._proc.returncode is None


# Module-level singleton
_bridge: MCPBridge | None = None


async def get_bridge() -> MCPBridge:
    global _bridge
    if _bridge is None:
        _bridge = MCPBridge()
    if not _bridge.is_running:
        await _bridge.start()
    return _bridge
