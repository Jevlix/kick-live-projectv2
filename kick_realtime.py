"""Kick's current anonymous realtime transport (Pusher or Centrifugo).

Kick currently negotiates realtime endpoints through web.kick.com.  Do not
hard-code its Pusher app key/cluster: the negotiated descriptor is authoritative.
The auth token is deliberately requested only after the WebSocket is open, and
is never printed or persisted.
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections import deque
from urllib.parse import urlsplit

import websockets

try:
    from curl_cffi import requests as http_requests
except Exception:  # pragma: no cover - curl_cffi is part of requirements.txt
    import requests as http_requests  # type: ignore[no-redef]

REALTIME_API = "https://web.kick.com/api/v1/realtime"
KICK_ORIGIN = "https://kick.com"


class KickRealtimeError(ConnectionError):
    """A safe-to-log Kick realtime error. Never includes credentials/tokens."""


def _new_session():
    try:
        session = http_requests.Session(impersonate="chrome")
    except TypeError:
        session = http_requests.Session()
    session.headers.update({"Origin": KICK_ORIGIN, "x-app-platform": "web"})
    return session


def parse_json_frames(raw: str | bytes) -> list[dict]:
    """Centrifugo can coalesce several newline-delimited JSON frames."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    frames = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(item, dict):
            frames.append(item)
    return frames


def unwrap_publication(frame: dict) -> dict | None:
    """Convert a Centrifugo publication to the recorder's legacy event shape."""
    push = frame.get("push")
    if not isinstance(push, dict):
        return None
    if push.get("disconnect") or push.get("unsubscribe"):
        detail = push.get("disconnect") or push.get("unsubscribe") or {}
        if isinstance(detail, dict):
            code = detail.get("code", "?")
        else:
            code = "?"
        raise KickRealtimeError(f"Kick realtime feed closed (code={code}).")
    pub = push.get("pub")
    if not isinstance(pub, dict):
        return None
    payload = pub.get("data")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return None
    if not isinstance(payload, dict) or not payload.get("event"):
        return None
    return {"event": payload.get("event"), "data": payload.get("data")}


class KickRealtimeConnection:
    """One channel-scoped Kick connection, with protocol negotiation and renewal."""

    def __init__(self, channel_id: str, chatroom_id: str):
        self.channel_id = str(channel_id)
        self.chatroom_id = str(chatroom_id)
        self.client_id = str(uuid.uuid4())
        self.session = _new_session()
        self.ws = None
        self.provider = ""
        self.url = ""
        self._pending_events: deque[dict] = deque()
        self._next_id = 0
        self._pending_commands: dict[int, str] = {}
        self._pending_deadlines: dict[int, float] = {}
        self._pong_required = False
        self._heartbeat_seconds = 180.0
        self._last_activity = time.monotonic()
        self._refresh_at = float("inf")
        self._closed = False

    def _post_json(self, path: str, body: dict) -> dict:
        """Synchronous HTTP call, always run in a worker thread by callers."""
        try:
            response = self.session.post(
                REALTIME_API + path,
                json=body,
                timeout=(10, 25),
                allow_redirects=False,
            )
            status = response.status_code
            if status < 200 or status >= 300:
                # Do not log response body; token/auth endpoints may be sensitive.
                raise KickRealtimeError(f"Kick realtime HTTP {status} at {path}.")
            result = response.json()
        except KickRealtimeError:
            raise
        except Exception as exc:
            raise KickRealtimeError(
                f"Kick realtime request failed at {path} ({type(exc).__name__})."
            ) from exc
        if not isinstance(result, dict):
            raise KickRealtimeError(f"Kick realtime returned invalid JSON at {path}.")
        return result

    def _negotiate_sync(self) -> tuple[str, str]:
        payload = self._post_json(
            f"/channels/{self.channel_id}/chat/connection",
            {
                "client": {"id": self.client_id, "type": "web"},
                "capabilities": {
                    "accepted_providers": [
                        {"provider": "centrifugo"},
                        {"provider": "pusher"},
                    ]
                },
            },
        )
        data = payload.get("data") or {}
        if not isinstance(data, dict) or data.get("mode") != "websocket":
            raise KickRealtimeError("Kick did not provide a WebSocket realtime mode.")
        connections = data.get("connections") or []
        if not isinstance(connections, list):
            raise KickRealtimeError("Kick realtime connection list was invalid.")

        # Prefer the negotiated order. The returned endpoint/key is authoritative.
        for item in connections:
            if not isinstance(item, dict):
                continue
            provider = item.get("provider")
            credentials = item.get("credentials") or {}
            if not isinstance(credentials, dict):
                continue
            if provider == "centrifugo":
                url = credentials.get("url")
                if not isinstance(url, str) or len(url) > 1024:
                    continue
                parsed = urlsplit(url)
                host = parsed.hostname or ""
                if (
                    parsed.scheme == "wss"
                    and host.startswith("realtime.")
                    and host.endswith(".platform.kick.com")
                    and parsed.netloc == host
                    and parsed.path == "/connection/websocket"
                    and not parsed.query
                    and not parsed.fragment
                ):
                    return "centrifugo", url
            elif provider == "pusher":
                key = credentials.get("app_key")
                cluster = credentials.get("cluster")
                if (
                    isinstance(key, str) and key.isascii() and key.isalnum()
                    and isinstance(cluster, str) and cluster.isascii() and cluster.isalnum()
                ):
                    return "pusher", (
                        f"wss://ws-{cluster}.pusher.com/app/{key}"
                        "?protocol=7&client=js&version=8.4.0&flash=false"
                    )
        raise KickRealtimeError("Kick returned no supported realtime provider.")

    def _token_sync(self) -> str:
        payload = self._post_json("/auth/connection", {"client_id": self.client_id})
        data = payload.get("data") or {}
        token = data.get("token") if isinstance(data, dict) else None
        if not isinstance(token, str) or not token or len(token) > 8192:
            raise KickRealtimeError("Kick did not return a valid anonymous connection token.")
        return token

    async def _send(self, payload: dict) -> None:
        if self.ws is None:
            raise KickRealtimeError("Kick WebSocket is not connected.")
        await self.ws.send(json.dumps(payload, separators=(",", ":")))

    async def _recv_frames(self, timeout: float = 10.0) -> list[dict]:
        if self.ws is None:
            raise KickRealtimeError("Kick WebSocket is not connected.")
        try:
            raw = await asyncio.wait_for(self.ws.recv(), timeout=timeout)
        except asyncio.TimeoutError:
            return []
        self._last_activity = time.monotonic()
        return parse_json_frames(raw)

    async def _send_command(self, kind: str, data: dict) -> int:
        self._next_id += 1
        command_id = self._next_id
        self._pending_commands[command_id] = kind
        self._pending_deadlines[command_id] = time.monotonic() + 15.0
        await self._send({"id": command_id, kind: data})
        return command_id

    def _apply_ack(self, kind: str, result: dict) -> None:
        if kind in ("connect", "refresh"):
            if result.get("expires"):
                try:
                    ttl = int(result.get("ttl") or 0)
                except (TypeError, ValueError):
                    ttl = 0
                if ttl < 1:
                    raise KickRealtimeError("Kick realtime token lease was invalid.")
                self._refresh_at = time.monotonic() + max(ttl * 0.8, 1.0)
            else:
                self._refresh_at = float("inf")
            if kind == "connect":
                self._pong_required = bool(result.get("pong"))
                try:
                    ping = int(result.get("ping") or 0)
                except (TypeError, ValueError):
                    ping = 0
                self._heartbeat_seconds = ping + 10.0 if ping > 0 else 180.0

    def _extract_publication(self, frame: dict) -> None:
        event = unwrap_publication(frame)
        if event is not None:
            self._pending_events.append(event)

    async def _process_unmatched_frame(self, frame: dict) -> None:
        if not frame:
            if self.provider == "centrifugo" and self._pong_required:
                await self._send({})
            return
        if self.provider == "pusher":
            event_name = frame.get("event", "")
            if event_name == "pusher:ping":
                await self._send({"event": "pusher:pong", "data": {}})
            elif event_name in ("pusher:error",):
                data = frame.get("data")
                raise KickRealtimeError(f"Kick Pusher rejected realtime: {data}")
            elif event_name not in (
                "pusher:pong", "pusher:connection_established",
                "pusher_internal:subscription_succeeded", "pusher:subscription_succeeded",
            ):
                self._pending_events.append(frame)
            return
        self._extract_publication(frame)

    async def _wait_command_ack(self, command_id: int, kind: str, timeout: float = 15.0) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            frames = await self._recv_frames(min(3.0, max(0.1, deadline - time.monotonic())))
            if not frames:
                continue
            for frame in frames:
                if frame.get("id") == command_id:
                    self._pending_commands.pop(command_id, None)
                    self._pending_deadlines.pop(command_id, None)
                    error = frame.get("error")
                    if isinstance(error, dict) and error:
                        code = error.get("code", "?")
                        message = str(error.get("message") or "command rejected")[:180]
                        raise KickRealtimeError(
                            f"Kick realtime {kind} rejected (code={code}, {message})."
                        )
                    result = frame.get(kind)
                    if not isinstance(result, dict):
                        raise KickRealtimeError(f"Kick realtime {kind} acknowledgement was invalid.")
                    self._apply_ack(kind, result)
                    return result
                # Keep any publications received while waiting for the ACK.
                if frame.get("id") is not None:
                    unknown_id = frame.get("id")
                    other_kind = self._pending_commands.pop(unknown_id, None)
                    if other_kind:
                        other_error = frame.get("error")
                        if isinstance(other_error, dict) and other_error:
                            raise KickRealtimeError(
                                f"Kick realtime {other_kind} rejected (code={other_error.get('code', '?')})."
                            )
                        other_result = frame.get(other_kind)
                        if isinstance(other_result, dict):
                            self._apply_ack(other_kind, other_result)
                else:
                    await self._process_unmatched_frame(frame)
        self._pending_commands.pop(command_id, None)
        self._pending_deadlines.pop(command_id, None)
        raise KickRealtimeError(f"Timed out waiting for Kick realtime {kind} acknowledgement.")

    async def connect(self) -> None:
        """Negotiate, open the socket, then fetch and send the auth token."""
        self.provider, self.url = await asyncio.to_thread(self._negotiate_sync)
        try:
            self.ws = await websockets.connect(
                self.url,
                origin=KICK_ORIGIN,
                open_timeout=15,
                close_timeout=5,
                # Kick's Centrifugo app-level heartbeat is handled below.
                ping_interval=None,
                max_size=4 * 1024 * 1024,
            )
        except Exception as exc:
            raise KickRealtimeError(
                f"Could not open negotiated Kick WebSocket ({type(exc).__name__})."
            ) from exc

        if self.provider == "centrifugo":
            # This order is important for Kick's current anonymous realtime flow:
            # open the negotiated WS first, then request the client-bound token.
            token = await asyncio.to_thread(self._token_sync)
            command_id = await self._send_command(
                "connect", {"token": token, "name": "chat-downloader", "version": "1"}
            )
            await self._wait_command_ack(command_id, "connect")
            command_id = await self._send_command(
                "subscribe", {"channel": f"chatrooms.{self.chatroom_id}.v2"}
            )
            await self._wait_command_ack(command_id, "subscribe")
        elif self.provider == "pusher":
            # Pusher sends connection_established before subscription is valid.
            deadline = time.monotonic() + 15
            established = False
            while time.monotonic() < deadline and not established:
                frames = await self._recv_frames(3)
                for frame in frames:
                    if frame.get("event") == "pusher:connection_established":
                        established = True
                    else:
                        await self._process_unmatched_frame(frame)
            if not established:
                raise KickRealtimeError("Timed out waiting for Pusher connection_established.")
            await self._send({
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"chatrooms.{self.chatroom_id}.v2"},
            })
            deadline = time.monotonic() + 15
            subscribed = False
            while time.monotonic() < deadline and not subscribed:
                frames = await self._recv_frames(3)
                for frame in frames:
                    if frame.get("event") in (
                        "pusher_internal:subscription_succeeded", "pusher:subscription_succeeded"
                    ):
                        subscribed = True
                    else:
                        await self._process_unmatched_frame(frame)
            if not subscribed:
                raise KickRealtimeError("Timed out waiting for Pusher subscription confirmation.")

    async def recv_event(self) -> dict | None:
        """Receive the next published event; None means a harmless timeout/control."""
        if self._pending_events:
            return self._pending_events.popleft()
        while True:
            if self.ws is None:
                raise KickRealtimeError("Kick WebSocket is not connected.")

            now = time.monotonic()
            if any(now >= deadline for deadline in self._pending_deadlines.values()):
                raise KickRealtimeError("Timed out waiting for a Kick realtime command acknowledgement.")

            if self.provider == "centrifugo" and now >= self._refresh_at:
                token = await asyncio.to_thread(self._token_sync)
                command_id = await self._send_command("refresh", {"token": token})
                # Avoid requesting tokens in a tight loop before the ACK arrives.
                # Acknowledgement is consumed in the loop below and renews the lease.
                self._refresh_at = float("inf")
                if command_id <= 0:  # defensive; command IDs are always positive
                    raise KickRealtimeError("Invalid refresh command ID.")

            if time.monotonic() - self._last_activity > self._heartbeat_seconds + 30:
                raise KickRealtimeError("Kick realtime heartbeat expired; reconnecting.")

            try:
                frames = await self._recv_frames(5)
            except Exception as exc:
                raise KickRealtimeError(
                    f"Kick WebSocket receive failed ({type(exc).__name__})."
                ) from exc
            if not frames:
                continue

            for frame in frames:
                if frame.get("id") is not None:
                    command_id = frame.get("id")
                    kind = self._pending_commands.pop(command_id, None)
                    self._pending_deadlines.pop(command_id, None)
                    if not kind:
                        continue
                    error = frame.get("error")
                    if isinstance(error, dict) and error:
                        raise KickRealtimeError(
                            f"Kick realtime {kind} rejected (code={error.get('code', '?')})."
                        )
                    result = frame.get(kind)
                    if not isinstance(result, dict):
                        raise KickRealtimeError(f"Kick realtime {kind} acknowledgement was invalid.")
                    self._apply_ack(kind, result)
                    continue

                await self._process_unmatched_frame(frame)
                if self._pending_events:
                    return self._pending_events.popleft()

    async def close(self) -> None:
        self._closed = True
        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None
        try:
            await asyncio.to_thread(self.session.close)
        except Exception:
            pass
