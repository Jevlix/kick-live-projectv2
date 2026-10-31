import asyncio
import json

import kick_realtime
from kick_realtime import KickRealtimeConnection, parse_json_frames, unwrap_publication


def test_parse_json_frames_supports_newline_delimited_frames():
    frames = parse_json_frames('{"id":1}\n{}\nnot-json\n{"push":{}}')
    assert frames == [{"id": 1}, {}, {"push": {}}]


def test_unwrap_publication_decodes_json_data_string():
    frame = {
        "push": {
            "channel": "chatrooms.25951243.v2",
            "pub": {
                "data": json.dumps({
                    "event": "App\\Events\\ChatMessageEvent",
                    "data": json.dumps({"content": "hello", "sender": {"username": "Test"}}),
                })
            },
        }
    }
    result = unwrap_publication(frame)
    assert result == {
        "event": "App\\Events\\ChatMessageEvent",
        "data": json.dumps({"content": "hello", "sender": {"username": "Test"}}),
    }


class _FakeResponse:
    status_code = 200

    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


class _FakeSession:
    def __init__(self, order):
        self.order = order
        self.headers = {}

    def post(self, url, json=None, **kwargs):
        if url.endswith("/channels/26239555/chat/connection"):
            self.order.append("negotiate")
            assert json["client"]["id"]
            return _FakeResponse({
                "data": {
                    "mode": "websocket",
                    "connections": [{
                        "provider": "centrifugo",
                        "credentials": {
                            "url": "wss://realtime.us-east-1.platform.kick.com/connection/websocket"
                        },
                    }],
                }
            })
        if url.endswith("/auth/connection"):
            self.order.append("token")
            return _FakeResponse({"data": {"token": "test-token"}})
        raise AssertionError(url)

    def close(self):
        self.order.append("http-close")


class _FakeWebSocket:
    def __init__(self, order):
        self.order = order
        self.frames = []

    async def send(self, raw):
        data = json.loads(raw)
        if "connect" in data:
            self.order.append("connect-command")
            self.frames.append(json.dumps({
                "id": data["id"], "connect": {"expires": False, "pong": False, "ping": 0}
            }))
        elif "subscribe" in data:
            self.order.append("subscribe-command")
            self.frames.append(json.dumps({"id": data["id"], "subscribe": {}}))
            self.frames.append(json.dumps({"push": {"pub": {"data": {
                "event": "App\\Events\\ChatMessageEvent",
                "data": {"content": "hello"},
            }}}}))

    async def recv(self):
        if not self.frames:
            await asyncio.sleep(0.01)
        if not self.frames:
            raise asyncio.TimeoutError()
        return self.frames.pop(0)

    async def close(self):
        self.order.append("ws-close")


def test_centifugo_handshake_opens_socket_before_fetching_token(monkeypatch):
    order = []
    realtime = KickRealtimeConnection("26239555", "25951243")
    realtime.session.close()
    realtime.session = _FakeSession(order)
    fake_ws = _FakeWebSocket(order)

    async def fake_connect(url, **kwargs):
        order.append("ws-open")
        assert url == "wss://realtime.us-east-1.platform.kick.com/connection/websocket"
        assert kwargs["origin"] == "https://kick.com"
        return fake_ws

    monkeypatch.setattr(kick_realtime.websockets, "connect", fake_connect)

    async def run_test():
        await realtime.connect()
        event = await realtime.recv_event()
        assert event["event"] == "App\\Events\\ChatMessageEvent"
        await realtime.close()

    asyncio.run(run_test())
    assert order.index("ws-open") < order.index("token")
    assert "connect-command" in order
    assert "subscribe-command" in order
