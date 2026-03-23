import asyncio
import json

from delta_exchange_bot.api.websocket_manager import WebSocketManager


class _CloseSignal(Exception):
    pass


class FakeWebSocket:
    def __init__(self, *, recv_items=None, ping_latencies=None):
        self.sent: list[dict] = []
        self._recv_queue: asyncio.Queue[object] = asyncio.Queue()
        for item in recv_items or []:
            self._recv_queue.put_nowait(item)
        self._ping_latencies = list(ping_latencies or [0.001])
        self.closed = False

    async def send(self, payload: str) -> None:
        self.sent.append(json.loads(payload))

    async def recv(self):
        item = await self._recv_queue.get()
        if isinstance(item, Exception):
            raise item
        return item

    async def ping(self):
        loop = asyncio.get_running_loop()
        waiter = loop.create_future()
        latency = self._ping_latencies.pop(0) if self._ping_latencies else 0.001
        if latency is None:
            return waiter

        async def _resolve() -> None:
            await asyncio.sleep(0)
            waiter.set_result(float(latency))

        asyncio.create_task(_resolve())
        return waiter

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._recv_queue.put_nowait(_CloseSignal("closed"))


class FakeConnectFactory:
    def __init__(self, sockets):
        self._sockets = list(sockets)
        self.calls = 0

    def __call__(self, *args, **kwargs):
        ws = self._sockets[self.calls]
        self.calls += 1
        return _FakeConnectionContext(ws)


class _FakeConnectionContext:
    def __init__(self, websocket):
        self.websocket = websocket

    async def __aenter__(self):
        return self.websocket

    async def __aexit__(self, exc_type, exc, tb):
        await self.websocket.close()
        return False


async def _wait_for(predicate, timeout=1.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met before timeout")


def test_manager_authenticates_before_subscribing(monkeypatch):
    async def _run():
        websocket = FakeWebSocket(recv_items=[json.dumps({"type": "auth", "success": True})])
        connect_factory = FakeConnectFactory([websocket])
        monkeypatch.setattr("delta_exchange_bot.api.websocket_manager.ws_connect", connect_factory)

        manager = WebSocketManager(
            ws_url="wss://example.test/ws",
            api_key="key",
            api_secret="secret",
            ping_interval_s=60,
            stale_after_s=120,
        )
        manager.add_subscription("v2/ticker", ["BTCUSD"])

        await manager.connect()
        await _wait_for(lambda: len(websocket.sent) >= 2)
        await manager.disconnect()

        assert websocket.sent[0]["type"] == "auth"
        assert websocket.sent[1]["type"] == "subscribe"
        assert websocket.sent[1]["payload"]["channels"][0]["symbols"] == ["BTCUSD"]

    asyncio.run(_run())


def test_manager_reconnects_and_resubscribes_without_duplicates(monkeypatch):
    async def _run():
        first = FakeWebSocket(recv_items=[RuntimeError("Connection to remote host was lost")])
        second = FakeWebSocket(recv_items=[json.dumps({"type": "heartbeat"})])
        connect_factory = FakeConnectFactory([first, second])
        monkeypatch.setattr("delta_exchange_bot.api.websocket_manager.ws_connect", connect_factory)

        disconnect_reasons: list[str] = []
        manager = WebSocketManager(
            ws_url="wss://example.test/ws",
            ping_interval_s=60,
            stale_after_s=120,
            reconnect_backoff_s=[0.01],
            on_disconnect=lambda reason="": disconnect_reasons.append(reason),
        )
        manager.add_subscription("v2/ticker", ["BTCUSD"])
        manager.add_subscription("v2/ticker", ["BTCUSD", "ETHUSD"])

        await manager.connect()
        await _wait_for(lambda: connect_factory.calls >= 2)
        await _wait_for(lambda: len(second.sent) >= 1)
        await manager.disconnect()

        first_symbols = first.sent[0]["payload"]["channels"][0]["symbols"]
        second_symbols = second.sent[0]["payload"]["channels"][0]["symbols"]
        assert first_symbols == ["BTCUSD", "ETHUSD"]
        assert second_symbols == ["BTCUSD", "ETHUSD"]
        assert disconnect_reasons

    asyncio.run(_run())


def test_manager_reconnects_on_stale_connection(monkeypatch):
    async def _run():
        first = FakeWebSocket()
        second = FakeWebSocket(recv_items=[json.dumps({"type": "heartbeat"})])
        connect_factory = FakeConnectFactory([first, second])
        monkeypatch.setattr("delta_exchange_bot.api.websocket_manager.ws_connect", connect_factory)

        manager = WebSocketManager(
            ws_url="wss://example.test/ws",
            ping_interval_s=60,
            stale_after_s=1,
            reconnect_backoff_s=[0.01],
        )

        await manager.connect()
        await _wait_for(lambda: connect_factory.calls >= 2, timeout=2.0)
        await manager.disconnect()

        assert manager.health_snapshot()["total_reconnects"] >= 1

    asyncio.run(_run())
