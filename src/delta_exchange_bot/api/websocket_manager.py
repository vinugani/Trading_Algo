from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import inspect
import json
import logging
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from websockets.asyncio.client import ClientConnection, connect as ws_connect
from websockets.exceptions import ConnectionClosed, WebSocketException

logger = logging.getLogger(__name__)

DEFAULT_RECONNECT_BACKOFF_S = (1, 2, 5, 10, 30)


class WebSocketManagerError(RuntimeError):
    """Base class for manager-level failures."""


class AuthenticationError(WebSocketManagerError):
    """Raised when the exchange rejects WebSocket authentication."""


class HeartbeatTimeoutError(WebSocketManagerError):
    """Raised when a pong isn't received within the expected timeout."""


class StaleConnectionError(WebSocketManagerError):
    """Raised when the connection stops delivering messages for too long."""


class WebSocketManager:
    """Async WebSocket manager with reconnect, heartbeat, and resubscription."""

    def __init__(
        self,
        ws_url: str,
        api_key: str = "",
        api_secret: str = "",
        on_message: Callable[[dict[str, Any]], Any] | None = None,
        on_message_callback: Callable[[dict[str, Any]], Any] | None = None,
        on_connect: Callable[..., Any] | None = None,
        on_disconnect: Callable[..., Any] | None = None,
        on_alert: Callable[..., Any] | None = None,
        subscriptions: Iterable[dict[str, Any]] | None = None,
        *,
        ping_interval_s: float = 20.0,
        ping_timeout_s: float = 10.0,
        stale_after_s: float = 45.0,
        reconnect_backoff_s: Iterable[float] | None = None,
        connect_timeout_s: float = 10.0,
        close_timeout_s: float = 5.0,
        max_queue: int = 1024,
        enable_outbound_queue: bool = False,
    ):
        self.ws_url = ws_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.on_message = on_message or on_message_callback
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.on_alert = on_alert
        self.ping_interval_s = max(0.1, float(ping_interval_s))
        self.ping_timeout_s = max(0.1, float(ping_timeout_s))
        self.stale_after_s = max(0.1, float(stale_after_s))
        self.reconnect_backoff_s = tuple(float(v) for v in (reconnect_backoff_s or DEFAULT_RECONNECT_BACKOFF_S))
        self.connect_timeout_s = max(1.0, float(connect_timeout_s))
        self.close_timeout_s = max(1.0, float(close_timeout_s))
        self.max_queue = max(1, int(max_queue))
        self.enable_outbound_queue = enable_outbound_queue

        self._channel_subscriptions: dict[str, set[str]] = defaultdict(set)
        self._raw_subscriptions: dict[str, dict[str, Any]] = {}
        self._supervisor_task: asyncio.Task[None] | None = None
        self._ws: ClientConnection | None = None
        self._stop_event = asyncio.Event()
        self._connected_event = asyncio.Event()
        self._lifecycle_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._auth_future: asyncio.Future[None] | None = None
        self._last_message_monotonic = 0.0
        self._last_pong_monotonic = 0.0
        self._connected_at_monotonic: float | None = None
        self._last_disconnect_reason: str | None = None
        self._reconnect_attempt = 0
        self._total_reconnects = 0
        self._fallback_active = False
        self._latency_s: float | None = None
        self._outbound_queue: asyncio.Queue[str] | None = asyncio.Queue() if enable_outbound_queue else None

        for payload in subscriptions or []:
            self.add_raw_subscription(payload)

    @property
    def is_connected(self) -> bool:
        return self._connected_event.is_set() and self._ws is not None

    @property
    def is_degraded(self) -> bool:
        return self._fallback_active or not self.is_healthy

    @property
    def is_healthy(self) -> bool:
        if not self.is_connected:
            return False
        if self._last_message_monotonic <= 0:
            return False
        return (time.monotonic() - self._last_message_monotonic) <= self.stale_after_s

    @property
    def last_disconnect_reason(self) -> str | None:
        return self._last_disconnect_reason

    async def connect(self) -> None:
        async with self._lifecycle_lock:
            if self._supervisor_task and not self._supervisor_task.done():
                return
            self._stop_event = asyncio.Event()
            self._connected_event.clear()
            self._supervisor_task = asyncio.create_task(self._supervise_connection(), name="delta-websocket-manager")

    async def disconnect(self) -> None:
        async with self._lifecycle_lock:
            self._stop_event.set()
            ws = self._ws
            supervisor = self._supervisor_task
            self._supervisor_task = None

        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()

        if supervisor is not None:
            supervisor.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await supervisor

        await self._mark_disconnected("client_shutdown", unexpected=False)

    async def start(self) -> None:
        await self.connect()

    async def stop(self) -> None:
        await self.disconnect()

    async def wait_until_connected(self, timeout: float | None = None) -> bool:
        try:
            if timeout is None:
                await self._connected_event.wait()
            else:
                await asyncio.wait_for(self._connected_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def add_subscription(self, channel: str, symbols: list[str]) -> None:
        normalized_symbols = sorted({str(symbol) for symbol in symbols if symbol})
        if not channel or not normalized_symbols:
            return

        new_symbols: list[str] = []
        existing = self._channel_subscriptions[channel]
        for symbol in normalized_symbols:
            if symbol not in existing:
                existing.add(symbol)
                new_symbols.append(symbol)

        if new_symbols and self.is_connected:
            payload = self._build_channel_payload(channel, new_symbols)
            self._schedule_background(self._send_subscription_payload(payload))

    def add_raw_subscription(self, payload: dict[str, Any]) -> None:
        payload_key = self._payload_key(payload)
        if payload_key in self._raw_subscriptions:
            return

        self._raw_subscriptions[payload_key] = payload
        if self.is_connected:
            self._schedule_background(self._send_subscription_payload(payload))

    async def send_json(self, payload: dict[str, Any], *, queue_if_disconnected: bool | None = None) -> None:
        websocket = self._ws
        if websocket is None:
            should_queue = self.enable_outbound_queue if queue_if_disconnected is None else queue_if_disconnected
            if should_queue and self._outbound_queue is not None:
                await self._outbound_queue.put(json.dumps(payload))
                logger.warning("WebSocket offline; queued outbound message")
                return
            raise WebSocketManagerError("WebSocket is not connected")

        await self._send_payload(payload, websocket=websocket)

    def health_snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        last_message_age_s = None
        if self._last_message_monotonic > 0:
            last_message_age_s = round(now - self._last_message_monotonic, 3)

        uptime_s = None
        if self._connected_at_monotonic is not None and self.is_connected:
            uptime_s = round(now - self._connected_at_monotonic, 3)

        return {
            "connected": self.is_connected,
            "healthy": self.is_healthy,
            "degraded": self.is_degraded,
            "fallback_active": self._fallback_active,
            "reconnect_attempt": self._reconnect_attempt,
            "total_reconnects": self._total_reconnects,
            "last_disconnect_reason": self._last_disconnect_reason,
            "last_message_age_s": last_message_age_s,
            "latency_s": self._latency_s,
            "uptime_s": uptime_s,
            "subscriptions": self._build_subscription_payloads(),
        }

    def _build_auth_payload(self) -> dict[str, Any]:
        timestamp = str(int(time.time()))
        signature_data = f"GET{timestamp}/v2/websocket"
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            signature_data.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {
            "type": "auth",
            "payload": {
                "api-key": self.api_key,
                "timestamp": timestamp,
                "signature": signature,
            },
        }

    def _build_channel_payload(self, channel: str, symbols: list[str]) -> dict[str, Any]:
        return {"channels": [{"name": channel, "symbols": sorted({str(symbol) for symbol in symbols})}]}

    def _build_subscription_payloads(self) -> list[dict[str, Any]]:
        payloads = [
            self._build_channel_payload(channel, sorted(symbols))
            for channel, symbols in sorted(self._channel_subscriptions.items())
            if symbols
        ]
        payloads.extend(self._raw_subscriptions.values())
        return payloads

    async def _supervise_connection(self) -> None:
        while not self._stop_event.is_set():
            reason = "connection closed"
            try:
                reason = await self._run_single_connection()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reason = self._format_exception(exc)

            unexpected = not self._stop_event.is_set()
            await self._mark_disconnected(reason, unexpected=unexpected)
            if not unexpected:
                return

            self._reconnect_attempt += 1
            self._total_reconnects += 1
            delay = self.reconnect_backoff_s[min(self._reconnect_attempt - 1, len(self.reconnect_backoff_s) - 1)]
            logger.warning(
                "WebSocket disconnected; reconnect in %.1fs (attempt %s): %s",
                delay,
                self._reconnect_attempt,
                reason,
            )
            await self._emit_alert(
                "warning",
                "Market data WebSocket disconnected; REST fallback active",
                {"reason": reason, "reconnect_in_s": delay, "attempt": self._reconnect_attempt},
            )
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                continue

    async def _run_single_connection(self) -> str:
        async with ws_connect(
            self.ws_url,
            open_timeout=self.connect_timeout_s,
            close_timeout=self.close_timeout_s,
            ping_interval=None,
            ping_timeout=None,
            max_queue=self.max_queue,
        ) as websocket:
            await self._mark_connected(websocket)

            receiver_task = asyncio.create_task(self._receiver_loop(websocket), name="delta-ws-recv")
            heartbeat_task = asyncio.create_task(self._heartbeat_loop(websocket), name="delta-ws-heartbeat")
            stale_task = asyncio.create_task(self._stale_watchdog(), name="delta-ws-stale")

            try:
                await self._authenticate_and_resubscribe(websocket)
                await self._flush_outbound_queue(websocket)
                done, pending = await asyncio.wait(
                    {receiver_task, heartbeat_task, stale_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

                result_reasons: list[str] = []
                for task in done:
                    exc = task.exception()
                    if exc is not None:
                        raise exc
                    result = task.result()
                    if result:
                        result_reasons.append(str(result))

                return result_reasons[0] if result_reasons else "connection task completed"
            finally:
                for task in (receiver_task, heartbeat_task, stale_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(receiver_task, heartbeat_task, stale_task, return_exceptions=True)

    async def _authenticate_and_resubscribe(self, websocket: ClientConnection) -> None:
        if self.api_key and self.api_secret:
            loop = asyncio.get_running_loop()
            self._auth_future = loop.create_future()
            await self._send_payload(self._build_auth_payload(), websocket=websocket)
            logger.info("WebSocket authentication request sent")
            await asyncio.wait_for(self._auth_future, timeout=self.connect_timeout_s)
        else:
            self._auth_future = None

        payloads = self._build_subscription_payloads()
        if not payloads:
            logger.info("WebSocket connected with no subscriptions to restore")
            return

        for payload in payloads:
            await self._send_subscription_payload(payload, websocket=websocket)
        logger.info("WebSocket resubscribed to %s payload(s)", len(payloads))

    async def _receiver_loop(self, websocket: ClientConnection) -> str:
        try:
            while not self._stop_event.is_set():
                raw_message = await websocket.recv()
                self._last_message_monotonic = time.monotonic()
                message = self._decode_message(raw_message)
                if message is None:
                    continue
                await self._handle_incoming_message(message)
        except ConnectionClosed as exc:
            return f"connection closed: code={exc.code} reason={exc.reason or 'n/a'}"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise WebSocketManagerError(f"receiver loop failed: {self._format_exception(exc)}") from exc
        return "receiver loop stopped"

    async def _heartbeat_loop(self, websocket: ClientConnection) -> str:
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(self.ping_interval_s)
                pong_waiter = await websocket.ping()
                latency_s = await asyncio.wait_for(pong_waiter, timeout=self.ping_timeout_s)
                self._latency_s = float(latency_s)
                self._last_pong_monotonic = time.monotonic()
        except asyncio.TimeoutError as exc:
            raise HeartbeatTimeoutError(
                f"pong not received within {self.ping_timeout_s:.1f}s"
            ) from exc
        except ConnectionClosed as exc:
            return f"connection closed during heartbeat: code={exc.code} reason={exc.reason or 'n/a'}"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise WebSocketManagerError(f"heartbeat loop failed: {self._format_exception(exc)}") from exc
        return "heartbeat loop stopped"

    async def _stale_watchdog(self) -> str:
        try:
            sleep_s = min(1.0, max(0.1, self.stale_after_s / 2))
            while not self._stop_event.is_set():
                await asyncio.sleep(sleep_s)
                age_s = time.monotonic() - self._last_message_monotonic
                if age_s > self.stale_after_s:
                    raise StaleConnectionError(
                        f"no messages received for {age_s:.1f}s (threshold {self.stale_after_s:.1f}s)"
                    )
        except asyncio.CancelledError:
            raise

    async def _handle_incoming_message(self, message: dict[str, Any]) -> None:
        msg_type = str(message.get("type") or "").lower()
        if msg_type == "auth" and self._auth_future is not None and not self._auth_future.done():
            if bool(message.get("success")):
                self._auth_future.set_result(None)
                logger.info("WebSocket authenticated successfully")
            else:
                error_detail = message.get("error") or message.get("message") or "authentication failed"
                self._auth_future.set_exception(AuthenticationError(str(error_detail)))

        if msg_type in {"pong", "heartbeat"}:
            self._last_pong_monotonic = time.monotonic()

        if msg_type in {"subscribed", "subscriptions"}:
            logger.info("WebSocket subscription status: %s", message)

        await self._run_callback(self.on_message, message)

    async def _mark_connected(self, websocket: ClientConnection) -> None:
        self._ws = websocket
        self._connected_event.set()
        self._connected_at_monotonic = time.monotonic()
        self._last_message_monotonic = self._connected_at_monotonic
        self._last_pong_monotonic = self._connected_at_monotonic
        self._last_disconnect_reason = None
        self._reconnect_attempt = 0
        restored_from_fallback = self._fallback_active
        self._fallback_active = False

        logger.info("WebSocket connection established to %s", self.ws_url)
        if restored_from_fallback:
            await self._emit_alert("info", "Market data WebSocket restored", {"ws_url": self.ws_url})
        await self._run_callback(self.on_connect)

    async def _mark_disconnected(self, reason: str, *, unexpected: bool) -> None:
        was_connected = self._connected_event.is_set() or self._ws is not None or self._fallback_active
        self._ws = None
        self._connected_event.clear()
        self._connected_at_monotonic = None
        self._latency_s = None
        self._last_disconnect_reason = reason

        if self._auth_future is not None and not self._auth_future.done():
            self._auth_future.cancel()
        self._auth_future = None

        if unexpected:
            self._fallback_active = True
            logger.warning("WebSocket disconnected unexpectedly: %s", reason)
        else:
            logger.info("WebSocket disconnected: %s", reason)

        if was_connected or unexpected:
            await self._run_callback(self.on_disconnect, reason)

    async def _send_subscription_payload(
        self,
        payload: dict[str, Any],
        *,
        websocket: ClientConnection | None = None,
    ) -> None:
        if "type" in payload:
            message = payload
        else:
            message = {"type": "subscribe", "payload": payload}
        await self._send_payload(message, websocket=websocket)

    async def _send_payload(
        self,
        payload: dict[str, Any],
        *,
        websocket: ClientConnection | None = None,
    ) -> None:
        target = websocket or self._ws
        if target is None:
            raise WebSocketManagerError("WebSocket is not connected")

        async with self._send_lock:
            await target.send(json.dumps(payload))

    async def _flush_outbound_queue(self, websocket: ClientConnection) -> None:
        if self._outbound_queue is None:
            return

        flushed = 0
        while not self._outbound_queue.empty():
            encoded_message = self._outbound_queue.get_nowait()
            async with self._send_lock:
                await websocket.send(encoded_message)
            flushed += 1

        if flushed:
            logger.info("Flushed %s queued outbound WebSocket message(s)", flushed)

    def _decode_message(self, raw_message: str | bytes) -> dict[str, Any] | None:
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")

        try:
            decoded = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.warning("Discarding invalid WebSocket JSON: %s", raw_message)
            return None

        if not isinstance(decoded, dict):
            logger.warning("Discarding unsupported WebSocket payload: %s", decoded)
            return None
        return decoded

    async def _emit_alert(self, level: str, message: str, context: dict[str, Any] | None = None) -> None:
        log_fn = logger.warning if level.lower() == "warning" else logger.info
        if context:
            log_fn("%s | %s", message, context)
        else:
            log_fn("%s", message)
        await self._run_callback(self.on_alert, level, message, context or {})

    async def _run_callback(self, callback: Callable[..., Any] | None, *args: Any) -> None:
        if callback is None:
            return
        try:
            result = callback(*args)
        except TypeError as exc:
            if args and "positional argument" in str(exc):
                result = callback()
            else:
                logger.exception("WebSocket callback failed before awaiting")
                return
        except Exception:
            logger.exception("WebSocket callback failed before awaiting")
            return

        try:
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.exception("WebSocket callback failed")

    def _schedule_background(self, coro: Awaitable[Any]) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        task = loop.create_task(coro)
        task.add_done_callback(self._log_background_task_error)

    def _log_background_task_error(self, task: asyncio.Task[Any]) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            exc = task.exception()
            if exc is not None:
                logger.error(
                    "WebSocket background task failed",
                    exc_info=(type(exc), exc, exc.__traceback__),
                )

    @staticmethod
    def _payload_key(payload: dict[str, Any]) -> str:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _format_exception(exc: Exception) -> str:
        if isinstance(exc, ConnectionClosed):
            return f"connection closed: code={exc.code} reason={exc.reason or 'n/a'}"
        if isinstance(exc, WebSocketException):
            return f"{exc.__class__.__name__}: {exc}"
        return f"{exc.__class__.__name__}: {exc}"
