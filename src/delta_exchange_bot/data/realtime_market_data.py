from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Optional

from websocket import WebSocketApp

from delta_exchange_bot.data.market_data import fetch_ticker

logger = logging.getLogger(__name__)


class RealtimeMarketDataService:
    """Robust WebSocket market data manager with REST polling fallback."""

    def __init__(
        self,
        ws_url: str,
        api_url: str,
        symbols: list[str],
        *,
        reconnect_interval_s: int = 5,
        fallback_poll_interval_s: int = 2,
        max_ws_failures_before_backoff: int = 3,
        ws_failure_backoff_s: int = 60,
        ping_interval_s: int = 30,
        ping_timeout_s: int = 10,
        stale_after_s: int = 45,
        subscribe_builder: Optional[Callable[[list[str]], dict]] = None,
    ):
        self.ws_url = ws_url
        self.api_url = api_url
        self.symbols = symbols
        self.reconnect_interval_s = reconnect_interval_s
        self.fallback_poll_interval_s = fallback_poll_interval_s
        self.max_ws_failures_before_backoff = max_ws_failures_before_backoff
        self.ws_failure_backoff_s = ws_failure_backoff_s
        self.ping_interval_s = max(1, int(ping_interval_s))
        self.ping_timeout_s = max(1, int(ping_timeout_s))
        self.stale_after_s = max(5, int(stale_after_s))
        self.subscribe_builder = subscribe_builder or self._default_subscribe_builder

        self._listeners: list[Callable[[str, float], None]] = []
        self._price_cache: dict[str, float] = {}
        self._ws: Optional[WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._watchdog_thread: Optional[threading.Thread] = None
        self._fallback_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._reconnect_request = threading.Event()
        self._state_lock = threading.RLock()
        self._ws_connected = False
        self._ws_failure_count = 0
        self._connection_attempt = 0
        self._last_message_monotonic = 0.0
        self._last_pong_monotonic = 0.0
        self._connected_since_monotonic = 0.0
        self._disconnect_reason = "not_connected"

    @staticmethod
    def _default_subscribe_builder(symbols: list[str]) -> dict:
        # Exchange-specific payload can be overridden via subscribe_builder.
        return {"type": "subscribe", "channels": [{"name": "ticker", "symbols": symbols}]}

    def add_listener(self, listener: Callable[[str, float], None]) -> None:
        self._listeners.append(listener)

    def get_cached_price(self, symbol: str) -> Optional[float]:
        with self._state_lock:
            return self._price_cache.get(symbol)

    def get_all_prices(self) -> dict[str, float]:
        with self._state_lock:
            return dict(self._price_cache)

    def start(self) -> None:
        with self._state_lock:
            if self._ws_thread is not None and self._ws_thread.is_alive():
                logger.info("WebSocket manager already running for %s", self.ws_url)
                return
            self._stop_event.clear()
            self._reconnect_request.clear()
            logger.info(
                "Starting WebSocket manager: url=%s symbols=%s ping_interval=%ss stale_after=%ss",
                self.ws_url,
                len(self.symbols),
                self.ping_interval_s,
                self.stale_after_s,
            )
        self._start_fallback_poller()
        self._start_watchdog()
        self._start_websocket_loop()

    def stop(self) -> None:
        self._stop_event.set()
        self._reconnect_request.set()
        with self._state_lock:
            ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception as exc:
                logger.warning("WebSocket close during shutdown failed: %s", exc)
        if self._ws_thread is not None and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=5)
        if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=5)
        if self._fallback_thread is not None and self._fallback_thread.is_alive():
            self._fallback_thread.join(timeout=5)
        with self._state_lock:
            self._ws = None
            self._ws_connected = False
            self._connected_since_monotonic = 0.0

    def _start_websocket_loop(self) -> None:
        if self._ws_thread is not None and self._ws_thread.is_alive():
            return

        def _runner() -> None:
            while not self._stop_event.is_set():
                ws: Optional[WebSocketApp] = None
                with self._state_lock:
                    self._connection_attempt += 1
                    attempt = self._connection_attempt
                    self._disconnect_reason = "connect_in_progress"
                    self._reconnect_request.clear()

                logger.info(
                    "WebSocket connection start: url=%s attempt=%s symbols=%s",
                    self.ws_url,
                    attempt,
                    len(self.symbols),
                )
                try:
                    ws = WebSocketApp(
                        self.ws_url,
                        on_open=self._on_open,
                        on_message=self._on_message,
                        on_error=self._on_error,
                        on_close=self._on_close,
                        on_pong=self._on_pong,
                    )
                    with self._state_lock:
                        self._ws = ws
                    ws.run_forever(
                        ping_interval=self.ping_interval_s,
                        ping_timeout=self.ping_timeout_s,
                        ping_payload="delta-heartbeat",
                    )
                except Exception as exc:
                    self._remember_disconnect_reason(f"loop_exception:{exc}", overwrite=False)
                    logger.exception("WebSocket loop failed on attempt %s: %s", attempt, exc)
                finally:
                    with self._state_lock:
                        if self._ws is ws:
                            self._ws = None
                        self._ws_connected = False
                        self._connected_since_monotonic = 0.0

                if not self._stop_event.is_set():
                    with self._state_lock:
                        self._ws_failure_count += 1
                        failure_count = self._ws_failure_count
                        reason = self._disconnect_reason
                        if reason in {"not_connected", "connect_in_progress", "connected"}:
                            reason = "run_forever_returned_without_close"
                            self._disconnect_reason = reason
                    delay = self._compute_reconnect_delay(failure_count)
                    logger.warning(
                        "WebSocket reconnect attempt %s scheduled in %.1fs after disconnect: %s",
                        failure_count,
                        delay,
                        reason,
                    )
                    if self._stop_event.wait(delay):
                        break

            logger.info("WebSocket supervisor stopped for %s", self.ws_url)

        self._ws_thread = threading.Thread(target=_runner, daemon=True, name="market-data-ws")
        self._ws_thread.start()

    def _start_watchdog(self) -> None:
        if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
            return

        def _watchdog() -> None:
            check_interval_s = min(1.0, max(0.5, self.stale_after_s / 4))
            while not self._stop_event.wait(check_interval_s):
                with self._state_lock:
                    if not self._ws_connected or self._last_message_monotonic <= 0:
                        continue
                    age_s = time.monotonic() - self._last_message_monotonic
                if age_s <= self.stale_after_s:
                    continue
                if self._request_reconnect(
                    f"watchdog_stale_connection:no_messages_for={age_s:.1f}s threshold={self.stale_after_s}s"
                ):
                    logger.warning(
                        "WebSocket watchdog triggered reconnect after %.1fs without messages",
                        age_s,
                    )

        self._watchdog_thread = threading.Thread(target=_watchdog, daemon=True, name="market-data-ws-watchdog")
        self._watchdog_thread.start()

    def _start_fallback_poller(self) -> None:
        if self._fallback_thread is not None and self._fallback_thread.is_alive():
            return

        def _poller() -> None:
            while not self._stop_event.is_set():
                if self._ws_connected:
                    self._stop_event.wait(0.5)
                    continue
                for symbol in self.symbols:
                    if self._stop_event.is_set():
                        return
                    try:
                        df = fetch_ticker(symbol=symbol, api_url=self.api_url)
                        if df.empty:
                            continue
                        row = df.iloc[0]
                        price = None
                        for col in ("mark_price", "close", "spot_price", "last_price", "price"):
                            if col in df.columns:
                                value = row.get(col)
                                if value is not None:
                                    try:
                                        parsed = float(value)
                                    except (TypeError, ValueError):
                                        parsed = 0.0
                                    if parsed > 0:
                                        price = parsed
                                        break
                        if price is not None:
                            self._set_price(symbol, price)
                    except Exception as exc:
                        logger.warning("Fallback poll failed for %s: %s", symbol, exc)
                self._stop_event.wait(self.fallback_poll_interval_s)

        self._fallback_thread = threading.Thread(target=_poller, daemon=True, name="market-data-rest-fallback")
        self._fallback_thread.start()

    def _on_open(self, ws) -> None:
        with self._state_lock:
            self._ws_connected = True
            self._ws_failure_count = 0
            self._reconnect_request.clear()
            self._disconnect_reason = "connected"
            now = time.monotonic()
            self._connected_since_monotonic = now
            self._last_message_monotonic = now
            self._last_pong_monotonic = now
            reconnect_number = max(0, self._connection_attempt - 1)
        try:
            payload = self.subscribe_builder(self.symbols)
            ws.send(json.dumps(payload))
            logger.info(
                "WebSocket connected: url=%s reconnects=%s; resubscribed to %s symbols",
                self.ws_url,
                reconnect_number,
                len(self.symbols),
            )
            self._restore_state_after_reconnect()
        except Exception as exc:
            self._remember_disconnect_reason(f"subscription_failed:{exc}", overwrite=True)
            logger.warning("WebSocket subscription failed: %s", exc)
            self._request_reconnect(f"subscription_failed:{exc}")

    def _on_message(self, ws, message: str) -> None:
        with self._state_lock:
            self._last_message_monotonic = time.monotonic()
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return

        # Flexible parsing for ticker-like payloads.
        updates = []
        if isinstance(payload, dict):
            updates.extend(self._extract_updates(payload))
            result = payload.get("result")
            if isinstance(result, dict):
                updates.extend(self._extract_updates(result))
            elif isinstance(result, list):
                for item in result:
                    if isinstance(item, dict):
                        updates.extend(self._extract_updates(item))
        elif isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    updates.extend(self._extract_updates(item))

        if isinstance(payload, dict):
            msg_type = str(payload.get("type") or "").lower()
            if msg_type in {"subscribed", "subscriptions"}:
                logger.info("WebSocket subscription status received: %s", payload)

        for symbol, price in updates:
            self._set_price(symbol, price)

    def _on_error(self, ws, error) -> None:
        self._remember_disconnect_reason(f"error:{error}", overwrite=False)
        logger.warning("WebSocket error: %s", error)

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        with self._state_lock:
            self._ws_connected = False
            self._connected_since_monotonic = 0.0
        if close_status_code is None and close_msg is None:
            reason = "code=None msg=None"
        else:
            reason = f"code={close_status_code} msg={close_msg}"
        self._remember_disconnect_reason(f"close:{reason}", overwrite=False)
        if self._stop_event.is_set():
            logger.info("WebSocket closed during shutdown: %s", reason)
        else:
            logger.warning("WebSocket disconnected: %s", reason)

    def _on_pong(self, ws, message: str) -> None:
        with self._state_lock:
            self._last_pong_monotonic = time.monotonic()

    @staticmethod
    def _extract_updates(payload: dict) -> list[tuple[str, float]]:
        symbol = payload.get("symbol") or payload.get("product_id")
        if not symbol:
            return []
        for key in ("mark_price", "price", "close", "last_price"):
            value = payload.get(key)
            try:
                price = float(value)
            except (TypeError, ValueError):
                continue
            if price > 0:
                return [(str(symbol), price)]
        return []

    def _set_price(self, symbol: str, price: float) -> None:
        with self._state_lock:
            self._price_cache[symbol] = float(price)
            listeners = list(self._listeners)
        for listener in listeners:
            try:
                listener(symbol, float(price))
            except Exception as exc:
                logger.warning("Price listener failed for %s: %s", symbol, exc)

    def _restore_state_after_reconnect(self) -> None:
        with self._state_lock:
            cached_symbols = len(self._price_cache)
            listener_count = len(self._listeners)
        logger.info(
            "WebSocket state restored after reconnect: cached_symbols=%s listeners=%s",
            cached_symbols,
            listener_count,
        )

    def _remember_disconnect_reason(self, reason: str, *, overwrite: bool) -> None:
        with self._state_lock:
            if overwrite or not self._disconnect_reason or self._disconnect_reason in {"not_connected", "connect_in_progress", "connected"}:
                self._disconnect_reason = reason

    def _request_reconnect(self, reason: str) -> bool:
        with self._state_lock:
            if self._stop_event.is_set() or self._reconnect_request.is_set():
                return False
            self._reconnect_request.set()
            self._disconnect_reason = reason
            ws = self._ws
        logger.warning("WebSocket reconnect requested: %s", reason)
        if ws is not None:
            try:
                ws.close()
            except Exception as exc:
                logger.warning("WebSocket close during reconnect failed: %s", exc)
        return True

    def _compute_reconnect_delay(self, failure_count: int) -> float:
        if failure_count <= 0:
            return float(self.reconnect_interval_s)
        delay = float(self.reconnect_interval_s) * (2 ** max(0, failure_count - 1))
        if failure_count >= self.max_ws_failures_before_backoff:
            delay = max(delay, float(self.ws_failure_backoff_s))
        return min(float(self.ws_failure_backoff_s), delay)
