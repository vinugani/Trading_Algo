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
    """WebSocket-first market data with REST polling fallback."""

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
        subscribe_builder: Optional[Callable[[list[str]], dict]] = None,
    ):
        self.ws_url = ws_url
        self.api_url = api_url
        self.symbols = symbols
        self.reconnect_interval_s = reconnect_interval_s
        self.fallback_poll_interval_s = fallback_poll_interval_s
        self.max_ws_failures_before_backoff = max_ws_failures_before_backoff
        self.ws_failure_backoff_s = ws_failure_backoff_s
        self.subscribe_builder = subscribe_builder or self._default_subscribe_builder

        self._listeners: list[Callable[[str, float], None]] = []
        self._price_cache: dict[str, float] = {}
        self._ws: Optional[WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._fallback_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._ws_connected = False
        self._ws_failure_count = 0

    @staticmethod
    def _default_subscribe_builder(symbols: list[str]) -> dict:
        # Exchange-specific payload can be overridden via subscribe_builder.
        return {"type": "subscribe", "channels": [{"name": "ticker", "symbols": symbols}]}

    def add_listener(self, listener: Callable[[str, float], None]) -> None:
        self._listeners.append(listener)

    def get_cached_price(self, symbol: str) -> Optional[float]:
        return self._price_cache.get(symbol)

    def get_all_prices(self) -> dict[str, float]:
        return dict(self._price_cache)

    def start(self) -> None:
        self._stop_event.clear()
        self._start_fallback_poller()
        self._start_websocket_loop()

    def stop(self) -> None:
        self._stop_event.set()
        if self._ws is not None:
            self._ws.close()
        if self._ws_thread is not None and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=5)
        if self._fallback_thread is not None and self._fallback_thread.is_alive():
            self._fallback_thread.join(timeout=5)

    def _start_websocket_loop(self) -> None:
        def _runner() -> None:
            while not self._stop_event.is_set():
                try:
                    self._ws = WebSocketApp(
                        self.ws_url,
                        on_open=self._on_open,
                        on_message=self._on_message,
                        on_error=self._on_error,
                        on_close=self._on_close,
                    )
                    self._ws.run_forever(
                        ping_interval=30,
                        ping_timeout=10,
                    )
                except Exception as exc:
                    logger.exception("WebSocket loop failed: %s", exc)
                    self._ws_connected = False

                if not self._stop_event.is_set():
                    if self._ws_failure_count >= self.max_ws_failures_before_backoff:
                        logger.warning(
                            "WebSocket entering cooldown after %s failures; using REST fallback for %ss",
                            self._ws_failure_count,
                            self.ws_failure_backoff_s,
                        )
                        time.sleep(self.ws_failure_backoff_s)
                        self._ws_failure_count = 0
                    else:
                        time.sleep(self.reconnect_interval_s)

        self._ws_thread = threading.Thread(target=_runner, daemon=True)
        self._ws_thread.start()

    def _start_fallback_poller(self) -> None:
        def _poller() -> None:
            while not self._stop_event.is_set():
                if self._ws_connected:
                    time.sleep(0.5)
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
                time.sleep(self.fallback_poll_interval_s)

        self._fallback_thread = threading.Thread(target=_poller, daemon=True)
        self._fallback_thread.start()

    def _on_open(self, ws) -> None:
        self._ws_connected = True
        self._ws_failure_count = 0
        try:
            payload = self.subscribe_builder(self.symbols)
            ws.send(json.dumps(payload))
            logger.info("WebSocket connected and subscription sent for %s symbols", len(self.symbols))
        except Exception as exc:
            logger.warning("WebSocket subscription failed: %s", exc)

    def _on_message(self, ws, message: str) -> None:
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

        for symbol, price in updates:
            self._set_price(symbol, price)

    def _on_error(self, ws, error) -> None:
        self._ws_connected = False
        self._ws_failure_count += 1
        logger.warning("WebSocket error: %s", error)

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        self._ws_connected = False
        logger.info("WebSocket closed: code=%s msg=%s", close_status_code, close_msg)

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
        self._price_cache[symbol] = float(price)
        for listener in list(self._listeners):
            try:
                listener(symbol, float(price))
            except Exception as exc:
                logger.warning("Price listener failed for %s: %s", symbol, exc)
