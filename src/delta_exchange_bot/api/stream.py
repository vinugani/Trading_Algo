import json
import logging
import threading
import time
from typing import Callable

from websocket import WebSocketApp

logger = logging.getLogger(__name__)


class DeltaWebSocket:
    """Thin WebSocket wrapper with error/close handlers and auto-reconnect.

    This is a low-level helper used by RealtimeMarketDataService.  For
    most purposes prefer RealtimeMarketDataService directly because it
    includes REST fallback, back-off on repeated failures, and a richer
    listener interface.
    """

    RECONNECT_DELAY_S = 5
    MAX_RECONNECT_FAILURES = 3
    BACKOFF_DELAY_S = 60

    def __init__(self, ws_url: str, on_message: Callable[[dict], None]):
        self.ws_url = ws_url
        self.on_message = on_message
        self._ws: WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._consecutive_failures = 0

    def _handle_message(self, ws, message: str) -> None:
        try:
            data = json.loads(message)
            self.on_message(data)
        except Exception as exc:
            logger.warning("DeltaWebSocket message parse error: %s", exc)

    def _handle_error(self, ws, error) -> None:
        self._consecutive_failures += 1
        logger.warning("DeltaWebSocket error (failure #%s): %s", self._consecutive_failures, error)

    def _handle_close(self, ws, code, msg) -> None:
        logger.info("DeltaWebSocket closed: code=%s msg=%s", code, msg)

    def _handle_open(self, ws) -> None:
        self._consecutive_failures = 0
        logger.info("DeltaWebSocket connected to %s", self.ws_url)

    def start(self) -> None:
        self._stop_event.clear()

        def _runner() -> None:
            while not self._stop_event.is_set():
                try:
                    self._ws = WebSocketApp(
                        self.ws_url,
                        on_open=self._handle_open,
                        on_message=self._handle_message,
                        on_error=self._handle_error,
                        on_close=self._handle_close,
                    )
                    self._ws.run_forever()
                except Exception as exc:
                    logger.exception("DeltaWebSocket loop crashed: %s", exc)

                if self._stop_event.is_set():
                    break

                # Back off after repeated failures (e.g. 403 block from CloudFront)
                if self._consecutive_failures >= self.MAX_RECONNECT_FAILURES:
                    logger.warning(
                        "DeltaWebSocket entering %ss back-off after %s consecutive failures",
                        self.BACKOFF_DELAY_S,
                        self._consecutive_failures,
                    )
                    self._stop_event.wait(self.BACKOFF_DELAY_S)
                    self._consecutive_failures = 0
                else:
                    self._stop_event.wait(self.RECONNECT_DELAY_S)

        self._thread = threading.Thread(target=_runner, daemon=True, name="delta-ws")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)
