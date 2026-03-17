import json
import threading
from typing import Callable

from websocket import WebSocketApp


class DeltaWebSocket:
    def __init__(self, ws_url: str, on_message: Callable[[dict], None]):
        self.ws_url = ws_url
        self.on_message = on_message
        self._ws: WebSocketApp | None = None
        self._thread: threading.Thread | None = None

    def _handle_message(self, ws, message):
        data = json.loads(message)
        self.on_message(data)

    def start(self):
        self._ws = WebSocketApp(self.ws_url, on_message=self._handle_message)
        self._thread = threading.Thread(target=self._ws.run_forever, daemon=True)
        self._thread.start()

    def stop(self):
        if self._ws is not None:
            self._ws.close()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)
