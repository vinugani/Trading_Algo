import asyncio
import json
import logging
import time
import hmac
import hashlib
from typing import Callable, Optional, List, Dict
import threading

import websocket # websocket-client

logger = logging.getLogger(__name__)

class WebSocketManager:
    """
    Robust WebSocket Manager for Delta Exchange.
    Handles:
    - Auto-reconnection with exponential backoff
    - Heartbeats (Ping/Pong)
    - Authentication
    - Subscription management
    """
    
    def __init__(
        self,
        ws_url: str,
        api_key: str = "",
        api_secret: str = "",
        on_message_callback: Optional[Callable[[dict], None]] = None,
        subscriptions: Optional[List[dict]] = None
    ):
        self.ws_url = ws_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.on_message_callback = on_message_callback
        self.subscriptions = subscriptions or []
        
        self.ws: Optional[websocket.WebSocketApp] = None
        self.reconnect_count = 0
        self.max_reconnect_delay = 60
        self.is_running = False
        self._stop_event = threading.Event()
        
        # Connection state
        self.last_heartbeat = time.time()
        self.is_authenticated = False

    def _generate_auth_payload(self) -> dict:
        timestamp = str(int(time.time()))
        method = "GET"
        path = "/v2/websocket"
        signature_data = method + timestamp + path
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            signature_data.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        
        return {
            "type": "auth",
            "payload": {
                "api-key": self.api_key,
                "timestamp": timestamp,
                "signature": signature
            }
        }

    def _on_open(self, ws):
        logger.info("WebSocket connected.")
        self.reconnect_count = 0
        self.last_heartbeat = time.time()
        
        # Authenticate if keys are provided
        if self.api_key and self.api_secret:
            auth_payload = self._generate_auth_payload()
            ws.send(json.dumps(auth_payload))
            logger.info("Authentication request sent.")
        else:
            self._subscribe_all()

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            
            # Handle Auth Response
            if data.get("type") == "auth":
                if data.get("success"):
                    logger.info("WebSocket Authenticated successfully.")
                    self.is_authenticated = True
                    self._subscribe_all()
                else:
                    logger.error(f"WebSocket Authentication failed: {data.get('error')}")
            
            # Handle Heartbeat (Delta uses 'ping' or similar sometimes, or just raw pong)
            if data.get("type") == "heartbeat":
                self.last_heartbeat = time.time()
            
            if self.on_message_callback:
                self.on_message_callback(data)
                
        except Exception as e:
            logger.error(f"Error processing WS message: {e}")

    def _on_error(self, ws, error):
        logger.warning(f"WebSocket Error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.info(f"WebSocket Closed: {close_status_code} - {close_msg}")
        self.is_authenticated = False

    def _subscribe_all(self):
        if not self.subscriptions:
            return
        
        for sub in self.subscriptions:
            logger.info(f"Subscribing to: {sub}")
            self.ws.send(json.dumps({
                "type": "subscribe",
                "payload": sub
            }))

    def _run_forever(self):
        while not self._stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                
                # Run with ping_interval to prevent silent drops (critical for 10-min issue)
                # ping_interval=20, ping_timeout=10
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
                
            except Exception as e:
                logger.error(f"WebSocket run_forever crashed: {e}")
            
            if self._stop_event.is_set():
                break
                
            # Exponential Backoff
            self.reconnect_count += 1
            delay = min(self.max_reconnect_delay, (2 ** self.reconnect_count))
            logger.info(f"Reconnecting in {delay} seconds... (Attempt {self.reconnect_count})")
            time.sleep(delay)

    def start(self):
        if self.is_running:
            return
        self.is_running = True
        self._stop_event.clear()
        self.thread = threading.Thread(target=self._run_forever, daemon=True)
        self.thread.start()
        logger.info("WebSocket Manager started.")

    def stop(self):
        self.is_running = False
        self._stop_event.set()
        if self.ws:
            self.ws.close()
        logger.info("WebSocket Manager stopped.")

    def add_subscription(self, channel: str, symbols: List[str]):
        sub = {"channels": [{"name": channel, "symbols": symbols}]}
        self.subscriptions.append(sub)
        if self.ws and self.ws.sock and self.ws.sock.connected:
            self.ws.send(json.dumps({
                "type": "subscribe",
                "payload": sub
            }))
