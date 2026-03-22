import json
import time
import sys
from pathlib import Path

# Add src to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.api.stream import DeltaWebSocket

def test_websocket():
    settings = Settings(mode="live")
    # Testing Researched URL (deltaex.org)
    ws_url = "wss://socket-ind.testnet.deltaex.org"
    print(f"Testing Research WebSocket URL: {ws_url}")
    
    received_messages = []
    
    def on_message(msg):
        print(f"Received: {msg}")
        received_messages.append(msg)

    ws = DeltaWebSocket(ws_url, on_message=on_message)
    
    print("Starting WebSocket...")
    ws.start()
    
    # Wait for some time to see if we get any heartbeat or initial message
    timeout = 10
    start_time = time.time()
    while time.time() - start_time < timeout:
        if received_messages:
            print("Successfully received message from WebSocket!")
            break
        time.sleep(1)
    
    if not received_messages:
        print("No messages received within 10 seconds.")
        print("Note: Some WebSockets require a subscription message before sending data.")
        
        # Try sending a subscribe message if we have a symbol
        symbol = settings.trade_symbols[0] if settings.trade_symbols else "BTCUSD"
        subscribe_msg = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "v2/ticker", "symbols": [symbol]}
                ]
            }
        }
        print(f"Sending subscription for {symbol}...")
        if ws._ws:
            ws._ws.send(json.dumps(subscribe_msg))
            
        # Wait another 5 seconds
        time.sleep(5)
        if received_messages:
            print("Successfully received message after subscription!")
        else:
            print("Still no messages received.")

    print("Stopping WebSocket...")
    ws.stop()
    
    if received_messages:
        print("WS TEST: SUCCESS")
    else:
        print("WS TEST: FAILED")

if __name__ == "__main__":
    test_websocket()
