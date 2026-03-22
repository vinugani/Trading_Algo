import requests
import time

def check_lag(symbol="BTCUSD"):
    url = "https://cdn-ind.testnet.deltaex.org/v2/history/candles"
    now = int(time.time())
    params = {
        "symbol": symbol,
        "resolution": "1m",
        "start": now - 86400 * 7, # 7 days
        "end": now
    }
    
    r = requests.get(url, params=params)
    data = r.json().get("result", [])
    if not data:
        print("No data found in last 7 days.")
        return

    latest_ts = data[-1]["time"]
    diff = now - latest_ts
    print(f"Current Time: {now}")
    print(f"Latest Candle: {latest_ts}")
    print(f"Lag (seconds): {diff}")
    print(f"Lag (hours): {diff / 3600:.2f}")

if __name__ == "__main__":
    check_lag()
