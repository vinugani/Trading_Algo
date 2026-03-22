import requests
import time

def test_candles(symbol="BTCUSD", resolutions=["1m"]):
    base_url = "https://cdn-ind.testnet.deltaex.org"
    path = "/v2/history/candles"
    
    end = int(time.time())
    start = end - (60 * 60 * 24 * 7) # 7 days
    
    for res in resolutions:
        params = {
            "symbol": symbol,
            "resolution": res,
            "start": start,
            "end": end
        }
        url = f"{base_url}{path}"
        print(f"Testing resolution '{res}' for {symbol}...")
        try:
            resp = requests.get(url, params=params)
            data = resp.json()
            if resp.ok:
                result = data.get("result", [])
                print(f"  SUCCESS: Received {len(result)} candles.")
                if result:
                    print(f"  Sample: {result[0]}")
            else:
                print(f"  ERROR {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"  EXCEPTION: {e}")

if __name__ == "__main__":
    test_candles()
