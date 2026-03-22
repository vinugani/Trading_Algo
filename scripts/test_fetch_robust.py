import sys
from pathlib import Path

# Add src to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from delta_exchange_bot.data.market_data import fetch_candles

def test_robust():
    api_url = "https://cdn-ind.testnet.deltaex.org"
    print("Testing robust fetch_candles for BTCUSD on Testnet...")
    df = fetch_candles("BTCUSD", "1m", api_url=api_url)
    
    if not df.empty:
        print(f"SUCCESS: Fetched {len(df)} candles.")
        print(f"Latest candle timestamp: {df['timestamp'].iloc[-1]}")
    else:
        print("FAILURE: Still fetched 0 candles.")

if __name__ == "__main__":
    test_robust()
