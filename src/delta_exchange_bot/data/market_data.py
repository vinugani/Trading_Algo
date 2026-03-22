import time
from typing import Optional

import pandas as pd

from delta_exchange_bot.api.delta_client import DeltaClient
from delta_exchange_bot.core.settings import Settings

SUPPORTED_INTERVALS = {"1m": 60, "5m": 300, "15m": 900}
DEFAULT_LOOKBACK_CANDLES = 200


def _build_public_client(api_url: Optional[str] = None) -> DeltaClient:
    if api_url is None:
        api_url = Settings().api_url
    return DeltaClient(api_key="", api_secret="", api_url=api_url)


def fetch_ticker(symbol: str, api_url: Optional[str] = None) -> pd.DataFrame:
    client = _build_public_client(api_url)
    payload = client.get_ticker(symbol)
    ticker = payload.get("result", payload) if isinstance(payload, dict) else {}

    if not isinstance(ticker, dict) or not ticker:
        return pd.DataFrame(columns=["symbol", "timestamp"])

    row = dict(ticker)
    row["symbol"] = row.get("symbol", symbol)
    row["timestamp"] = pd.to_datetime(row.get("time"), utc=True, errors="coerce")

    df = pd.json_normalize([row], sep="_")
    for col in (
        "mark_price",
        "spot_price",
        "close",
        "open",
        "high",
        "low",
        "volume",
        "turnover",
        "turnover_usd",
        "oi",
        "oi_value",
        "oi_value_usd",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def fetch_candles(symbol: str, interval: str, api_url: Optional[str] = None) -> pd.DataFrame:
    if interval not in SUPPORTED_INTERVALS:
        raise ValueError(f"Unsupported interval '{interval}'. Allowed: {', '.join(SUPPORTED_INTERVALS)}")

    client = _build_public_client(api_url)
    end_ts = int(time.time())
    lookback_s = SUPPORTED_INTERVALS[interval] * DEFAULT_LOOKBACK_CANDLES
    start_ts = end_ts - lookback_s

    payload = client.get_candles(symbol=symbol, resolution=interval, start=start_ts, end=end_ts)
    result = payload.get("result", []) if isinstance(payload, dict) else []

    # Robust Fallback for lagged Testnets (e.g. Delta India Testnet can lag by 60+ hours)
    if not result:
        # Try a 7-day lookback to find the latest available data
        fallback_start = end_ts - (86400 * 7)
        fallback_payload = client.get_candles(symbol=symbol, resolution=interval, start=fallback_start, end=end_ts)
        fallback_result = fallback_payload.get("result", []) if isinstance(fallback_payload, dict) else []
        if fallback_result:
            # Anchor to the latest candle found
            latest_found = fallback_result[-1]["time"]
            start_ts = latest_found - lookback_s
            end_ts = latest_found
            payload = client.get_candles(symbol=symbol, resolution=interval, start=start_ts, end=end_ts)
            result = payload.get("result", []) if isinstance(payload, dict) else []

    if not result:
        return pd.DataFrame(columns=["symbol", "timestamp", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(result)
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "time" in df.columns:
        df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True, errors="coerce")
    else:
        df["timestamp"] = pd.NaT
    df["symbol"] = symbol
    df = df.sort_values("timestamp").reset_index(drop=True)

    for col in ("open", "high", "low", "close", "volume"):
        if col not in df.columns:
            df[col] = pd.NA

    return df[["symbol", "timestamp", "open", "high", "low", "close", "volume"]]
