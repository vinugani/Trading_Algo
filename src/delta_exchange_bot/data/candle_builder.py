from typing import Iterable, Mapping

import pandas as pd

SUPPORTED_TIMEFRAMES = {
    "1m": "1min",
    "1 minute": "1min",
    "5m": "5min",
    "5 minutes": "5min",
}


def build_ohlc_candles(
    tick_data: pd.DataFrame | Iterable[Mapping[str, object]],
    timeframe: str,
    *,
    timestamp_col: str = "timestamp",
    price_col: str = "price",
) -> pd.DataFrame:
    """Build OHLC candles from tick data.

    `tick_data` must contain timestamp and price fields.
    Supported timeframes: 1 minute, 5 minutes.
    """
    resolution = SUPPORTED_TIMEFRAMES.get(timeframe.strip().lower())
    if resolution is None:
        raise ValueError("Unsupported timeframe. Allowed: 1 minute, 5 minutes")

    if isinstance(tick_data, pd.DataFrame):
        ticks = tick_data.copy()
    else:
        ticks = pd.DataFrame(list(tick_data))

    if timestamp_col not in ticks.columns or price_col not in ticks.columns:
        raise ValueError(f"Tick data must include '{timestamp_col}' and '{price_col}' columns")

    ticks[timestamp_col] = pd.to_datetime(ticks[timestamp_col], utc=True, errors="coerce")
    ticks[price_col] = pd.to_numeric(ticks[price_col], errors="coerce")
    ticks = ticks.dropna(subset=[timestamp_col, price_col]).sort_values(timestamp_col)

    if ticks.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close"])

    ohlc = (
        ticks.set_index(timestamp_col)[price_col]
        .resample(resolution, label="left", closed="left")
        .ohlc()
        .dropna(how="any")
        .reset_index()
    )
    return ohlc.rename(columns={timestamp_col: "timestamp"})
