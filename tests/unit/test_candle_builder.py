import pandas as pd
import pytest

from delta_exchange_bot.data.candle_builder import build_ohlc_candles


def test_build_ohlc_candles_1_minute():
    ticks = pd.DataFrame(
        [
            {"timestamp": "2026-03-16T10:00:05Z", "price": 100},
            {"timestamp": "2026-03-16T10:00:20Z", "price": 102},
            {"timestamp": "2026-03-16T10:00:50Z", "price": 101},
            {"timestamp": "2026-03-16T10:01:10Z", "price": 103},
            {"timestamp": "2026-03-16T10:01:55Z", "price": 99},
        ]
    )

    candles = build_ohlc_candles(ticks, "1 minute")

    assert len(candles) == 2
    assert list(candles.columns) == ["timestamp", "open", "high", "low", "close"]
    assert candles.iloc[0]["open"] == 100
    assert candles.iloc[0]["high"] == 102
    assert candles.iloc[0]["low"] == 100
    assert candles.iloc[0]["close"] == 101
    assert candles.iloc[1]["open"] == 103
    assert candles.iloc[1]["close"] == 99


def test_build_ohlc_candles_5_minutes():
    ticks = pd.DataFrame(
        [
            {"timestamp": "2026-03-16T10:00:05Z", "price": 100},
            {"timestamp": "2026-03-16T10:01:20Z", "price": 105},
            {"timestamp": "2026-03-16T10:02:10Z", "price": 103},
            {"timestamp": "2026-03-16T10:04:59Z", "price": 107},
            {"timestamp": "2026-03-16T10:05:01Z", "price": 106},
        ]
    )

    candles = build_ohlc_candles(ticks, "5m")

    assert len(candles) == 2
    assert candles.iloc[0]["open"] == 100
    assert candles.iloc[0]["high"] == 107
    assert candles.iloc[0]["low"] == 100
    assert candles.iloc[0]["close"] == 107
    assert candles.iloc[1]["open"] == 106
    assert candles.iloc[1]["close"] == 106


def test_build_ohlc_candles_invalid_timeframe():
    with pytest.raises(ValueError):
        build_ohlc_candles([{"timestamp": "2026-03-16T10:00:00Z", "price": 100}], "15m")
