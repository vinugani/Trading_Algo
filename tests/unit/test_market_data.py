import pandas as pd
import pytest

from delta_exchange_bot.data.market_data import fetch_candles, fetch_ticker


class FakeClient:
    def get_ticker(self, symbol: str):
        return {
            "success": True,
            "result": {
                "symbol": symbol,
                "mark_price": "100.25",
                "spot_price": "100.10",
                "time": "2026-03-16T12:00:00Z",
            },
        }

    def get_candles(self, symbol: str, resolution: str, start: int, end: int):
        return {
            "success": True,
            "result": [
                {"time": 200, "open": 12, "high": 15, "low": 11, "close": 14, "volume": 5},
                {"time": 100, "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 7},
            ],
        }


def test_fetch_ticker_returns_dataframe(monkeypatch):
    monkeypatch.setattr(
        "delta_exchange_bot.data.market_data._build_public_client",
        lambda api_url=None: FakeClient(),
    )

    df = fetch_ticker("BTCUSD")

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.loc[0, "symbol"] == "BTCUSD"
    assert df.loc[0, "mark_price"] == pytest.approx(100.25)


def test_fetch_candles_returns_sorted_dataframe(monkeypatch):
    monkeypatch.setattr(
        "delta_exchange_bot.data.market_data._build_public_client",
        lambda api_url=None: FakeClient(),
    )

    df = fetch_candles("BTCUSD", "1m")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["symbol", "timestamp", "open", "high", "low", "close", "volume"]
    assert len(df) == 2
    assert df["timestamp"].is_monotonic_increasing
    assert df.loc[0, "open"] == 10


def test_fetch_candles_rejects_unsupported_interval():
    with pytest.raises(ValueError):
        fetch_candles("BTCUSD", "30m")
