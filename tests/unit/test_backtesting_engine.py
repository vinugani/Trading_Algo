import pandas as pd
import pytest

from delta_exchange_bot.backtesting.engine import BacktestEngine
from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.strategy.base import Strategy


class ScriptedStrategy(Strategy):
    def __init__(self, signal_by_len: dict[int, Signal]):
        self.signal_by_len = signal_by_len

    def generate(self, market_data: dict[str, dict]) -> list[Signal]:
        symbol = next(iter(market_data.keys()))
        prices = market_data[symbol]["prices"]
        n = len(prices)
        signal = self.signal_by_len.get(n, Signal(symbol=symbol, action="hold", confidence=0.0, price=prices[-1]))
        return [signal]


def _candles(closes: list[float], symbol: str = "BTCUSD") -> pd.DataFrame:
    ts = pd.date_range("2026-01-01 00:00:00+00:00", periods=len(closes), freq="1min")
    return pd.DataFrame(
        {
            "symbol": [symbol] * len(closes),
            "timestamp": ts,
            "open": closes,
            "high": closes,
            "low": closes,
            "close": closes,
        }
    )


def test_backtesting_metrics_from_signal_exits():
    candles = _candles([99, 100, 105, 110, 105, 110, 115])
    signals = {
        2: Signal(symbol="BTCUSD", action="buy", confidence=1.0, price=100.0),
        4: Signal(symbol="BTCUSD", action="sell", confidence=1.0, price=110.0),  # closes long
        5: Signal(symbol="BTCUSD", action="sell", confidence=1.0, price=105.0),  # opens short
        7: Signal(symbol="BTCUSD", action="buy", confidence=1.0, price=115.0),   # closes short
    }
    strategy = ScriptedStrategy(signals)
    engine = BacktestEngine(strategy, initial_equity=10000.0, position_size=1.0, fee_rate=0.0)

    result = engine.run(candles, symbol="BTCUSD")

    assert result.metrics["total_trades"] == 2.0
    assert result.metrics["total_pnl"] == pytest.approx(0.0)
    assert result.metrics["win_rate"] == pytest.approx(50.0)
    assert result.metrics["profit_factor"] == pytest.approx(1.0)
    assert result.metrics["max_drawdown"] > 0.0


def test_backtesting_stop_loss_trigger():
    candles = pd.DataFrame(
        {
            "symbol": ["BTCUSD", "BTCUSD", "BTCUSD", "BTCUSD"],
            "timestamp": pd.date_range("2026-01-01 00:00:00+00:00", periods=4, freq="1min"),
            "open": [100.0, 100.0, 100.0, 100.0],
            "high": [100.0, 100.1, 100.5, 100.5],
            "low": [100.0, 99.7, 98.9, 99.5],
            "close": [100.0, 100.0, 99.2, 99.7],
        }
    )
    signals = {
        2: Signal(
            symbol="BTCUSD",
            action="buy",
            confidence=1.0,
            price=100.0,
            stop_loss=99.0,
            take_profit=110.0,
            trailing_stop_pct=0.004,
        )
    }
    strategy = ScriptedStrategy(signals)
    engine = BacktestEngine(strategy, initial_equity=10000.0, position_size=1.0, fee_rate=0.0)

    result = engine.run(candles, symbol="BTCUSD")

    assert len(result.trades) == 1
    assert result.trades.iloc[0]["exit_reason"] == "stop_loss"
    assert result.trades.iloc[0]["net_pnl"] != 0
