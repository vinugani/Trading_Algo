import pandas as pd
import pytest

from delta_exchange_bot.strategy.rsi_scalping import RSIScalpingStrategy, RSIScalpingCandleStrategy
from delta_exchange_bot.strategy.market_regime import MarketRegime, MarketRegimeSnapshot


def _regime(r: MarketRegime) -> MarketRegimeSnapshot:
    return MarketRegimeSnapshot(regime=r, adx=0.0, atr=0.0, atr_pct=0.0, ema_slope_pct=0.0)


def _prices(last: float, n: int = 25) -> list[float]:
    return [last] * n


def test_rsi_scalping_long_signal(monkeypatch):
    strategy = RSIScalpingStrategy()

    monkeypatch.setattr(strategy, "_rsi", lambda prices, period: 25.0)
    monkeypatch.setattr(strategy, "_ema", lambda prices, period: 100.0)

    signals = strategy.generate({"BTCUSD": {"prices": _prices(101.0)}})

    assert len(signals) == 1
    s = signals[0]
    assert s.action == "buy"
    assert s.stop_loss == pytest.approx(101.0 * (1 - 0.004))
    assert s.take_profit == pytest.approx(101.0 * (1 + 0.008))
    assert s.trailing_stop_pct == pytest.approx(0.004)


def test_rsi_scalping_short_signal(monkeypatch):
    strategy = RSIScalpingStrategy()

    monkeypatch.setattr(strategy, "_rsi", lambda prices, period: 75.0)
    monkeypatch.setattr(strategy, "_ema", lambda prices, period: 100.0)

    signals = strategy.generate({"BTCUSD": {"prices": _prices(99.0)}})

    assert len(signals) == 1
    s = signals[0]
    assert s.action == "sell"
    assert s.stop_loss == pytest.approx(99.0 * (1 + 0.004))
    assert s.take_profit == pytest.approx(99.0 * (1 - 0.008))
    assert s.trailing_stop_pct == pytest.approx(0.004)


def test_rsi_scalping_hold_signal(monkeypatch):
    strategy = RSIScalpingStrategy()

    monkeypatch.setattr(strategy, "_rsi", lambda prices, period: 50.0)
    monkeypatch.setattr(strategy, "_ema", lambda prices, period: 100.0)

    signals = strategy.generate({"BTCUSD": {"prices": _prices(101.0)}})

    assert len(signals) == 1
    s = signals[0]
    assert s.action == "hold"
    assert s.stop_loss is None
    assert s.take_profit is None
    assert s.trailing_stop_pct is None


def test_rsi_scalping_candle_strategy_runs_in_trending_regime():
    """RSIScalpingCandleStrategy must not be blocked in TRENDING regime.

    Before the fix, TRENDING was absent from allowed_regimes, so can_run()
    returned False and the strategy silently returned hold — capping the
    portfolio confidence at 0.30 in trending markets and making the 0.50
    threshold unreachable.
    """
    strategy = RSIScalpingCandleStrategy()
    assert MarketRegime.TRENDING in strategy.allowed_regimes

    # Build minimal candles: 30 bars of slowly rising price so RSI is neutral
    prices = [100.0 + i * 0.01 for i in range(30)]
    candles = pd.DataFrame({"close": prices, "open": prices, "high": prices, "low": prices})

    trending_regime = _regime(MarketRegime.TRENDING)
    sig = strategy.generate("BTCUSD", candles, trending_regime)

    # Strategy must have evaluated — action is whatever RSI says, NOT a
    # forced hold due to regime gate.  We verify by checking can_run().
    assert strategy.can_run(trending_regime) is True
    # Signal must be a valid Signal object with a price (not the default 0.0
    # that would indicate data was rejected before evaluation).
    assert sig.price > 0


def test_rsi_scalping_candle_strategy_blocked_regimes_unchanged():
    """Confirm no regressions: the strategy still reports as runnable for all
    previously supported regimes."""
    strategy = RSIScalpingCandleStrategy()
    for regime in (MarketRegime.RANGING, MarketRegime.LOW_VOLATILITY, MarketRegime.HIGH_VOLATILITY):
        assert strategy.can_run(_regime(regime)) is True
