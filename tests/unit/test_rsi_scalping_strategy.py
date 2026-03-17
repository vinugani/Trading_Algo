import pytest

from delta_exchange_bot.strategy.rsi_scalping import RSIScalpingStrategy


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
