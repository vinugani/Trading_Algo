import pytest

from delta_exchange_bot.strategy.ema_crossover import EMACrossoverStrategy


def test_ema_crossover_buy_signal():
    strategy = EMACrossoverStrategy(fast_period=3, slow_period=5)
    prices = [100, 101, 102, 103, 104, 105, 106]
    signals = strategy.generate({"BTCUSD": {"prices": prices}})
    assert len(signals) == 1
    s = signals[0]
    assert s.action == "buy"
    assert s.stop_loss == pytest.approx(s.price * (1.0 - 0.004))
    assert s.take_profit == pytest.approx(s.price * (1.0 + 0.008))


def test_ema_crossover_sell_signal():
    strategy = EMACrossoverStrategy(fast_period=3, slow_period=5)
    prices = [106, 105, 104, 103, 102, 101, 100]
    signals = strategy.generate({"BTCUSD": {"prices": prices}})
    assert len(signals) == 1
    s = signals[0]
    assert s.action == "sell"
    assert s.stop_loss == pytest.approx(s.price * (1.0 + 0.004))
    assert s.take_profit == pytest.approx(s.price * (1.0 - 0.008))


def test_ema_crossover_rejects_invalid_periods():
    with pytest.raises(ValueError):
        EMACrossoverStrategy(fast_period=10, slow_period=10)
