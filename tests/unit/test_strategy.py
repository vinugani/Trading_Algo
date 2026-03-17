from delta_exchange_bot.strategy.momentum import MomentumStrategy


def test_momentum_strategy_buy_signal():
    strategy = MomentumStrategy(window=3, threshold=0.001)
    market_data = {
        "BTCUSDT": {"prices": [100.0, 101.0, 102.0, 103.0, 110.0]}
    }

    signals = strategy.generate(market_data)

    assert len(signals) == 1
    assert signals[0].action == "buy"
    assert signals[0].symbol == "BTCUSDT"


def test_momentum_strategy_hold_signal():
    strategy = MomentumStrategy(window=3, threshold=0.5)
    market_data = {
        "BTCUSDT": {"prices": [100.0, 101.0, 102.0, 101.5, 101.8]}
    }

    signals = strategy.generate(market_data)
    assert signals[0].action == "hold"
