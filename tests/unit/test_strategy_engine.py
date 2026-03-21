import pandas as pd

from delta_exchange_bot.strategy.manager import StrategyManager
from delta_exchange_bot.strategy.market_regime import MarketRegime
from delta_exchange_bot.strategy.market_regime import MarketRegimeSnapshot


def _candles() -> pd.DataFrame:
    closes = [100 + (i * 0.2) for i in range(120)]
    return pd.DataFrame(
        {
            "open": closes,
            "high": [c + 0.5 for c in closes],
            "low": [c - 0.5 for c in closes],
            "close": closes,
            "volume": [1000] * len(closes),
        }
    )


def test_strategy_manager_picks_trend_following_for_trending(monkeypatch):
    manager = StrategyManager()
    monkeypatch.setattr(
        manager.regime_detector,
        "detect",
        lambda candles: MarketRegimeSnapshot(
            regime=MarketRegime.TRENDING,
            adx=35.0,
            atr=1.5,
            atr_pct=0.01,
            ema_slope_pct=0.002,
        ),
    )

    signal, regime, strategy_name = manager.generate_signal(symbol="BTCUSD", candles=_candles())

    assert regime == "trending"
    assert strategy_name == "trend_following"
    assert signal.symbol == "BTCUSD"


def test_strategy_manager_picks_mean_reversion_for_ranging(monkeypatch):
    manager = StrategyManager()
    monkeypatch.setattr(
        manager.regime_detector,
        "detect",
        lambda candles: MarketRegimeSnapshot(
            regime=MarketRegime.RANGING,
            adx=10.0,
            atr=0.3,
            atr_pct=0.001,
            ema_slope_pct=0.0001,
        ),
    )

    signal, regime, strategy_name = manager.generate_signal(symbol="ETHUSD", candles=_candles())

    assert regime == "ranging"
    assert strategy_name == "mean_reversion"
    assert signal.symbol == "ETHUSD"
