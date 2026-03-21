from __future__ import annotations

import pandas as pd

from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.strategy.market_regime import MarketRegime
from delta_exchange_bot.strategy.market_regime import MarketRegimeDetector

from .base import CandleStrategy
from .mean_reversion import MeanReversionStrategy
from .rsi_scalping import RSIScalpingCandleStrategy
from .trend_following import TrendFollowingStrategy


class StrategyManager:
    """Selects strategy by detected market regime."""

    def __init__(
        self,
        regime_detector: MarketRegimeDetector | None = None,
        rsi_scalping: CandleStrategy | None = None,
        trend_following: CandleStrategy | None = None,
        mean_reversion: CandleStrategy | None = None,
    ):
        self.regime_detector = regime_detector or MarketRegimeDetector()
        self.rsi_scalping = rsi_scalping or RSIScalpingCandleStrategy()
        self.trend_following = trend_following or TrendFollowingStrategy()
        self.mean_reversion = mean_reversion or MeanReversionStrategy()

    def _pick(self, regime: MarketRegime) -> CandleStrategy:
        if regime == MarketRegime.TRENDING:
            return self.trend_following
        if regime == MarketRegime.RANGING:
            return self.mean_reversion
        if regime == MarketRegime.HIGH_VOLATILITY:
            return self.rsi_scalping
        if regime == MarketRegime.LOW_VOLATILITY:
            return self.mean_reversion
        return self.rsi_scalping

    def generate_signal(self, symbol: str, candles: pd.DataFrame) -> tuple[Signal, str, str]:
        snapshot = self.regime_detector.detect(candles)
        strategy = self._pick(snapshot.regime)
        signal = strategy.generate(symbol=symbol, candles=candles, regime=snapshot)
        return signal, snapshot.regime.value, strategy.name
