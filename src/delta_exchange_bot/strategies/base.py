from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.strategy.market_regime import MarketRegime
from delta_exchange_bot.strategy.market_regime import MarketRegimeSnapshot


class CandleStrategy(ABC):
    name: str = "base"
    allowed_regimes: set[MarketRegime] = set()

    def can_run(self, regime: MarketRegimeSnapshot) -> bool:
        if not self.allowed_regimes:
            return True
        return regime.regime in self.allowed_regimes

    @abstractmethod
    def generate(self, symbol: str, candles: pd.DataFrame, regime: MarketRegimeSnapshot) -> Signal:
        raise NotImplementedError
