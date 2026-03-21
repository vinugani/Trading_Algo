from __future__ import annotations

import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any
from typing import Optional

from delta_exchange_bot.strategy.market_regime import MarketRegime
from delta_exchange_bot.strategy.market_regime import MarketRegimeSnapshot


@dataclass
class Signal:
    symbol: str
    action: str  # 'buy'/'sell'/'hold'
    confidence: float
    price: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_stop_pct: Optional[float] = None


class Strategy(ABC):
    @abstractmethod
    def generate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Generate signals based on market data."""
        raise NotImplementedError


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
