from __future__ import annotations

import pandas as pd

from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.strategy.market_regime import MarketRegime
from delta_exchange_bot.strategy.market_regime import MarketRegimeSnapshot
from delta_exchange_bot.strategy.rsi_scalping import RSIScalpingStrategy

from .base import CandleStrategy


class RSIScalpingCandleStrategy(CandleStrategy):
    name = "rsi_scalping"
    allowed_regimes = {MarketRegime.RANGING, MarketRegime.LOW_VOLATILITY, MarketRegime.HIGH_VOLATILITY}

    def __init__(self):
        self._impl = RSIScalpingStrategy()

    def generate(self, symbol: str, candles: pd.DataFrame, regime: MarketRegimeSnapshot) -> Signal:
        prices = pd.to_numeric(candles.get("close", pd.Series(dtype=float)), errors="coerce").dropna().tolist()
        if not prices or not self.can_run(regime):
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=float(prices[-1]) if prices else 0.0)
        out = self._impl.generate({symbol: {"prices": prices}})
        return out[0] if out else Signal(symbol=symbol, action="hold", confidence=0.0, price=float(prices[-1]))
