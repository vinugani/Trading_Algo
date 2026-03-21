from __future__ import annotations

import pandas as pd

from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.strategy.market_regime import MarketRegime
from delta_exchange_bot.strategy.market_regime import MarketRegimeSnapshot

from .base import CandleStrategy


class MeanReversionStrategy(CandleStrategy):
    name = "mean_reversion"
    allowed_regimes = {MarketRegime.RANGING, MarketRegime.LOW_VOLATILITY}

    def __init__(
        self,
        lookback: int = 20,
        z_entry: float = 1.5,
        stop_loss_pct: float = 0.004,
        take_profit_pct: float = 0.006,
        trailing_stop_pct: float = 0.003,
    ):
        self.lookback = lookback
        self.z_entry = z_entry
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.trailing_stop_pct = trailing_stop_pct

    def generate(self, symbol: str, candles: pd.DataFrame, regime: MarketRegimeSnapshot) -> Signal:
        close = pd.to_numeric(candles.get("close", pd.Series(dtype=float)), errors="coerce").dropna()
        if close.empty:
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=0.0)
        if not self.can_run(regime):
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=float(close.iloc[-1]))
        if len(close) < self.lookback:
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=float(close.iloc[-1]))

        window = close.iloc[-self.lookback :]
        mean = float(window.mean())
        std = float(window.std(ddof=0))
        price = float(close.iloc[-1])
        if std <= 0:
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=price)
        z = (price - mean) / std
        confidence = min(1.0, abs(z) / 3.0)

        if z <= -self.z_entry:
            return Signal(
                symbol=symbol,
                action="buy",
                confidence=confidence,
                price=price,
                stop_loss=price * (1.0 - self.stop_loss_pct),
                take_profit=price * (1.0 + self.take_profit_pct),
                trailing_stop_pct=self.trailing_stop_pct,
            )
        if z >= self.z_entry:
            return Signal(
                symbol=symbol,
                action="sell",
                confidence=confidence,
                price=price,
                stop_loss=price * (1.0 + self.stop_loss_pct),
                take_profit=price * (1.0 - self.take_profit_pct),
                trailing_stop_pct=self.trailing_stop_pct,
            )
        return Signal(symbol=symbol, action="hold", confidence=0.0, price=price)
