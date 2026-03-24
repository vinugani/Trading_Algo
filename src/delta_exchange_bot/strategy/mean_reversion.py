from __future__ import annotations

import logging

import pandas as pd

from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.strategy.market_regime import MarketRegime
from delta_exchange_bot.strategy.market_regime import MarketRegimeSnapshot

from .base import CandleStrategy

logger = logging.getLogger(__name__)


class MeanReversionStrategy(CandleStrategy):
    name = "mean_reversion"
    allowed_regimes = {MarketRegime.RANGING, MarketRegime.LOW_VOLATILITY}

    def __init__(
        self,
        lookback: int = 20,
        z_entry: float = 0.5,
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
            logger.debug("[%s] Mean reversion hold: no close data", symbol)
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=0.0)
        if not self.can_run(regime):
            logger.debug("[%s] Mean reversion hold: regime=%s not allowed", symbol, regime.regime.value)
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=float(close.iloc[-1]))
        if len(close) < self.lookback:
            logger.debug(
                "[%s] Mean reversion hold: insufficient candles=%s required=%s",
                symbol,
                len(close),
                self.lookback,
            )
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=float(close.iloc[-1]))

        window = close.iloc[-self.lookback :]
        mean = float(window.mean())
        std = float(window.std(ddof=0))
        price = float(close.iloc[-1])
        if std <= 0:
            logger.debug("[%s] Mean reversion hold: std=%.6f", symbol, std)
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=price)
        z = (price - mean) / std
        confidence = min(1.0, max(0.0, (abs(z) - self.z_entry) / max(self.z_entry, 0.1) + 0.25))

        logger.debug(
            "[%s] Mean reversion: price=%.4f mean=%.4f std=%.6f z=%.4f threshold=%.4f confidence=%.4f",
            symbol,
            price,
            mean,
            std,
            z,
            self.z_entry,
            confidence,
        )

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
        logger.debug(
            "[%s] Mean reversion hold: z-score %.4f did not cross +/- %.4f",
            symbol,
            z,
            self.z_entry,
        )
        return Signal(symbol=symbol, action="hold", confidence=0.0, price=price)
