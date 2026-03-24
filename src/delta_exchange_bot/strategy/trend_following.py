from __future__ import annotations

import logging

import pandas as pd

from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.strategy.market_regime import MarketRegime
from delta_exchange_bot.strategy.market_regime import MarketRegimeSnapshot

from .base import CandleStrategy

logger = logging.getLogger(__name__)


class TrendFollowingStrategy(CandleStrategy):
    name = "trend_following"
    allowed_regimes = {MarketRegime.TRENDING, MarketRegime.HIGH_VOLATILITY}

    def __init__(
        self,
        fast_ema: int = 9,
        slow_ema: int = 21,
        stop_loss_atr_mult: float = 1.2,
        take_profit_atr_mult: float = 2.4,
        trailing_stop_pct: float = 0.004,
    ):
        self.fast_ema = fast_ema
        self.slow_ema = slow_ema
        self.stop_loss_atr_mult = stop_loss_atr_mult
        self.take_profit_atr_mult = take_profit_atr_mult
        self.trailing_stop_pct = trailing_stop_pct

    def generate(self, symbol: str, candles: pd.DataFrame, regime: MarketRegimeSnapshot) -> Signal:
        close = pd.to_numeric(candles.get("close", pd.Series(dtype=float)), errors="coerce").dropna()
        if close.empty:
            logger.debug("[%s] Trend following hold: no close data", symbol)
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=0.0)
        if not self.can_run(regime):
            logger.debug("[%s] Trend following hold: regime=%s not allowed", symbol, regime.regime.value)
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=float(close.iloc[-1]))
        if len(close) < max(self.fast_ema, self.slow_ema):
            logger.debug(
                "[%s] Trend following hold: insufficient candles=%s required=%s",
                symbol,
                len(close),
                max(self.fast_ema, self.slow_ema),
            )
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=float(close.iloc[-1]))

        fast = close.ewm(span=self.fast_ema, adjust=False).mean().iloc[-1]
        slow = close.ewm(span=self.slow_ema, adjust=False).mean().iloc[-1]
        price = float(close.iloc[-1])
        atr = max(0.0, regime.atr)
        if atr == 0:
            atr = price * 0.003

        confidence = min(1.0, abs(float(fast - slow)) / price) if price > 0 else 0.0
        logger.debug(
            "[%s] Trend following: price=%.4f fast_ema=%.4f slow_ema=%.4f atr=%.6f confidence=%.4f",
            symbol,
            price,
            float(fast),
            float(slow),
            float(atr),
            confidence,
        )
        if fast > slow:
            stop = price - (atr * self.stop_loss_atr_mult)
            tp = price + (atr * self.take_profit_atr_mult)
            return Signal(
                symbol=symbol,
                action="buy",
                confidence=confidence,
                price=price,
                stop_loss=stop,
                take_profit=tp,
                trailing_stop_pct=self.trailing_stop_pct,
            )
        if fast < slow:
            stop = price + (atr * self.stop_loss_atr_mult)
            tp = price - (atr * self.take_profit_atr_mult)
            return Signal(
                symbol=symbol,
                action="sell",
                confidence=confidence,
                price=price,
                stop_loss=stop,
                take_profit=tp,
                trailing_stop_pct=self.trailing_stop_pct,
            )
        logger.debug("[%s] Trend following hold: fast EMA equals slow EMA", symbol)
        return Signal(symbol=symbol, action="hold", confidence=0.0, price=price)
