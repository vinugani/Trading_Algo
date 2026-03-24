from __future__ import annotations

import logging

import pandas as pd

from delta_exchange_bot.strategy.base import Signal, Strategy
from delta_exchange_bot.strategy.market_regime import MarketRegime
from delta_exchange_bot.strategy.market_regime import MarketRegimeSnapshot
from delta_exchange_bot.strategy.base import CandleStrategy

logger = logging.getLogger(__name__)


class RSIScalpingStrategy(Strategy):
    def __init__(
        self,
        rsi_period: int = 14,
        ema_period: int = 20,
        long_rsi_threshold: float = 45.0,
        short_rsi_threshold: float = 55.0,
        stop_loss_pct: float = 0.004,
        take_profit_pct: float = 0.008,
        trailing_stop_pct: float = 0.004,
        price_ema_tolerance_pct: float = 0.002,
        extreme_rsi_buffer: float = 5.0,
    ):
        self.rsi_period = rsi_period
        self.ema_period = ema_period
        self.long_rsi_threshold = long_rsi_threshold
        self.short_rsi_threshold = short_rsi_threshold
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.trailing_stop_pct = trailing_stop_pct
        self.price_ema_tolerance_pct = price_ema_tolerance_pct
        self.extreme_rsi_buffer = extreme_rsi_buffer

    def _ema(self, prices: list[float], period: int) -> float | None:
        if len(prices) < period:
            return None

        alpha = 2.0 / (period + 1)
        ema = sum(prices[:period]) / period
        for price in prices[period:]:
            ema = (price - ema) * alpha + ema
        return ema

    def _rsi(self, prices: list[float], period: int) -> float | None:
        if len(prices) < period + 1:
            return None

        deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]

        # Wilder's smoothing — matches TradingView and all standard charting platforms
        gains = [d if d > 0 else 0.0 for d in deltas]
        losses = [-d if d < 0 else 0.0 for d in deltas]

        # Seed with simple average of first `period` values
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        # Apply Wilder's smoothing for all remaining values
        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def generate(self, market_data: dict[str, dict]) -> list[Signal]:
        signals: list[Signal] = []

        for symbol, series in market_data.items():
            prices = series.get("prices", [])
            if len(prices) < max(self.ema_period, self.rsi_period + 1):
                continue

            current_price = prices[-1]
            ema20 = self._ema(prices, self.ema_period)
            rsi = self._rsi(prices, self.rsi_period)

            if ema20 is None or rsi is None:
                continue

            long_denominator = max(float(self.long_rsi_threshold), 1e-9)
            short_denominator = max(100.0 - float(self.short_rsi_threshold), 1e-9)
            long_score = max(0.0, (self.long_rsi_threshold - rsi) / long_denominator)
            short_score = max(0.0, (rsi - self.short_rsi_threshold) / short_denominator)
            long_price_ok = current_price >= float(ema20) * (1.0 - self.price_ema_tolerance_pct)
            short_price_ok = current_price <= float(ema20) * (1.0 + self.price_ema_tolerance_pct)
            long_extreme_ok = rsi <= max(0.0, self.long_rsi_threshold - self.extreme_rsi_buffer)
            short_extreme_ok = rsi >= min(100.0, self.short_rsi_threshold + self.extreme_rsi_buffer)
            action = "hold"
            confidence = 0.0

            # Temporary relaxed test condition:
            # allow entries when price is very close to the EMA, or RSI is strongly stretched.
            if rsi < self.long_rsi_threshold and (long_price_ok or long_extreme_ok):
                action = "buy"
                confidence = min(1.0, long_score)
                signals.append(
                    Signal(
                        symbol=symbol,
                        action=action,
                        confidence=confidence,
                        price=current_price,
                        stop_loss=current_price * (1.0 - self.stop_loss_pct),
                        take_profit=current_price * (1.0 + self.take_profit_pct),
                        trailing_stop_pct=self.trailing_stop_pct,
                    )
                )
            elif rsi > self.short_rsi_threshold and (short_price_ok or short_extreme_ok):
                action = "sell"
                confidence = min(1.0, short_score)
                signals.append(
                    Signal(
                        symbol=symbol,
                        action=action,
                        confidence=confidence,
                        price=current_price,
                        stop_loss=current_price * (1.0 + self.stop_loss_pct),
                        take_profit=current_price * (1.0 - self.take_profit_pct),
                        trailing_stop_pct=self.trailing_stop_pct,
                    )
                )
            else:
                signals.append(
                    Signal(
                        symbol=symbol,
                        action=action,
                        confidence=confidence,
                        price=current_price,
                    )
                )

            logger.debug(
                "[%s] RSI scalping: price=%.4f rsi=%.2f ema=%.4f long_score=%.4f short_score=%.4f long_price_ok=%s short_price_ok=%s long_extreme_ok=%s short_extreme_ok=%s action=%s confidence=%.4f",
                symbol,
                current_price,
                float(rsi),
                float(ema20),
                min(1.0, long_score),
                min(1.0, short_score),
                long_price_ok,
                short_price_ok,
                long_extreme_ok,
                short_extreme_ok,
                action,
                confidence,
            )

        return signals


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
