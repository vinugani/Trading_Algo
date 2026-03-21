from __future__ import annotations

import pandas as pd

from delta_exchange_bot.strategy.base import Signal, Strategy
from delta_exchange_bot.strategy.market_regime import MarketRegime
from delta_exchange_bot.strategy.market_regime import MarketRegimeSnapshot
from delta_exchange_bot.strategy.base import CandleStrategy


class RSIScalpingStrategy(Strategy):
    def __init__(
        self,
        rsi_period: int = 14,
        ema_period: int = 20,
        long_rsi_threshold: float = 30.0,
        short_rsi_threshold: float = 70.0,
        stop_loss_pct: float = 0.004,
        take_profit_pct: float = 0.008,
        trailing_stop_pct: float = 0.004,
    ):
        self.rsi_period = rsi_period
        self.ema_period = ema_period
        self.long_rsi_threshold = long_rsi_threshold
        self.short_rsi_threshold = short_rsi_threshold
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.trailing_stop_pct = trailing_stop_pct

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
        window = deltas[-period:]
        gains = [d for d in window if d > 0]
        losses = [-d for d in window if d < 0]

        avg_gain = sum(gains) / period if gains else 0.0
        avg_loss = sum(losses) / period if losses else 0.0

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

            if rsi < self.long_rsi_threshold and current_price > ema20:
                signals.append(
                    Signal(
                        symbol=symbol,
                        action="buy",
                        confidence=min(1.0, (self.long_rsi_threshold - rsi) / self.long_rsi_threshold),
                        price=current_price,
                        stop_loss=current_price * (1.0 - self.stop_loss_pct),
                        take_profit=current_price * (1.0 + self.take_profit_pct),
                        trailing_stop_pct=self.trailing_stop_pct,
                    )
                )
            elif rsi > self.short_rsi_threshold and current_price < ema20:
                signals.append(
                    Signal(
                        symbol=symbol,
                        action="sell",
                        confidence=min(1.0, (rsi - self.short_rsi_threshold) / (100.0 - self.short_rsi_threshold)),
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
                        action="hold",
                        confidence=0.0,
                        price=current_price,
                    )
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
