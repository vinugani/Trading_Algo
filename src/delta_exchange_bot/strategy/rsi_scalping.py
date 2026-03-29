from __future__ import annotations

import json
import logging

import pandas as pd

from delta_exchange_bot.strategy.base import Signal, Strategy
from delta_exchange_bot.strategy.market_regime import MarketRegime
from delta_exchange_bot.strategy.market_regime import MarketRegimeSnapshot
from delta_exchange_bot.strategy.base import CandleStrategy

logger = logging.getLogger(__name__)


def _log_structured(event: str, **fields) -> None:
    logger.debug(json.dumps({"event": event, **fields}, separators=(",", ":"), sort_keys=True))


class RSIScalpingStrategy(Strategy):
    def __init__(
        self,
        rsi_period: int = 14,
        ema_period: int = 20,
        long_rsi_threshold: float = 47.0,
        short_rsi_threshold: float = 53.0,
        stop_loss_pct: float = 0.004,
        take_profit_pct: float = 0.008,
        trailing_stop_pct: float = 0.004,
        price_ema_tolerance_pct: float = 0.003,
        extreme_rsi_buffer: float = 5.0,
        momentum_window: int = 3,
        min_momentum_confirmation_pct: float = 0.0005,
        min_signal_confidence: float = 0.6,
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
        self.momentum_window = momentum_window
        self.min_momentum_confirmation_pct = min_momentum_confirmation_pct
        self.min_signal_confidence = min_signal_confidence

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

            momentum = 0.0
            momentum_anchor = prices[-self.momentum_window - 1] if len(prices) >= self.momentum_window + 1 else None
            if momentum_anchor not in (None, 0):
                momentum = (current_price - float(momentum_anchor)) / float(momentum_anchor)
            last_step = float(current_price - prices[-2]) if len(prices) >= 2 else 0.0

            long_denominator = max(float(self.long_rsi_threshold), 1e-9)
            short_denominator = max(100.0 - float(self.short_rsi_threshold), 1e-9)
            long_score = max(0.0, (self.long_rsi_threshold - rsi) / long_denominator)
            short_score = max(0.0, (rsi - self.short_rsi_threshold) / short_denominator)
            long_price_ok = current_price >= float(ema20) * (1.0 - self.price_ema_tolerance_pct)
            short_price_ok = current_price <= float(ema20) * (1.0 + self.price_ema_tolerance_pct)
            long_extreme_ok = rsi <= max(0.0, self.long_rsi_threshold - self.extreme_rsi_buffer)
            short_extreme_ok = rsi >= min(100.0, self.short_rsi_threshold + self.extreme_rsi_buffer)
            long_momentum_ok = momentum >= self.min_momentum_confirmation_pct or last_step >= 0 or long_extreme_ok
            short_momentum_ok = momentum <= -self.min_momentum_confirmation_pct or last_step <= 0 or short_extreme_ok
            momentum_score = min(
                1.0,
                abs(momentum) / max(self.min_momentum_confirmation_pct * 4.0, 1e-6),
            )
            long_extreme_score = 1.0 if long_extreme_ok else 0.0
            short_extreme_score = 1.0 if short_extreme_ok else 0.0
            ema_distance_pct = abs(current_price - float(ema20)) / max(abs(float(ema20)), 1e-9)
            ema_score = max(
                0.0,
                1.0 - (ema_distance_pct / max(self.price_ema_tolerance_pct * 2.0, 1e-6)),
            )
            action = "hold"
            confidence = 0.0
            reject_reason = "rsi_not_outside_entry_band"

            if rsi < self.long_rsi_threshold and (long_price_ok or long_extreme_ok) and long_momentum_ok:
                action = "buy"
                confidence = min(1.0, 0.40 * long_score + 0.20 * ema_score + 0.15 * momentum_score + 0.25 * long_extreme_score)
                if long_extreme_ok:
                    confidence = max(confidence, self.min_signal_confidence)
                if confidence < self.min_signal_confidence:
                    action = "hold"
                    confidence = 0.0
                    reject_reason = "long_signal_confidence_below_threshold"
                else:
                    reject_reason = "long_entry_confirmed"
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
            elif rsi > self.short_rsi_threshold and (short_price_ok or short_extreme_ok) and short_momentum_ok:
                action = "sell"
                confidence = min(1.0, 0.40 * short_score + 0.20 * ema_score + 0.15 * momentum_score + 0.25 * short_extreme_score)
                if short_extreme_ok:
                    confidence = max(confidence, self.min_signal_confidence)
                if confidence < self.min_signal_confidence:
                    action = "hold"
                    confidence = 0.0
                    reject_reason = "short_signal_confidence_below_threshold"
                else:
                    reject_reason = "short_entry_confirmed"
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

            if action == "hold":
                if rsi < self.long_rsi_threshold and not (long_price_ok or long_extreme_ok):
                    reject_reason = "long_rejected_price_too_far_from_ema"
                elif rsi < self.long_rsi_threshold and not long_momentum_ok:
                    reject_reason = "long_rejected_momentum_not_confirmed"
                elif rsi > self.short_rsi_threshold and not (short_price_ok or short_extreme_ok):
                    reject_reason = "short_rejected_price_too_far_from_ema"
                elif rsi > self.short_rsi_threshold and not short_momentum_ok:
                    reject_reason = "short_rejected_momentum_not_confirmed"
                signals.append(
                    Signal(
                        symbol=symbol,
                        action=action,
                        confidence=confidence,
                        price=current_price,
                    )
                )

            _log_structured(
                "strategy_component",
                strategy="rsi_scalping",
                symbol=symbol,
                action=action,
                decision_reason=reject_reason,
                indicators={
                    "price": float(current_price),
                    "rsi": float(rsi),
                    "ema": float(ema20),
                },
                momentum={
                    "pct": float(momentum),
                    "last_step": float(last_step),
                    "long_ok": bool(long_momentum_ok),
                    "short_ok": bool(short_momentum_ok),
                },
                score_components={
                    "long_score": float(min(1.0, long_score)),
                    "short_score": float(min(1.0, short_score)),
                    "ema_score": float(ema_score),
                    "momentum_score": float(momentum_score),
                    "long_extreme_score": float(long_extreme_score),
                    "short_extreme_score": float(short_extreme_score),
                },
                qualifiers={
                    "long_price_ok": bool(long_price_ok),
                    "short_price_ok": bool(short_price_ok),
                    "long_extreme_ok": bool(long_extreme_ok),
                    "short_extreme_ok": bool(short_extreme_ok),
                },
                calculated_score=float(confidence),
                final_confidence=float(confidence),
            )

        return signals


class RSIScalpingCandleStrategy(CandleStrategy):
    name = "rsi_scalping"
    allowed_regimes = {MarketRegime.RANGING, MarketRegime.LOW_VOLATILITY, MarketRegime.HIGH_VOLATILITY, MarketRegime.TRENDING}

    def __init__(self):
        self._impl = RSIScalpingStrategy()

    def generate(self, symbol: str, candles: pd.DataFrame, regime: MarketRegimeSnapshot) -> Signal:
        prices = pd.to_numeric(candles.get("close", pd.Series(dtype=float)), errors="coerce").dropna().tolist()
        if not prices or not self.can_run(regime):
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=float(prices[-1]) if prices else 0.0)
        out = self._impl.generate({symbol: {"prices": prices}})
        return out[0] if out else Signal(symbol=symbol, action="hold", confidence=0.0, price=float(prices[-1]))
