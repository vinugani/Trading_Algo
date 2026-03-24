import json
import logging

from delta_exchange_bot.strategy.base import Signal, Strategy

logger = logging.getLogger(__name__)


def _log_structured(event: str, **fields) -> None:
    logger.debug(json.dumps({"event": event, **fields}, separators=(",", ":"), sort_keys=True))


class EMACrossoverStrategy(Strategy):
    def __init__(
        self,
        fast_period: int = 8,
        slow_period: int = 18,
        stop_loss_pct: float = 0.004,
        take_profit_pct: float = 0.008,
        trailing_stop_pct: float = 0.004,
        crossover_tolerance_pct: float = 0.0005,
        momentum_window: int = 3,
        min_momentum_confirmation_pct: float = 0.0003,
    ):
        if fast_period <= 0 or slow_period <= 0:
            raise ValueError("EMA periods must be > 0")
        if fast_period >= slow_period:
            raise ValueError("fast_period must be less than slow_period")
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.trailing_stop_pct = trailing_stop_pct
        self.crossover_tolerance_pct = crossover_tolerance_pct
        self.momentum_window = momentum_window
        self.min_momentum_confirmation_pct = min_momentum_confirmation_pct

    @staticmethod
    def _ema(prices: list[float], period: int) -> float | None:
        if len(prices) < period:
            return None
        alpha = 2.0 / (period + 1)
        ema = sum(prices[:period]) / period
        for price in prices[period:]:
            ema = (price - ema) * alpha + ema
        return ema

    def generate(self, market_data: dict[str, dict]) -> list[Signal]:
        signals: list[Signal] = []
        for symbol, series in market_data.items():
            prices = series.get("prices", [])
            if len(prices) < self.slow_period:
                continue

            current_price = float(prices[-1])
            fast_ema = self._ema(prices, self.fast_period)
            slow_ema = self._ema(prices, self.slow_period)
            if fast_ema is None or slow_ema is None:
                continue

            denominator = abs(slow_ema) if slow_ema != 0 else 1.0
            signed_gap_pct = (fast_ema - slow_ema) / denominator
            ema_gap_pct = abs(signed_gap_pct)
            returns = [
                abs((float(prices[idx]) - float(prices[idx - 1])) / float(prices[idx - 1]))
                for idx in range(1, len(prices))
                if float(prices[idx - 1]) != 0
            ]
            volatility_baseline = max((sum(returns) / len(returns)) if returns else 0.0, 1e-4)
            momentum_anchor = prices[-self.momentum_window - 1] if len(prices) >= self.momentum_window + 1 else None
            momentum_pct = 0.0
            if momentum_anchor not in (None, 0):
                momentum_pct = (current_price - float(momentum_anchor)) / float(momentum_anchor)
            gap_score = min(1.0, ema_gap_pct / max(volatility_baseline, self.crossover_tolerance_pct))
            momentum_score = min(
                1.0,
                abs(momentum_pct) / max(self.min_momentum_confirmation_pct * 4.0, 1e-6),
            )
            confidence = min(1.0, 0.6 * gap_score + 0.4 * momentum_score)
            bullish_ready = signed_gap_pct >= -self.crossover_tolerance_pct and momentum_pct >= -self.min_momentum_confirmation_pct
            bearish_ready = signed_gap_pct <= self.crossover_tolerance_pct and momentum_pct <= self.min_momentum_confirmation_pct
            reject_reason = "ema_and_momentum_not_aligned"

            if bullish_ready and signed_gap_pct > 0:
                action = "buy"
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
                reject_reason = "bullish_crossover_confirmed"
            elif bearish_ready and signed_gap_pct < 0:
                action = "sell"
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
                reject_reason = "bearish_crossover_confirmed"
            else:
                action = "hold"
                confidence = 0.0
                if signed_gap_pct >= -self.crossover_tolerance_pct and momentum_pct < -self.min_momentum_confirmation_pct:
                    reject_reason = "bullish_setup_rejected_negative_momentum"
                elif signed_gap_pct <= self.crossover_tolerance_pct and momentum_pct > self.min_momentum_confirmation_pct:
                    reject_reason = "bearish_setup_rejected_positive_momentum"
                elif abs(signed_gap_pct) > self.crossover_tolerance_pct:
                    reject_reason = "ema_gap_not_near_crossover"
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
                strategy="ema_crossover",
                symbol=symbol,
                action=action,
                decision_reason=reject_reason,
                indicators={
                    "price": float(current_price),
                    "fast_ema": float(fast_ema),
                    "slow_ema": float(slow_ema),
                },
                momentum={
                    "pct": float(momentum_pct),
                    "bullish_ready": bool(bullish_ready),
                    "bearish_ready": bool(bearish_ready),
                },
                score_components={
                    "signed_gap_pct": float(signed_gap_pct),
                    "gap_score": float(gap_score),
                    "momentum_score": float(momentum_score),
                    "volatility_baseline": float(volatility_baseline),
                },
                calculated_score=float(confidence),
                final_confidence=float(confidence),
            )
        return signals
