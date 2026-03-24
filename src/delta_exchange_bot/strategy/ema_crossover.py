import logging

from delta_exchange_bot.strategy.base import Signal, Strategy

logger = logging.getLogger(__name__)


class EMACrossoverStrategy(Strategy):
    def __init__(
        self,
        fast_period: int = 9,
        slow_period: int = 21,
        stop_loss_pct: float = 0.004,
        take_profit_pct: float = 0.008,
        trailing_stop_pct: float = 0.004,
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
            ema_gap_pct = abs(fast_ema - slow_ema) / denominator
            returns = [
                abs((float(prices[idx]) - float(prices[idx - 1])) / float(prices[idx - 1]))
                for idx in range(1, len(prices))
                if float(prices[idx - 1]) != 0
            ]
            volatility_baseline = max((sum(returns) / len(returns)) if returns else 0.0, 1e-4)
            confidence = min(1.0, ema_gap_pct / volatility_baseline)

            if fast_ema > slow_ema:
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
            elif fast_ema < slow_ema:
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
            else:
                action = "hold"
                confidence = 0.0
                signals.append(
                    Signal(
                        symbol=symbol,
                        action=action,
                        confidence=confidence,
                        price=current_price,
                    )
                )
            logger.debug(
                "[%s] EMA crossover: price=%.4f fast_ema=%.4f slow_ema=%.4f gap_pct=%.6f baseline=%.6f score=%.4f action=%s",
                symbol,
                current_price,
                float(fast_ema),
                float(slow_ema),
                float(ema_gap_pct),
                volatility_baseline,
                confidence,
                action,
            )
        return signals
