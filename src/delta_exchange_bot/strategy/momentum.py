import logging

from .base import Signal, Strategy

logger = logging.getLogger(__name__)


class MomentumStrategy(Strategy):
    def __init__(self, window: int = 3, threshold: float = 0.001):
        self.window = window
        self.threshold = threshold

    def generate(self, market_data: dict[str, dict]) -> list[Signal]:
        signals = []
        for symbol, series in market_data.items():
            prices = series.get("prices", [])
            if len(prices) < self.window + 1:
                continue

            recent = prices[-self.window - 1 :]
            prev = recent[:-1]
            current = recent[-1]
            avg_past = sum(prev) / len(prev)
            if avg_past == 0:
                continue

            momentum = (current - avg_past) / avg_past
            threshold = max(abs(float(self.threshold)), 1e-9)
            score = max(0.0, (abs(momentum) - threshold) / threshold)
            confidence = min(1.0, score)

            if momentum > self.threshold:
                action = "buy"
                signals.append(Signal(symbol=symbol, action=action, confidence=confidence, price=current))
            elif momentum < -self.threshold:
                action = "sell"
                signals.append(Signal(symbol=symbol, action=action, confidence=confidence, price=current))
            else:
                action = "hold"
                confidence = 0.0
                signals.append(Signal(symbol=symbol, action=action, confidence=confidence, price=current))

            logger.debug(
                "[%s] Momentum score: current=%.4f avg_past=%.4f momentum=%.6f threshold=%.6f score=%.4f action=%s",
                symbol,
                current,
                avg_past,
                momentum,
                float(self.threshold),
                confidence,
                action,
            )

        return signals
