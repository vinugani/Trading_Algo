from .base import Signal, Strategy


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

            if momentum > self.threshold:
                signals.append(Signal(symbol=symbol, action="buy", confidence=momentum, price=current))
            elif momentum < -self.threshold:
                signals.append(Signal(symbol=symbol, action="sell", confidence=-momentum, price=current))
            else:
                signals.append(Signal(symbol=symbol, action="hold", confidence=0.0, price=current))

        return signals
