from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any
from typing import Optional


@dataclass
class Signal:
    symbol: str
    action: str  # 'buy'/'sell'/'hold'
    confidence: float
    price: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_stop_pct: Optional[float] = None


class Strategy(ABC):
    @abstractmethod
    def generate(self, market_data: dict[str, Any]) -> list[Signal]:
        """Generate signals based on market data."""
        raise NotImplementedError
