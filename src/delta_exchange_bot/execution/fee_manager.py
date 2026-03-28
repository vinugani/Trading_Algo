from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class FeeConfig:
    maker_fee_rate: float = 0.0002
    taker_fee_rate: float = 0.0005


class FeeManager:
    def __init__(self, config: FeeConfig | None = None):
        self.config = config or FeeConfig()

    def get_fee_rate(self, order_type: str) -> float:
        normalized = str(order_type or "").lower()
        if "limit" in normalized and "market" not in normalized:
            return max(0.0, float(self.config.maker_fee_rate))
        return max(0.0, float(self.config.taker_fee_rate))

    def calculate_entry_fee(self, price: float, size: float, order_type: str = "market_order") -> float:
        return max(0.0, float(price)) * max(0.0, float(size)) * self.get_fee_rate(order_type)

    def calculate_exit_fee(self, price: float, size: float, order_type: str = "market_order") -> float:
        return max(0.0, float(price)) * max(0.0, float(size)) * self.get_fee_rate(order_type)

    def calculate_funding_cost(
        self, notional: float, funding_rate: float, holding_seconds: float
    ) -> float:
        """Funding is charged every 8 hours on perpetuals.

        cost = notional × |rate| × (seconds_held / 28800)
        Always positive — represents a cost regardless of long/short,
        because at extreme rates the position pays (not receives) funding.
        """
        if notional <= 0 or holding_seconds <= 0:
            return 0.0
        periods = holding_seconds / 28_800.0  # 8h = 28,800s
        return abs(notional) * abs(funding_rate) * periods

    def calculate_total_fee(self, trade: dict[str, Any]) -> float:
        """Return total all-in cost: entry fee + exit fee + funding cost.

        Funding cost is included when the trade dict contains both
        ``funding_rate`` (float, per-8h rate from ticker) and
        ``holding_seconds`` (float, seconds the position was open).
        Both fields are optional; if absent funding cost is 0.
        """
        entry_price = float(trade.get("entry_price", 0.0) or 0.0)
        exit_price = float(trade.get("exit_price", 0.0) or 0.0)
        size = float(trade.get("size", 0.0) or 0.0)
        entry_type = str(trade.get("entry_order_type", "market_order"))
        exit_type = str(trade.get("exit_order_type", "market_order"))
        trading_fees = self.calculate_entry_fee(entry_price, size, entry_type) + self.calculate_exit_fee(exit_price, size, exit_type)

        funding_rate = trade.get("funding_rate")
        holding_seconds = trade.get("holding_seconds")
        if funding_rate is not None and holding_seconds is not None:
            notional = entry_price * size
            funding_fees = self.calculate_funding_cost(notional, float(funding_rate), float(holding_seconds))
        else:
            funding_fees = 0.0

        return trading_fees + funding_fees
