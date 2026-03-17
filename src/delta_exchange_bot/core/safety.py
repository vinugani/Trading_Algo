from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    cooldown_seconds: int = 60


class APICircuitBreaker:
    def __init__(self, config: CircuitBreakerConfig | None = None):
        self.config = config or CircuitBreakerConfig()
        self._failures = 0
        self._opened_at: float | None = None

    def record_success(self) -> None:
        self._failures = 0
        self._opened_at = None

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self.config.failure_threshold and self._opened_at is None:
            self._opened_at = time.time()

    def is_open(self) -> bool:
        if self._opened_at is None:
            return False
        elapsed = time.time() - self._opened_at
        if elapsed >= self.config.cooldown_seconds:
            self._opened_at = None
            self._failures = 0
            return False
        return True


class SafetyController:
    def __init__(self, breaker: APICircuitBreaker | None = None, daily_loss_limit: float = 0.05):
        self.breaker = breaker or APICircuitBreaker()
        self.daily_loss_limit = daily_loss_limit

    def can_trade(self) -> bool:
        return not self.breaker.is_open()

    def check_daily_loss_kill_switch(self, current_equity: float, start_of_day_equity: float) -> bool:
        if start_of_day_equity <= 0:
            return False
        drawdown = max(0.0, (start_of_day_equity - current_equity) / start_of_day_equity)
        return drawdown >= self.daily_loss_limit

    @staticmethod
    def detect_position_mismatch(local_size: float, exchange_size: float, tolerance: float = 1e-8) -> bool:
        return abs(float(local_size) - float(exchange_size)) > tolerance

    @staticmethod
    def should_auto_cancel_orders_if_flat(position_size: float, tolerance: float = 1e-8) -> bool:
        return abs(float(position_size)) <= tolerance
