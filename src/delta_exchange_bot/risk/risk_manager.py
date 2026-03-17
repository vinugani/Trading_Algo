from typing import List


class RiskManager:
    def __init__(self, max_positions: int = 5, max_drawdown_pct: float = 0.1):
        self.max_positions = max_positions
        self.max_drawdown_pct = max_drawdown_pct
        self.equity_peak = 100000.0

    def check_position_limit(self, current_positions: int) -> bool:
        return current_positions < self.max_positions

    def check_drawdown(self, current_equity: float) -> bool:
        drawdown = (self.equity_peak - current_equity) / self.equity_peak
        return drawdown < self.max_drawdown_pct

    def assess(self, current_positions: int, current_equity: float) -> bool:
        if not self.check_position_limit(current_positions):
            return False
        if not self.check_drawdown(current_equity):
            return False
        return True
