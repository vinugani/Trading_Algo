from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# ── Module-level constants (re-exported for backward compat) ──────────────────
MAX_RISK_PER_TRADE: float = 0.01
MAX_LEVERAGE: float = 10.0
MAX_DAILY_LOSS: float = 0.05


# ── Standalone helpers (previously in risk_management.py) ────────────────────

def calculate_position_size(
    account_equity: float,
    entry_price: float,
    stop_loss_price: float,
    current_open_notional: float = 0.0,
    max_risk_per_trade: float = MAX_RISK_PER_TRADE,
    max_leverage: float = MAX_LEVERAGE,
) -> float:
    """Return position size (units) capped by risk-per-trade and leverage."""
    if account_equity <= 0 or entry_price <= 0:
        return 0.0
    risk_per_unit = abs(entry_price - stop_loss_price)
    if risk_per_unit <= 0:
        return 0.0
    risk_budget = account_equity * max_risk_per_trade
    size_by_risk = risk_budget / risk_per_unit
    leverage_notional_cap = account_equity * max_leverage
    remaining_notional = max(0.0, leverage_notional_cap - max(0.0, current_open_notional))
    size_by_leverage = remaining_notional / entry_price
    return max(0.0, min(size_by_risk, size_by_leverage))


def validate_trade(
    account_equity: float,
    start_of_day_equity: float,
    entry_price: float,
    stop_loss_price: float,
    position_size: float,
    current_open_notional: float = 0.0,
    max_risk_per_trade: float = MAX_RISK_PER_TRADE,
    max_leverage: float = MAX_LEVERAGE,
    max_daily_loss: float = MAX_DAILY_LOSS,
) -> bool:
    """Validate trade against max risk/trade, max leverage, and max daily loss."""
    if account_equity <= 0 or start_of_day_equity <= 0 or entry_price <= 0 or position_size <= 0:
        return False
    daily_loss_pct = max(0.0, (start_of_day_equity - account_equity) / start_of_day_equity)
    if daily_loss_pct > max_daily_loss:
        return False
    risk_amount = abs(entry_price - stop_loss_price) * abs(position_size)
    if risk_amount > account_equity * max_risk_per_trade:
        return False
    projected_notional = abs(position_size) * entry_price + max(0.0, current_open_notional)
    projected_leverage = projected_notional / account_equity
    if projected_leverage > max_leverage:
        return False
    return True


# ── RiskManager (previously in risk_manager.py) ───────────────────────────────

class RiskManager:
    """
    Signal-level risk gate: confidence, daily kill-switch, position cap, SL presence.
    Used by TradingEngine for per-signal go/no-go decisions.
    """

    def __init__(
        self,
        max_positions: int = 3,
        max_risk_per_trade: float = 0.02,
        max_daily_loss_pct: float = 0.05,
        min_confidence: float = 0.6,
    ):
        self.max_positions = max_positions
        self.max_risk_per_trade = max_risk_per_trade
        self.max_daily_loss_pct = max_daily_loss_pct
        self.min_confidence = min_confidence
        self.daily_pnl = 0.0
        self.starting_balance = 0.0

    def set_daily_baseline(self, balance: float) -> None:
        self.starting_balance = balance
        self.daily_pnl = 0.0

    def calculate_position_size(
        self,
        balance: float,
        entry_price: float,
        stop_loss: float,
        risk_pct: Optional[float] = None,
    ) -> float:
        if abs(entry_price - stop_loss) < 1e-8:
            return 0.0
        risk_allowance = (risk_pct or self.max_risk_per_trade) * balance
        risk_per_unit = abs(entry_price - stop_loss)
        size = risk_allowance / risk_per_unit
        max_notional = balance * 0.5
        if (size * entry_price) > max_notional:
            size = max_notional / entry_price
        return round(size, 8)

    def check_kill_switch(self, current_balance: float) -> bool:
        if self.starting_balance <= 0:
            return False
        total_loss_pct = (self.starting_balance - current_balance) / self.starting_balance
        if total_loss_pct >= self.max_daily_loss_pct:
            logger.critical(
                "KILL SWITCH TRIGGERED: Daily loss %.2f%% hit limit %.2f%%",
                total_loss_pct * 100,
                self.max_daily_loss_pct * 100,
            )
            return True
        return False

    def assess_signal(
        self,
        signal: dict,
        current_positions: int,
        balance: float,
    ) -> dict:
        status: dict = {"allowed": False, "reason": "", "size": 0.0}

        if signal.get("confidence", 0) < self.min_confidence:
            status["reason"] = f"Low confidence: {signal.get('confidence')}"
            return status

        if self.check_kill_switch(balance):
            status["reason"] = "Daily loss limit hit"
            return status

        if current_positions >= self.max_positions:
            status["reason"] = f"Max positions reached: {current_positions}"
            return status

        if not signal.get("stop_loss"):
            status["reason"] = "Mandatory Stop-Loss missing"
            return status

        size = self.calculate_position_size(
            balance=balance,
            entry_price=signal["price"],
            stop_loss=signal["stop_loss"],
        )
        if size <= 0:
            status["reason"] = "Calculated size is zero"
            return status

        status["allowed"] = True
        status["size"] = size
        return status


# ── AdvancedRiskManager ───────────────────────────────────────────────────────

@dataclass
class AdvancedRiskConfig:
    max_risk_per_trade: float = 0.01
    max_daily_loss: float = 0.05
    max_leverage: float = 10.0
    max_asset_exposure: float = 0.25
    atr_risk_multiplier: float = 1.5
    base_leverage: float = 5.0
    min_leverage: float = 1.0


class AdvancedRiskManager:
    def __init__(self, config: AdvancedRiskConfig | None = None):
        self.config = config or AdvancedRiskConfig()
        self._daily_realized_pnl = 0.0

    def reset_daily_pnl(self) -> None:
        self._daily_realized_pnl = 0.0

    def register_realized_pnl(self, pnl: float) -> None:
        self._daily_realized_pnl += float(pnl)

    def current_daily_loss_pct(self, start_of_day_equity: float) -> float:
        if start_of_day_equity <= 0:
            return 0.0
        loss = max(0.0, -self._daily_realized_pnl)
        return loss / start_of_day_equity

    def daily_kill_switch_triggered(self, start_of_day_equity: float) -> bool:
        return self.current_daily_loss_pct(start_of_day_equity) >= self.config.max_daily_loss

    def adjust_leverage(self, atr_pct: float) -> float:
        if atr_pct <= 0:
            return self.config.base_leverage
        lev = self.config.base_leverage / (1.0 + (atr_pct * 100.0))
        return max(self.config.min_leverage, min(self.config.max_leverage, lev))

    def dynamic_position_size(
        self,
        *,
        account_equity: float,
        entry_price: float,
        atr: float,
        signal_confidence: float,
        current_asset_notional: float,
    ) -> float:
        if account_equity <= 0 or entry_price <= 0:
            return 0.0
        confidence = max(0.0, min(1.0, float(signal_confidence)))
        atr = max(0.0, float(atr))
        if atr == 0:
            atr = entry_price * 0.003
        risk_budget = account_equity * self.config.max_risk_per_trade * (0.5 + confidence / 2.0)
        risk_per_unit = atr * self.config.atr_risk_multiplier
        size_by_risk = risk_budget / risk_per_unit if risk_per_unit > 0 else 0.0
        max_asset_notional = account_equity * self.config.max_asset_exposure
        remaining_asset_notional = max(0.0, max_asset_notional - max(0.0, current_asset_notional))
        size_by_asset_exposure = remaining_asset_notional / entry_price
        leverage = self.adjust_leverage(atr / entry_price if entry_price > 0 else 0.0)
        size_by_leverage = (account_equity * leverage) / entry_price
        return max(0.0, min(size_by_risk, size_by_asset_exposure, size_by_leverage))

    def validate_trade(
        self,
        *,
        account_equity: float,
        start_of_day_equity: float,
        asset_notional_after_trade: float,
        total_notional_after_trade: float,
        leverage_after_trade: float,
    ) -> bool:
        if account_equity <= 0:
            return False
        if self.daily_kill_switch_triggered(start_of_day_equity):
            return False
        if asset_notional_after_trade > account_equity * self.config.max_asset_exposure:
            return False
        if leverage_after_trade > self.config.max_leverage:
            return False
        if total_notional_after_trade > account_equity * self.config.max_leverage:
            return False
        return True
