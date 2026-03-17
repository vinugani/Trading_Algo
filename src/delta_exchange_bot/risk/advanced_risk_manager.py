from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AdvancedRiskConfig:
    max_risk_per_trade: float = 0.01
    max_daily_loss: float = 0.05
    max_leverage: float = 10.0
    max_asset_exposure: float = 0.25  # fraction of account equity
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
        # Higher volatility => lower leverage.
        lev = self.config.base_leverage / (1.0 + (atr_pct * 100.0))
        lev = max(self.config.min_leverage, min(self.config.max_leverage, lev))
        return lev

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
