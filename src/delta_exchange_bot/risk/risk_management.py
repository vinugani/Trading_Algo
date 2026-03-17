MAX_RISK_PER_TRADE = 0.01
MAX_LEVERAGE = 10.0
MAX_DAILY_LOSS = 0.05


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
