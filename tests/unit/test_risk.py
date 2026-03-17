from delta_exchange_bot.risk.risk_manager import RiskManager


def test_risk_manager_position_limit():
    risk = RiskManager(max_positions=2, max_drawdown_pct=0.1)
    assert risk.check_position_limit(1)
    assert not risk.check_position_limit(2)


def test_risk_manager_drawdown_check():
    risk = RiskManager(max_positions=5, max_drawdown_pct=0.1)
    assert risk.check_drawdown(95000.0) is True
    assert risk.check_drawdown(85000.0) is False
