from delta_exchange_bot.risk.risk_manager import RiskManager


def test_risk_manager_assess_signal_position_limit():
    risk = RiskManager(max_positions=2, min_confidence=0.5)
    
    # 1. First position allowed
    res1 = risk.assess_signal({"price": 100, "stop_loss": 90, "confidence": 0.8}, current_positions=0, balance=10000)
    assert res1["allowed"] is True
    
    # 2. Second position allowed
    res2 = risk.assess_signal({"price": 100, "stop_loss": 90, "confidence": 0.8}, current_positions=1, balance=10000)
    assert res2["allowed"] is True
    
    # 3. Third position blocked
    res3 = risk.assess_signal({"price": 100, "stop_loss": 90, "confidence": 0.8}, current_positions=2, balance=10000)
    assert res3["allowed"] is False
    assert "Max positions reached" in res3["reason"]


def test_risk_manager_kill_switch():
    risk = RiskManager(max_daily_loss_pct=0.1)
    risk.set_daily_baseline(100000.0)
    
    # 1. Small loss allowed
    res1 = risk.assess_signal({"price": 100, "stop_loss": 90, "confidence": 0.8}, current_positions=0, balance=95000)
    assert res1["allowed"] is True
    
    # 2. Large loss blocked
    res2 = risk.assess_signal({"price": 100, "stop_loss": 90, "confidence": 0.8}, current_positions=0, balance=85000)
    assert res2["allowed"] is False
    assert "Daily loss limit hit" in res2["reason"]
