import pytest

from delta_exchange_bot.risk.risk_management import calculate_position_size, validate_trade


def test_calculate_position_size_by_risk():
    size = calculate_position_size(
        account_equity=10000.0,
        entry_price=100.0,
        stop_loss_price=99.0,
    )
    assert size == pytest.approx(100.0)


def test_calculate_position_size_capped_by_leverage():
    size = calculate_position_size(
        account_equity=1000.0,
        entry_price=1000.0,
        stop_loss_price=999.9,
    )
    assert size == pytest.approx(10.0)


def test_validate_trade_passes_within_limits():
    ok = validate_trade(
        account_equity=10000.0,
        start_of_day_equity=10000.0,
        entry_price=100.0,
        stop_loss_price=99.0,
        position_size=50.0,
        current_open_notional=20000.0,
    )
    assert ok is True


def test_validate_trade_blocks_risk_breach():
    ok = validate_trade(
        account_equity=10000.0,
        start_of_day_equity=10000.0,
        entry_price=100.0,
        stop_loss_price=99.0,
        position_size=200.0,
    )
    assert ok is False


def test_validate_trade_blocks_daily_loss_breach():
    ok = validate_trade(
        account_equity=9400.0,
        start_of_day_equity=10000.0,
        entry_price=100.0,
        stop_loss_price=99.0,
        position_size=50.0,
    )
    assert ok is False


def test_validate_trade_blocks_leverage_breach():
    ok = validate_trade(
        account_equity=10000.0,
        start_of_day_equity=10000.0,
        entry_price=100.0,
        stop_loss_price=99.0,
        position_size=100.0,
        current_open_notional=95000.0,
    )
    assert ok is False
