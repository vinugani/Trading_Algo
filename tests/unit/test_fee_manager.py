from delta_exchange_bot.execution.fee_manager import FeeConfig
from delta_exchange_bot.execution.fee_manager import FeeManager


def test_get_fee_rate_by_order_type():
    mgr = FeeManager(FeeConfig(maker_fee_rate=0.001, taker_fee_rate=0.002))
    assert mgr.get_fee_rate("limit_order") == 0.001
    assert mgr.get_fee_rate("market_order") == 0.002
    assert mgr.get_fee_rate("market_or_limit_smart") == 0.002


def test_calculate_total_fee():
    mgr = FeeManager(FeeConfig(maker_fee_rate=0.001, taker_fee_rate=0.002))
    trade = {
        "entry_price": 100.0,
        "exit_price": 105.0,
        "size": 2.0,
        "entry_order_type": "limit_order",
        "exit_order_type": "market_order",
    }
    # Entry fee: 100 * 2 * 0.001 = 0.2
    # Exit fee: 105 * 2 * 0.002 = 0.42
    assert mgr.calculate_total_fee(trade) == 0.62
