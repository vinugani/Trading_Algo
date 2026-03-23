import pytest
from delta_exchange_bot.persistence.db import DatabaseManager

def test_trade_record_lifecycle(tmp_path):
    # Use in-memory DB for testing
    db = DatabaseManager("sqlite:///:memory:")

    trade_id = "test-trade-001"

    # 1. Open Trade
    db.create_trade({
        "trade_id": trade_id,
        "symbol": "BTCUSD",
        "side": "long",
        "size": 0.1,
        "entry_price": 50000.0,
        "strategy_name": "rsi_scalping"
    })

    records = db.get_trade_records()
    assert len(records) == 1
    assert records[0]["trade_id"] == trade_id
    assert records[0]["status"] == "open"

    # 2. Close Trade
    db.close_trade(trade_id, exit_price=51000.0)

    records = db.get_trade_records()
    assert records[0]["status"] == "closed"
    assert records[0]["pnl_raw"] == pytest.approx(100.0) # (51000-50000)*0.1

def test_trade_record_short_pnl(tmp_path):
    db = DatabaseManager("sqlite:///:memory:")

    trade_id = "short-001"
    db.create_trade({
        "trade_id": trade_id,
        "symbol": "ETHUSD",
        "side": "short",
        "size": 1.0,
        "entry_price": 3000.0
    })

    db.close_trade(trade_id, exit_price=2900.0)

    records = db.get_trade_records()
    assert records[0]["pnl_raw"] == pytest.approx(100.0) # (3000-2900)*1.0
