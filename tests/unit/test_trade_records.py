import pytest
from delta_exchange_bot.persistence.db import StateDB
import os

def test_trade_record_lifecycle(tmp_path):
    # Use a temporary DB for testing
    db_path = str(tmp_path / "test_trade.db")
    db = StateDB(db_path)
    
    trade_id = "test-trade-001"
    
    # 1. Open Trade
    db.upsert_trade_record(
        trade_id=trade_id,
        symbol="BTCUSD",
        side="long",
        size=0.1,
        entry_price=50000.0,
        strategy_name="rsi_scalping"
    )
    
    records = db.get_trade_records(limit=1)
    assert len(records) == 1
    assert records[0]["trade_id"] == trade_id
    assert records[0]["status"] == "open"
    assert records[0]["entry_price"] == 50000.0
    
    # 2. Close Trade
    db.close_trade_record(
        trade_id=trade_id,
        exit_price=51000.0
    )
    
    records = db.get_trade_records(limit=1)
    assert records[0]["status"] == "closed"
    assert records[0]["exit_price"] == 51000.0
    assert records[0]["pnl_raw"] == pytest.approx(100.0) # (51000 - 50000) * 0.1
    assert records[0]["pnl_pct"] == pytest.approx(2.0)   # 100 / (50000 * 0.1) * 100
    assert records[0]["duration_s"] is not None

def test_trade_record_short_pnl(tmp_path):
    db_path = str(tmp_path / "test_short.db")
    db = StateDB(db_path)
    
    trade_id = "short-001"
    db.upsert_trade_record(
        trade_id=trade_id,
        symbol="ETHUSD",
        side="short",
        size=1.0,
        entry_price=3000.0
    )
    
    # Position drops (Profit for short)
    db.close_trade_record(trade_id=trade_id, exit_price=2900.0)
    
    records = db.get_trade_records(limit=1)
    assert records[0]["pnl_raw"] == pytest.approx(100.0)
    assert records[0]["pnl_pct"] == pytest.approx(3.3333333)
