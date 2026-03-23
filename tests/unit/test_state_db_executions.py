from delta_exchange_bot.persistence.db import DatabaseManager


def test_log_execution_idempotency(tmp_path):
    db = DatabaseManager("sqlite:///:memory:")

    exec_data = {
        "trade_id": "trade-1",
        "execution_id": "trade-1:entry",
        "symbol": "BTCUSD",
        "side": "buy",
        "size": 1.0,
        "price": 100.0,
        "event_type": "entry",
        "status": "filled",
    }
    
    db.log_execution(exec_data)
    # Logging same execution_id should not crash (it's handled by try-except in log_execution)
    db.log_execution(exec_data)

    history = db.get_execution_history()
    assert len(history) == 1


def test_get_execution_history(tmp_path):
    db = DatabaseManager("sqlite:///:memory:")

    db.log_execution({
        "trade_id": "trade-2",
        "execution_id": "trade-2:entry",
        "symbol": "ETHUSD",
        "side": "buy",
        "size": 2.0,
        "price": 200.0,
        "event_type": "entry",
        "status": "submitted",
    })
    db.log_execution({
        "trade_id": "trade-2",
        "execution_id": "trade-2:exit",
        "symbol": "ETHUSD",
        "side": "sell",
        "size": 2.0,
        "price": 201.0,
        "event_type": "exit",
        "status": "filled",
        "reason": "take_profit",
    })

    rows = db.get_execution_history()
    assert len(rows) == 2
    assert rows[0]["event_type"] == "exit"
    assert rows[1]["event_type"] == "entry"


def test_position_state_roundtrip(tmp_path):
    db = DatabaseManager("sqlite:///:memory:")
    db.update_position({
        "symbol": "BTCUSD",
        "trade_id": "trade-3",
        "side": "long",
        "size": 1.5,
        "avg_entry_price": 101.0,
        "stop_loss": 100.0,
        "take_profit": 103.0,
    })

    pos = db.get_active_position("BTCUSD")
    assert pos is not None
    assert pos["trade_id"] == "trade-3"

    db.close_position("BTCUSD")
    pos_after = db.get_active_position("BTCUSD")
    assert pos_after is None
