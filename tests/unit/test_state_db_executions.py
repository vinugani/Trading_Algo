from delta_exchange_bot.persistence.db import StateDB


def test_save_execution_enforces_unique_execution_id(tmp_path):
    db = StateDB(str(tmp_path / "state.db"))

    first = db.save_execution(
        trade_id="trade-1",
        execution_id="trade-1:entry",
        symbol="BTCUSD",
        side="buy",
        size=1.0,
        price=100.0,
        event_type="entry",
        order_type="limit_order",
        mode="paper",
        status="filled",
    )
    duplicate = db.save_execution(
        trade_id="trade-1",
        execution_id="trade-1:entry",
        symbol="BTCUSD",
        side="buy",
        size=1.0,
        price=100.0,
        event_type="entry",
        order_type="limit_order",
        mode="paper",
        status="filled",
    )

    assert first is True
    assert duplicate is False


def test_get_executions_by_trade_id_returns_full_lifecycle(tmp_path):
    db = StateDB(str(tmp_path / "state.db"))

    db.save_execution(
        trade_id="trade-2",
        execution_id="trade-2:entry",
        symbol="ETHUSD",
        side="buy",
        size=2.0,
        price=200.0,
        event_type="entry",
        order_type="limit_order",
        mode="live",
        status="submitted",
        client_order_id="trade-2-entry",
        exchange_order_id="o-1",
    )
    db.save_execution(
        trade_id="trade-2",
        execution_id="trade-2:exit",
        symbol="ETHUSD",
        side="sell",
        size=2.0,
        price=201.0,
        event_type="exit",
        order_type="market_order",
        mode="live",
        status="filled",
        client_order_id="trade-2-exit-stop_loss",
        exchange_order_id="o-2",
        reason="take_profit",
    )

    rows = db.get_executions_by_trade_id("trade-2")
    assert len(rows) == 2
    assert rows[0]["trade_id"] == "trade-2"
    assert rows[1]["trade_id"] == "trade-2"
    assert rows[0]["event_type"] == "entry"
    assert rows[1]["event_type"] == "exit"


def test_open_position_state_roundtrip(tmp_path):
    db = StateDB(str(tmp_path / "state.db"))
    db.upsert_open_position_state(
        symbol="BTCUSD",
        trade_id="trade-3",
        side="long",
        size=1.5,
        entry_price=101.0,
        stop_loss=100.0,
        take_profit=103.0,
        trailing_stop_pct=0.004,
        mode="live",
    )

    loaded = db.load_open_position_state(mode="live")
    assert "BTCUSD" in loaded
    assert loaded["BTCUSD"]["trade_id"] == "trade-3"

    db.remove_open_position_state("BTCUSD")
    loaded_after = db.load_open_position_state(mode="live")
    assert "BTCUSD" not in loaded_after
