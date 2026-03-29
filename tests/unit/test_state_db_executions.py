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


def test_schema_migration_adds_missing_columns(tmp_path):
    """Simulate an old DB that has 'positions' without stop_order_id/tp_order_id,
    then verify _apply_schema_migrations adds them so bot startup doesn't crash."""
    import sqlalchemy as sa

    db_path = tmp_path / "test_migration.db"
    db_url = f"sqlite:///{db_path}"
    engine = sa.create_engine(db_url)

    # Create an old-style positions table without the new columns.
    with engine.connect() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY,
                trade_id VARCHAR(64) UNIQUE NOT NULL,
                symbol VARCHAR(32),
                strategy_name VARCHAR(64),
                side VARCHAR(16) NOT NULL,
                size FLOAT NOT NULL,
                entry_price FLOAT,
                exit_price FLOAT,
                entry_time DATETIME,
                exit_time DATETIME,
                pnl_raw FLOAT,
                pnl_pct FLOAT,
                status VARCHAR(16),
                metadata_json JSON
            )
        """))
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS positions (
                symbol VARCHAR(32) PRIMARY KEY,
                trade_id VARCHAR(64) NOT NULL,
                side VARCHAR(16) NOT NULL,
                size FLOAT NOT NULL,
                avg_entry_price FLOAT NOT NULL,
                stop_loss FLOAT,
                take_profit FLOAT,
                liquidation_price FLOAT,
                margin FLOAT,
                updated_at DATETIME
            )
        """))
        conn.commit()
    engine.dispose()

    # DatabaseManager startup should apply the migration without crashing.
    db = DatabaseManager(db_url)

    # Verify the columns now exist by querying get_all_active_positions.
    results = db.get_all_active_positions()
    assert isinstance(results, list)  # no UndefinedColumn error

    # Verify columns are queryable directly.
    with engine.connect() as conn:
        row = conn.execute(sa.text(
            "SELECT stop_order_id, tp_order_id FROM positions LIMIT 1"
        )).fetchone()
        # No exception means columns exist (row is None since table is empty).
        assert row is None


def test_schema_migration_is_idempotent(tmp_path):
    """Running _apply_schema_migrations twice must not raise."""
    db = DatabaseManager("sqlite:///:memory:")
    db._apply_schema_migrations()  # second call — columns already exist
    # No exception means duplicate-column handling works.
    assert True
