from delta_exchange_bot.persistence.db import DatabaseManager


def test_save_signal_accepts_keyword_arguments():
    db = DatabaseManager("sqlite:///:memory:")

    db.save_signal(
        signal_id="sig-1",
        strategy_name="portfolio",
        regime="ranging",
        symbol="BTCUSD",
        action="buy",
        confidence=0.75,
        price=100.0,
        stop_loss=99.0,
        take_profit=102.0,
        metadata={"source": "test"},
    )

    rows = db.get_signals_history(limit=10)
    assert len(rows) == 1
    assert rows[0]["signal_id"] == "sig-1"
