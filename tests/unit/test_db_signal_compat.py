from delta_exchange_bot.persistence.db import DatabaseManager
from delta_exchange_bot.persistence.models import Order


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


def test_save_order_record_accepts_keyword_arguments():
    db = DatabaseManager("sqlite:///:memory:")

    db.save_order_record(
        client_order_id="client-1",
        order_id="order-1",
        trade_id="trade-1",
        symbol="BTCUSD",
        side="sell",
        order_type="paper_limit",
        size=0.5,
        price=70000.0,
        status="submitted",
        metadata={"source": "test"},
    )

    with db.get_session() as session:
        rows = session.query(Order).all()
        assert len(rows) == 1
        assert rows[0].client_order_id == "client-1"
