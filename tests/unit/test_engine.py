import pytest

from delta_exchange_bot.core.engine import TradingEngine
from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.strategy.rsi_scalping import RSIScalpingStrategy


class DummyDB:
    def __init__(self):
        self.trades = []

    def save_trade(self, symbol: str, side: str, size: float, price: float):
        self.trades.append(
            {
                "symbol": symbol,
                "side": side,
                "size": size,
                "price": price,
            }
        )

    def upsert_trade_record(self, **kwargs):
        pass

    def close_trade_record(self, **kwargs):
        pass


def test_fetch_market_snapshot_reads_nested_delta_ticker(monkeypatch):
    settings = Settings(mode="paper", trade_symbols=["BTCUSD"])
    engine = TradingEngine(settings)

    monkeypatch.setattr(
        engine.api,
        "get_ticker",
        lambda symbol: {"success": True, "result": {"mark_price": "123.45"}},
    )

    snapshot = engine._fetch_market_snapshot()

    assert snapshot["BTCUSD"]["prices"] == pytest.approx([123.45])


def test_execute_signal_live_places_order_with_delta_client(monkeypatch):
    settings = Settings(mode="live", trade_symbols=["BTCUSD"], api_key="key", api_secret="secret")
    engine = TradingEngine(settings)
    engine.db = DummyDB()

    captured = {}

    def fake_place_order(**kwargs):
        captured.update(
            {
                "symbol": kwargs["symbol"],
                "side": kwargs["side"],
                "size": kwargs["size"],
                "price": kwargs.get("price"),
                "order_type": kwargs["order_type"],
            }
        )
        return {"success": True, "result": {"id": "order-1"}}

    monkeypatch.setattr(engine.api, "place_order", fake_place_order)

    engine._execute_signal(Signal(symbol="BTCUSD", action="buy", confidence=0.9, price=123.0))

    assert captured["symbol"] == "BTCUSD"
    assert captured["side"] == "buy"
    assert captured["order_type"] == "limit_order"
    assert engine.positions["BTCUSD"]["size"] > 0
    assert len(engine.db.trades) == 1


def test_execute_signal_paper_uses_local_order_manager(monkeypatch):
    settings = Settings(mode="paper", trade_symbols=["BTCUSD"])
    engine = TradingEngine(settings)
    engine.db = DummyDB()

    def should_not_call(*args, **kwargs):
        raise AssertionError("DeltaClient.place_order should not be called in paper mode")

    monkeypatch.setattr(engine.api, "place_order", should_not_call)

    engine._execute_signal(Signal(symbol="BTCUSD", action="buy", confidence=0.8, price=120.0))

    assert len(engine.order_manager.get_open_orders()) == 1
    assert len(engine.db.trades) == 1


def test_process_protection_triggers_updates_position_and_db(monkeypatch):
    settings = Settings(mode="live", trade_symbols=["BTCUSD"], api_key="key", api_secret="secret")
    engine = TradingEngine(settings)
    engine.db = DummyDB()
    engine.positions["BTCUSD"] = {"size": 2.0, "side": "long", "entry_time": 1000.0, "entry_price": 100.0, "trade_id": "test-trade"}

    monkeypatch.setattr(
        engine.execution_engine,
        "on_price_update",
        lambda symbol, current_price: {
            "symbol": symbol,
            "reason": "stop_loss",
            "exit_side": "sell",
            "size": 2.0,
            "trigger_price": current_price,
            "order": {"success": True},
        },
    )

    market_data = {"BTCUSD": {"prices": [100.0], "ticker": {}}}
    engine._process_protection_triggers(market_data)

    assert "BTCUSD" not in engine.positions
    assert len(engine.db.trades) == 1
    assert engine.db.trades[0]["side"] == "sell"


def test_engine_uses_rsi_strategy_when_configured():
    settings = Settings(mode="paper", strategy_name="rsi_scalping", trade_symbols=["BTCUSD"])
    engine = TradingEngine(settings)
    assert isinstance(engine.strategy, RSIScalpingStrategy)
