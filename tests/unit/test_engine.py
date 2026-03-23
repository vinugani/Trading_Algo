import pytest
import pandas as pd
from typing import Optional

from delta_exchange_bot.core.engine import TradingEngine
from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.strategy.rsi_scalping import RSIScalpingStrategy
from delta_exchange_bot.persistence.db import DatabaseManager


class DummyDB:
    def __init__(self):
        self.trades = []

    def create_trade(self, trade_data: dict):
        pass

    def close_trade(self, trade_id: str, exit_price: float):
        pass

    def log_execution(self, exec_data: dict):
        self.trades.append(exec_data)

    def update_position(self, pos_data: dict):
        pass

    def close_position(self, symbol: str):
        pass

    def get_active_position(self, symbol: str):
         return None


def test_fetch_market_snapshot_reads_nested_delta_ticker(monkeypatch):
    settings = Settings(mode="paper", trade_symbols=["BTCUSD"])
    engine = TradingEngine(settings, db=DummyDB())

    monkeypatch.setattr(
        engine.api,
        "get_ticker",
        lambda symbol: {"success": True, "result": {"mark_price": "123.45"}},
    )

    snapshot = engine._fetch_market_snapshot()

    assert snapshot["BTCUSD"]["prices"] == pytest.approx([123.45])


def test_execute_signal_live_places_order_with_delta_client(monkeypatch):
    settings = Settings(mode="live", trade_symbols=["BTCUSD"], api_key="key", api_secret="secret")
    engine = TradingEngine(settings, db=DummyDB())

    orders_placed = []

    def fake_place_order(**kwargs):
        orders_placed.append(kwargs)
        return {"success": True, "result": {"id": f"order-{len(orders_placed)}"}}

    monkeypatch.setattr(engine.api, "place_order", fake_place_order)

    # Risk manager requires stop_loss
    engine._execute_signal(Signal(symbol="BTCUSD", action="buy", confidence=0.9, price=123.0, stop_loss=120.0))

    # Should have entry order + stop loss order
    assert len(orders_placed) >= 1
    entry_order = next(o for o in orders_placed if o["order_type"] == "limit_order")
    assert entry_order["symbol"] == "BTCUSD"
    assert entry_order["side"] == "buy"
    
    assert engine.positions["BTCUSD"]["size"] > 0
    assert len(engine.db.trades) >= 1


def test_execute_signal_paper_uses_local_order_manager(monkeypatch):
    settings = Settings(mode="paper", trade_symbols=["BTCUSD"])
    engine = TradingEngine(settings, db=DummyDB())

    def should_not_call(*args, **kwargs):
        raise AssertionError("DeltaClient.place_order should not be called in paper mode")

    monkeypatch.setattr(engine.api, "place_order", should_not_call)

    # Risk manager requires stop_loss
    engine._execute_signal(Signal(symbol="BTCUSD", action="buy", confidence=0.8, price=120.0, stop_loss=118.0))

    assert len(engine.order_manager.get_open_orders()) == 1
    assert len(engine.db.trades) == 1


def test_process_protection_triggers_updates_position_and_db(monkeypatch):
    settings = Settings(mode="live", trade_symbols=["BTCUSD"], api_key="key", api_secret="secret")
    engine = TradingEngine(settings, db=DummyDB())
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
    engine = TradingEngine(settings, db=DummyDB())
    assert isinstance(engine.strategy, RSIScalpingStrategy)
