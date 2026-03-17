from delta_exchange_bot.execution.order_execution_engine import OrderExecutionEngine


class FakeDeltaClient:
    def __init__(self):
        self.calls = []

    def place_order(self, **kwargs):
        self.calls.append(kwargs)
        return {"success": True, "result": {"id": f"order-{len(self.calls)}"}}


def test_execute_market_order():
    client = FakeDeltaClient()
    engine = OrderExecutionEngine(client)

    out = engine.execute_market_order(symbol="BTCUSD", side="BUY", size=2.0)

    assert out["success"] is True
    assert client.calls[-1]["order_type"] == "market_order"
    assert client.calls[-1]["side"] == "buy"
    assert client.calls[-1]["size"] == 2.0


def test_execute_limit_order():
    client = FakeDeltaClient()
    engine = OrderExecutionEngine(client)

    out = engine.execute_limit_order(symbol="BTCUSD", side="SELL", size=1.5, price=73500.0)

    assert out["success"] is True
    assert client.calls[-1]["order_type"] == "limit_order"
    assert client.calls[-1]["side"] == "sell"
    assert client.calls[-1]["price"] == 73500.0


def test_trailing_stop_long_triggers_exit_market_order():
    client = FakeDeltaClient()
    engine = OrderExecutionEngine(client)

    engine.place_stop_loss("BTCUSD", "long", size=1.0, stop_price=95.0, trade_id="trade-abc")
    engine.set_trailing_stop("BTCUSD", "long", size=1.0, trail_pct=0.01, entry_price=100.0, trade_id="trade-abc")

    assert engine.on_price_update("BTCUSD", 110.0) is None
    triggered = engine.on_price_update("BTCUSD", 108.0)

    assert triggered is not None
    assert triggered["trade_id"] == "trade-abc"
    assert triggered["client_order_id"] == "trade-abc-exit-stop_loss"
    assert triggered["reason"] == "stop_loss"
    assert triggered["exit_side"] == "sell"
    assert client.calls[-1]["order_type"] == "market_order"
    assert client.calls[-1]["reduce_only"] is True


def test_take_profit_short_triggers_exit_market_order():
    client = FakeDeltaClient()
    engine = OrderExecutionEngine(client)

    engine.place_take_profit("ETHUSD", "short", size=2.0, target_price=90.0)
    triggered = engine.on_price_update("ETHUSD", 89.0)

    assert triggered is not None
    assert triggered["reason"] == "take_profit"
    assert triggered["exit_side"] == "buy"
    assert client.calls[-1]["order_type"] == "market_order"
    assert client.calls[-1]["reduce_only"] is True
