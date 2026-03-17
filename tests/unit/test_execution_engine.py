import pytest

from delta_exchange_bot.api.delta_client import DeltaAPIError
from delta_exchange_bot.execution.order_execution_engine import OrderExecutionEngine


class FakeClient:
    def __init__(self):
        self.calls = []

    def place_order(self, **kwargs):
        self.calls.append(kwargs)
        return {"success": True, "result": {"id": f"order-{len(self.calls)}", "status": "filled"}}


def test_smart_order_routes_market_when_spread_small():
    client = FakeClient()
    engine = OrderExecutionEngine(client)

    responses = engine.execute_smart_order(
        symbol="BTCUSD",
        side="buy",
        size=2.0,
        reference_price=100.0,
        best_bid=99.99,
        best_ask=100.01,
        spread_threshold_pct=0.001,
        max_slippage_pct=0.01,
        chunk_size=0.0,
    )

    assert len(responses) == 1
    assert client.calls[0]["order_type"] == "market_order"


def test_smart_order_routes_limit_and_chunks_when_spread_large():
    client = FakeClient()
    engine = OrderExecutionEngine(client)

    responses = engine.execute_smart_order(
        symbol="BTCUSD",
        side="sell",
        size=5.0,
        reference_price=100.0,
        best_bid=99.0,
        best_ask=101.0,
        spread_threshold_pct=0.001,
        max_slippage_pct=0.02,
        chunk_size=2.0,
    )

    assert len(responses) == 3
    assert all(call["order_type"] == "limit_order" for call in client.calls)


def test_smart_order_rejects_on_slippage():
    client = FakeClient()
    engine = OrderExecutionEngine(client)

    with pytest.raises(DeltaAPIError):
        engine.execute_smart_order(
            symbol="BTCUSD",
            side="buy",
            size=1.0,
            reference_price=100.0,
            best_bid=90.0,
            best_ask=110.0,
            spread_threshold_pct=0.5,
            max_slippage_pct=0.01,
            chunk_size=0.0,
        )
