import pandas as pd

from delta_exchange_bot.cli.trading_bot import MainTradingBot
from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.strategy.base import Signal


class FakeExecutionEngine:
    def __init__(self):
        self.calls = []
        self.trigger_next = None

    def execute_limit_order(self, **kwargs):
        self.calls.append(("execute_limit_order", kwargs))
        return {"success": True, "id": "1"}

    def execute_market_order(self, **kwargs):
        self.calls.append(("execute_market_order", kwargs))
        return {"success": True, "id": "2"}

    def place_stop_loss(self, *args, **kwargs):
        self.calls.append(("place_stop_loss", {"args": args, "kwargs": kwargs}))

    def place_take_profit(self, *args, **kwargs):
        self.calls.append(("place_take_profit", {"args": args, "kwargs": kwargs}))

    def set_trailing_stop(self, *args, **kwargs):
        self.calls.append(("set_trailing_stop", {"args": args, "kwargs": kwargs}))

    def on_price_update(self, symbol, current_price):
        self.calls.append(("on_price_update", {"symbol": symbol, "current_price": current_price}))
        out = self.trigger_next
        self.trigger_next = None
        return out


class FakeDB:
    def __init__(self):
        self.rows = []
        self.open_positions = {}

    def save_execution(self, **kwargs):
        existing = {row["execution_id"] for row in self.rows}
        if kwargs["execution_id"] in existing:
            return False
        self.rows.append(kwargs)
        return True

    def load_open_position_state(self, mode=None):
        return {}

    def upsert_open_position_state(self, **kwargs):
        self.open_positions[kwargs["symbol"]] = kwargs

    def remove_open_position_state(self, symbol):
        self.open_positions.pop(symbol, None)


class FakeMetrics:
    def __init__(self):
        self.trade_pnls = []
        self.drawdowns = []
        self.api_latencies = []

    def record_trade(self, pnl):
        self.trade_pnls.append(pnl)

    def set_drawdown(self, drawdown):
        self.drawdowns.append(drawdown)

    def observe_api_latency(self, endpoint, latency):
        self.api_latencies.append((endpoint, latency))

    def start_server(self, *args, **kwargs):
        return None


def _candles() -> pd.DataFrame:
    ts = pd.date_range("2026-03-16 10:00:00+00:00", periods=30, freq="1min")
    closes = [100 + i * 0.1 for i in range(30)]
    return pd.DataFrame(
        {
            "symbol": ["BTCUSD"] * len(ts),
            "timestamp": ts,
            "open": closes,
            "high": [x + 0.2 for x in closes],
            "low": [x - 0.2 for x in closes],
            "close": closes,
            "volume": [1000] * len(ts),
        }
    )


def test_process_symbol_live_runs_full_flow(monkeypatch):
    settings = Settings(mode="live", strategy_name="rsi_scalping", trade_symbols=["BTCUSD"], api_key="k", api_secret="s")
    bot = MainTradingBot(settings)
    bot.execution_engine = FakeExecutionEngine()
    bot.db = FakeDB()
    bot.metrics = FakeMetrics()

    monkeypatch.setattr(bot, "fetch_market_data", lambda symbol: _candles())
    monkeypatch.setattr(
        bot,
        "generate_strategy_signal",
        lambda symbol, candles: Signal(
            symbol=symbol,
            action="buy",
            confidence=0.9,
            price=100.0,
            stop_loss=99.6,
            take_profit=100.8,
            trailing_stop_pct=0.004,
        ),
    )

    bot.process_symbol("BTCUSD")

    call_names = [name for name, _ in bot.execution_engine.calls]
    assert "execute_limit_order" in call_names
    assert "place_stop_loss" in call_names
    assert "place_take_profit" in call_names
    assert "set_trailing_stop" in call_names
    assert "BTCUSD" in bot._open_positions
    assert len(bot.db.rows) == 1
    assert bot.db.rows[0]["event_type"] == "entry"


def test_process_symbol_handles_protection_trigger(monkeypatch):
    settings = Settings(mode="live", strategy_name="rsi_scalping", trade_symbols=["BTCUSD"], api_key="k", api_secret="s")
    bot = MainTradingBot(settings)
    bot.execution_engine = FakeExecutionEngine()
    bot.db = FakeDB()
    bot.metrics = FakeMetrics()
    bot._open_positions["BTCUSD"] = {
        "trade_id": "BTCUSD-trade-1",
        "side": "long",
        "size": 1.0,
        "entry_price": 100.0,
    }
    bot._recalculate_open_notional()

    bot.execution_engine.trigger_next = {
        "trade_id": "BTCUSD-trade-1",
        "symbol": "BTCUSD",
        "reason": "stop_loss",
        "exit_side": "sell",
        "size": 1.0,
        "trigger_price": 99.0,
        "client_order_id": "BTCUSD-trade-1-exit-stop_loss",
        "exchange_order_id": "order-2",
        "order": {"success": True},
    }

    monkeypatch.setattr(bot, "fetch_market_data", lambda symbol: _candles())

    bot.process_symbol("BTCUSD")

    assert "BTCUSD" not in bot._open_positions
    assert len(bot.db.rows) == 1
    assert bot.db.rows[0]["trade_id"] == "BTCUSD-trade-1"
    assert bot.db.rows[0]["event_type"] == "exit"
    assert len(bot.metrics.trade_pnls) == 1
