import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "analyze_paper_trades.py"
SPEC = importlib.util.spec_from_file_location("analyze_paper_trades_script", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_compute_closed_trade_metrics():
    trades = [
        MODULE.TradeRow(
            trade_id="t1",
            symbol="BTCUSD",
            side="long",
            size=1.0,
            entry_price=100.0,
            exit_price=110.0,
            pnl_raw=10.0,
            pnl_pct=10.0,
            status="closed",
            strategy_name="s1",
            entry_time=datetime(2026, 3, 20, tzinfo=timezone.utc),
            exit_time=datetime(2026, 3, 20, 1, tzinfo=timezone.utc),
        ),
        MODULE.TradeRow(
            trade_id="t2",
            symbol="ETHUSD",
            side="short",
            size=2.0,
            entry_price=50.0,
            exit_price=55.0,
            pnl_raw=-10.0,
            pnl_pct=-10.0,
            status="closed",
            strategy_name="s1",
            entry_time=datetime(2026, 3, 21, tzinfo=timezone.utc),
            exit_time=datetime(2026, 3, 21, 1, tzinfo=timezone.utc),
        ),
    ]

    metrics = MODULE._compute_closed_trade_metrics(trades, initial_equity=1000.0)

    assert metrics["total_closed_trades"] == 2.0
    assert metrics["wins"] == 1.0
    assert metrics["losses"] == 1.0
    assert metrics["realized_pnl"] == 0.0
    assert metrics["profit_factor"] == 1.0
    assert metrics["max_drawdown"] == 10.0


def test_build_symbol_summary_includes_open_positions():
    closed_trades = [
        MODULE.TradeRow(
            trade_id="t1",
            symbol="BTCUSD",
            side="long",
            size=1.0,
            entry_price=100.0,
            exit_price=110.0,
            pnl_raw=10.0,
            pnl_pct=10.0,
            status="closed",
            strategy_name="s1",
            entry_time=datetime(2026, 3, 20, tzinfo=timezone.utc),
            exit_time=datetime(2026, 3, 20, 1, tzinfo=timezone.utc),
        )
    ]
    open_positions = [
        {
            "symbol": "BTCUSD",
            "trade_id": "open-btc",
            "side": "long",
            "size": 1.0,
            "avg_entry_price": 105.0,
            "mark_price": 106.0,
            "notional": 105.0,
            "unrealized_pnl": 1.0,
            "unrealized_pnl_pct": 0.95,
        },
        {
            "symbol": "ETHUSD",
            "trade_id": "open-eth",
            "side": "short",
            "size": 1.0,
            "avg_entry_price": 50.0,
            "mark_price": 49.0,
            "notional": 50.0,
            "unrealized_pnl": 1.0,
            "unrealized_pnl_pct": 2.0,
        },
    ]

    summary = MODULE._build_symbol_summary(closed_trades, open_positions)

    assert list(summary["symbol"]) == ["BTCUSD", "ETHUSD"]
    btc_row = summary[summary["symbol"] == "BTCUSD"].iloc[0]
    eth_row = summary[summary["symbol"] == "ETHUSD"].iloc[0]
    assert btc_row["closed_trades"] == 1
    assert btc_row["open_positions"] == 1
    assert eth_row["closed_trades"] == 0
    assert eth_row["open_positions"] == 1
