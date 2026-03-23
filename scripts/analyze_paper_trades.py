from __future__ import annotations

import argparse
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import func, or_

# Add src to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.data.market_data import fetch_ticker
from delta_exchange_bot.persistence.db import DatabaseManager
from delta_exchange_bot.persistence.models import ExecutionLog, Order, PerformanceMetric, Position, Trade, TradeStatus


@dataclass
class TradeRow:
    trade_id: str
    symbol: str
    side: str
    size: float
    entry_price: float
    exit_price: float | None
    pnl_raw: float
    pnl_pct: float
    status: str
    strategy_name: str | None
    entry_time: datetime | None
    exit_time: datetime | None


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return None


def _iso_dt(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _safe_pct(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-12:
        return 0.0
    return (numerator / denominator) * 100.0


def _safe_ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-12:
        return 0.0
    return numerator / denominator


def _sanitize_dsn(dsn: str) -> str:
    if "@" not in dsn:
        return dsn
    return dsn.split("@", 1)[1]


def _get_mark_price(symbol: str, api_url: str) -> float | None:
    try:
        df = fetch_ticker(symbol=symbol, api_url=api_url)
        if df.empty:
            return None
        row = df.iloc[0]
        for col in ("mark_price", "close", "spot_price", "last_price", "price"):
            value = _to_float(row.get(col))
            if value > 0:
                return value
    except Exception:
        return None
    return None


def _load_trade_rows(db: DatabaseManager, lookback_days: int) -> list[TradeRow]:
    cutoff = None
    if lookback_days > 0:
        cutoff = datetime.utcnow() - timedelta(days=lookback_days)

    with db.get_session() as session:
        query = session.query(Trade)
        if cutoff is not None:
            query = query.filter(or_(Trade.entry_time >= cutoff, Trade.exit_time >= cutoff))
        rows = query.order_by(Trade.entry_time.desc()).all()

    out: list[TradeRow] = []
    for row in rows:
        status = row.status.value if row.status else ""
        side = row.side.value if row.side else ""
        out.append(
            TradeRow(
                trade_id=row.trade_id,
                symbol=row.symbol,
                side=side,
                size=_to_float(row.size),
                entry_price=_to_float(row.entry_price),
                exit_price=float(row.exit_price) if row.exit_price is not None else None,
                pnl_raw=_to_float(row.pnl_raw),
                pnl_pct=_to_float(row.pnl_pct),
                status=status,
                strategy_name=row.strategy_name,
                entry_time=_to_datetime(row.entry_time),
                exit_time=_to_datetime(row.exit_time),
            )
        )
    return out


def _load_open_positions(db: DatabaseManager) -> list[dict[str, Any]]:
    with db.get_session() as session:
        rows = session.query(Position).order_by(Position.symbol.asc()).all()

    out: list[dict[str, Any]] = []
    for row in rows:
        side = row.side.value if row.side else ""
        out.append(
            {
                "symbol": row.symbol,
                "trade_id": row.trade_id,
                "side": side,
                "size": _to_float(row.size),
                "avg_entry_price": _to_float(row.avg_entry_price),
                "updated_at": _to_datetime(row.updated_at),
            }
        )
    return out


def _load_order_status_counts(db: DatabaseManager) -> dict[str, int]:
    with db.get_session() as session:
        rows = session.query(Order.status, func.count(Order.id)).group_by(Order.status).all()
    counts: dict[str, int] = {}
    for status, count in rows:
        key = status.value if status else "unknown"
        counts[key] = int(count)
    return counts


def _load_execution_event_counts(db: DatabaseManager) -> dict[str, int]:
    with db.get_session() as session:
        rows = session.query(ExecutionLog.event_type, func.count(ExecutionLog.id)).group_by(ExecutionLog.event_type).all()
    return {str(event_type): int(count) for event_type, count in rows}


def _load_latest_performance_metric(db: DatabaseManager) -> dict[str, Any] | None:
    with db.get_session() as session:
        row = session.query(PerformanceMetric).order_by(PerformanceMetric.timestamp.desc()).first()
    if row is None:
        return None
    return {
        "timestamp": _to_datetime(row.timestamp),
        "mode": row.mode,
        "total_trades": row.total_trades,
        "win_rate": _to_float(row.win_rate),
        "profit_factor": _to_float(row.profit_factor),
        "max_drawdown": _to_float(row.max_drawdown),
        "realized_pnl": _to_float(row.realized_pnl),
        "unrealized_pnl": _to_float(row.unrealized_pnl),
    }


def _analyze_open_positions(open_positions: list[dict[str, Any]], api_url: str) -> tuple[list[dict[str, Any]], float]:
    mark_prices: dict[str, float | None] = {}
    analyzed: list[dict[str, Any]] = []
    total_upnl = 0.0

    for position in open_positions:
        symbol = position["symbol"]
        if symbol not in mark_prices:
            mark_prices[symbol] = _get_mark_price(symbol, api_url)
        mark_price = mark_prices[symbol]

        entry_price = _to_float(position["avg_entry_price"])
        size = _to_float(position["size"])
        side = position["side"]
        notional = entry_price * size

        unrealized_pnl = None
        unrealized_pnl_pct = None
        if mark_price is not None and mark_price > 0 and entry_price > 0 and size > 0:
            if side == "long":
                unrealized_pnl = (mark_price - entry_price) * size
            else:
                unrealized_pnl = (entry_price - mark_price) * size
            unrealized_pnl_pct = _safe_pct(unrealized_pnl, notional)
            total_upnl += unrealized_pnl

        analyzed.append(
            {
                **position,
                "mark_price": mark_price,
                "notional": notional,
                "unrealized_pnl": unrealized_pnl,
                "unrealized_pnl_pct": unrealized_pnl_pct,
            }
        )

    return analyzed, total_upnl


def _compute_closed_trade_metrics(closed_trades: list[TradeRow], initial_equity: float) -> dict[str, float]:
    total_trades = len(closed_trades)
    wins = sum(1 for trade in closed_trades if trade.pnl_raw > 0)
    losses = sum(1 for trade in closed_trades if trade.pnl_raw < 0)
    realized_pnl = sum(trade.pnl_raw for trade in closed_trades)
    gross_profit = sum(trade.pnl_raw for trade in closed_trades if trade.pnl_raw > 0)
    gross_loss = abs(sum(trade.pnl_raw for trade in closed_trades if trade.pnl_raw < 0))
    avg_pnl = _safe_ratio(realized_pnl, total_trades)
    avg_win = _safe_ratio(gross_profit, wins)
    avg_loss = _safe_ratio(sum(trade.pnl_raw for trade in closed_trades if trade.pnl_raw < 0), losses)

    best_trade = max((trade.pnl_raw for trade in closed_trades), default=0.0)
    worst_trade = min((trade.pnl_raw for trade in closed_trades), default=0.0)

    exposure = sum(abs(trade.entry_price * trade.size) for trade in closed_trades if trade.entry_price and trade.size)
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)

    running_equity = float(initial_equity)
    peak_equity = float(initial_equity)
    max_drawdown = 0.0
    max_drawdown_pct = 0.0

    ordered = sorted(closed_trades, key=lambda trade: trade.exit_time or trade.entry_time or datetime.min.replace(tzinfo=timezone.utc))
    for trade in ordered:
        running_equity += trade.pnl_raw
        peak_equity = max(peak_equity, running_equity)
        drawdown = peak_equity - running_equity
        max_drawdown = max(max_drawdown, drawdown)
        max_drawdown_pct = max(max_drawdown_pct, _safe_pct(drawdown, peak_equity))

    return {
        "total_closed_trades": float(total_trades),
        "wins": float(wins),
        "losses": float(losses),
        "win_rate": _safe_pct(wins, total_trades),
        "realized_pnl": realized_pnl,
        "realized_pnl_pct_initial_equity": _safe_pct(realized_pnl, initial_equity),
        "realized_pnl_pct_exposure": _safe_pct(realized_pnl, exposure),
        "avg_pnl": avg_pnl,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "profit_factor": profit_factor,
        "expectancy": avg_pnl,
        "max_drawdown": max_drawdown,
        "max_drawdown_pct": max_drawdown_pct,
        "ending_equity_realized_only": initial_equity + realized_pnl,
        "exposure": exposure,
    }


def _build_daily_summary(closed_trades: list[TradeRow]) -> pd.DataFrame:
    if not closed_trades:
        return pd.DataFrame(columns=["date", "trades", "wins", "losses", "pnl", "pnl_pct", "win_rate"])

    records: list[dict[str, Any]] = []
    for trade in closed_trades:
        trade_dt = trade.exit_time or trade.entry_time
        if trade_dt is None:
            continue
        records.append(
            {
                "date": trade_dt.date().isoformat(),
                "symbol": trade.symbol,
                "pnl": trade.pnl_raw,
                "pnl_pct": trade.pnl_pct,
                "is_win": 1 if trade.pnl_raw > 0 else 0,
                "is_loss": 1 if trade.pnl_raw < 0 else 0,
            }
        )
    if not records:
        return pd.DataFrame(columns=["date", "trades", "wins", "losses", "pnl", "pnl_pct", "win_rate"])

    frame = pd.DataFrame(records)
    summary = frame.groupby("date", as_index=False).agg(
        trades=("pnl", "count"),
        wins=("is_win", "sum"),
        losses=("is_loss", "sum"),
        pnl=("pnl", "sum"),
        pnl_pct=("pnl_pct", "sum"),
    )
    summary["win_rate"] = summary.apply(
        lambda row: _safe_pct(float(row["wins"]), float(row["trades"])),
        axis=1,
    )
    return summary.sort_values("date")


def _build_symbol_summary(closed_trades: list[TradeRow], open_positions: list[dict[str, Any]]) -> pd.DataFrame:
    all_symbols = sorted({trade.symbol for trade in closed_trades} | {row["symbol"] for row in open_positions})
    if not all_symbols:
        return pd.DataFrame(
            columns=[
                "symbol",
                "closed_trades",
                "wins",
                "losses",
                "win_rate",
                "realized_pnl",
                "avg_pnl",
                "open_positions",
            ]
        )

    open_counts = Counter(position["symbol"] for position in open_positions)
    rows: list[dict[str, Any]] = []
    for symbol in all_symbols:
        symbol_trades = [trade for trade in closed_trades if trade.symbol == symbol]
        wins = sum(1 for trade in symbol_trades if trade.pnl_raw > 0)
        losses = sum(1 for trade in symbol_trades if trade.pnl_raw < 0)
        realized_pnl = sum(trade.pnl_raw for trade in symbol_trades)
        rows.append(
            {
                "symbol": symbol,
                "closed_trades": len(symbol_trades),
                "wins": wins,
                "losses": losses,
                "win_rate": _safe_pct(wins, len(symbol_trades)),
                "realized_pnl": realized_pnl,
                "avg_pnl": _safe_ratio(realized_pnl, len(symbol_trades)),
                "open_positions": open_counts.get(symbol, 0),
            }
        )

    return pd.DataFrame(rows).sort_values(["realized_pnl", "symbol"], ascending=[False, True])


def _print_summary_block(
    metrics: dict[str, float],
    mode: str,
    dsn: str,
    lookback_days: int,
    total_open_positions: int,
    total_open_trade_rows: int,
    total_upnl: float,
    initial_equity: float,
    order_counts: dict[str, int],
    execution_counts: dict[str, int],
) -> None:
    total_pnl = metrics["realized_pnl"] + total_upnl
    total_pnl_pct = _safe_pct(total_pnl, initial_equity)
    unrealized_pct = _safe_pct(total_upnl, initial_equity)

    print("=" * 110)
    print(f"TRADE ANALYSIS REPORT ({mode.upper()})")
    print("=" * 110)
    print(f"Database         : {_sanitize_dsn(dsn)}")
    print(f"Lookback         : {lookback_days if lookback_days > 0 else 'ALL'} days")
    print(f"Initial Equity   : {initial_equity:,.2f}")
    print("-" * 110)
    print(f"Closed Trades    : {int(metrics['total_closed_trades'])}")
    print(f"Open Trades      : {total_open_trade_rows}")
    print(f"Open Positions   : {total_open_positions}")
    print(f"Wins / Losses    : {int(metrics['wins'])} / {int(metrics['losses'])}")
    print(f"Win Rate         : {metrics['win_rate']:.2f}%")
    print(f"Realized PnL     : {metrics['realized_pnl']:.4f}")
    print(f"Realized PnL %   : {metrics['realized_pnl_pct_initial_equity']:.4f}% of initial equity")
    print(f"Unrealized PnL   : {total_upnl:.4f}")
    print(f"Unrealized PnL % : {unrealized_pct:.4f}% of initial equity")
    print(f"Total PnL        : {total_pnl:.4f}")
    print(f"Total PnL %      : {total_pnl_pct:.4f}% of initial equity")
    print(f"Profit Factor    : {metrics['profit_factor']:.4f}")
    print(f"Avg PnL/Trade    : {metrics['avg_pnl']:.4f}")
    print(f"Avg Win / Loss   : {metrics['avg_win']:.4f} / {metrics['avg_loss']:.4f}")
    print(f"Best / Worst     : {metrics['best_trade']:.4f} / {metrics['worst_trade']:.4f}")
    print(f"Max Drawdown     : {metrics['max_drawdown']:.4f} ({metrics['max_drawdown_pct']:.4f}%)")
    print(f"Ending Equity    : {metrics['ending_equity_realized_only'] + total_upnl:,.2f}")
    print("-" * 110)
    print(f"Orders By Status : {order_counts if order_counts else '{}'}")
    print(f"Execution Events : {execution_counts if execution_counts else '{}'}")
    print("=" * 110)


def _print_open_positions(rows: list[dict[str, Any]], recent_limit: int) -> None:
    if not rows:
        print("\nOPEN POSITIONS")
        print("-" * 110)
        print("No open positions.")
        return

    print("\nOPEN POSITIONS")
    print("-" * 110)
    print(
        f"{'Symbol':<10} {'Side':<6} {'Size':>10} {'Entry':>12} {'Mark':>12} {'uPnL':>12} {'uPnL%':>10} {'Trade ID':<24}"
    )
    print("-" * 110)
    for row in rows[:recent_limit]:
        mark = "-" if row["mark_price"] is None else f"{row['mark_price']:.4f}"
        upnl = "-" if row["unrealized_pnl"] is None else f"{row['unrealized_pnl']:.4f}"
        upnl_pct = "-" if row["unrealized_pnl_pct"] is None else f"{row['unrealized_pnl_pct']:.2f}%"
        print(
            f"{row['symbol']:<10} {row['side']:<6} {row['size']:>10.4f} {row['avg_entry_price']:>12.4f} "
            f"{mark:>12} {upnl:>12} {upnl_pct:>10} {row['trade_id'][:24]:<24}"
        )


def _print_recent_closed_trades(rows: list[TradeRow], recent_limit: int) -> None:
    print("\nRECENT CLOSED TRADES")
    print("-" * 140)
    if not rows:
        print("No closed trades found.")
        return
    print(
        f"{'Trade ID':<24} {'Symbol':<8} {'Side':<6} {'Size':>10} {'Entry':>12} {'Exit':>12} {'PnL':>12} {'PnL%':>10} {'Exit Time':<22}"
    )
    print("-" * 140)
    recent_rows = sorted(
        rows,
        key=lambda trade: trade.exit_time or trade.entry_time or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    for trade in recent_rows[:recent_limit]:
        exit_price = "-" if trade.exit_price is None else f"{trade.exit_price:.4f}"
        print(
            f"{trade.trade_id[:24]:<24} {trade.symbol:<8} {trade.side:<6} {trade.size:>10.4f} "
            f"{trade.entry_price:>12.4f} {exit_price:>12} {trade.pnl_raw:>12.4f} {trade.pnl_pct:>9.2f}% {_iso_dt(trade.exit_time):<22}"
        )


def _print_dataframe(title: str, frame: pd.DataFrame) -> None:
    print(f"\n{title}")
    print("-" * 110)
    if frame.empty:
        print("No data.")
        return
    print(frame.to_string(index=False))


def _print_latest_metric(metric: dict[str, Any] | None) -> None:
    print("\nLATEST PERFORMANCE SNAPSHOT")
    print("-" * 110)
    if metric is None:
        print("No performance_metrics rows found.")
        return

    print(f"Timestamp        : {_iso_dt(metric['timestamp'])}")
    print(f"Mode             : {metric['mode']}")
    print(f"Total Trades     : {metric['total_trades']}")
    print(f"Win Rate         : {metric['win_rate']:.2f}%")
    print(f"Profit Factor    : {metric['profit_factor']:.4f}")
    print(f"Max Drawdown     : {metric['max_drawdown']:.4f}")
    print(f"Realized PnL     : {metric['realized_pnl']:.4f}")
    print(f"Unrealized PnL   : {metric['unrealized_pnl']:.4f}")


def build_report(mode: str, lookback_days: int, initial_equity: float, recent_limit: int) -> dict[str, Any]:
    settings = Settings(mode=mode)
    db = DatabaseManager(settings.postgres_dsn)

    trade_rows = _load_trade_rows(db, lookback_days=lookback_days)
    closed_trades = [trade for trade in trade_rows if trade.status == TradeStatus.CLOSED.value]
    open_trade_rows = [trade for trade in trade_rows if trade.status == TradeStatus.OPEN.value]
    open_positions = _load_open_positions(db)
    analyzed_open_positions, total_upnl = _analyze_open_positions(open_positions, settings.api_url)
    order_counts = _load_order_status_counts(db)
    execution_counts = _load_execution_event_counts(db)
    latest_metric = _load_latest_performance_metric(db)

    metrics = _compute_closed_trade_metrics(closed_trades, initial_equity=initial_equity)
    daily_summary = _build_daily_summary(closed_trades)
    symbol_summary = _build_symbol_summary(closed_trades, analyzed_open_positions)

    return {
        "settings": settings,
        "trade_rows": trade_rows,
        "closed_trades": closed_trades,
        "open_trade_rows": open_trade_rows,
        "open_positions": analyzed_open_positions,
        "total_upnl": total_upnl,
        "order_counts": order_counts,
        "execution_counts": execution_counts,
        "latest_metric": latest_metric,
        "metrics": metrics,
        "daily_summary": daily_summary,
        "symbol_summary": symbol_summary,
        "recent_limit": recent_limit,
        "lookback_days": lookback_days,
        "initial_equity": initial_equity,
        "mode": mode,
    }


def print_report(report: dict[str, Any]) -> None:
    settings = report["settings"]
    metrics = report["metrics"]

    _print_summary_block(
        metrics=metrics,
        mode=report["mode"],
        dsn=settings.postgres_dsn,
        lookback_days=report["lookback_days"],
        total_open_positions=len(report["open_positions"]),
        total_open_trade_rows=len(report["open_trade_rows"]),
        total_upnl=report["total_upnl"],
        initial_equity=report["initial_equity"],
        order_counts=report["order_counts"],
        execution_counts=report["execution_counts"],
    )
    _print_open_positions(report["open_positions"], report["recent_limit"])
    _print_recent_closed_trades(report["closed_trades"], report["recent_limit"])
    _print_dataframe("SYMBOL SUMMARY", report["symbol_summary"])
    _print_dataframe("DAILY SUMMARY", report["daily_summary"])
    _print_latest_metric(report["latest_metric"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze PostgreSQL trade data for paper or live mode.")
    parser.add_argument("--mode", default="paper", choices=["paper", "live"])
    parser.add_argument("--lookback-days", type=int, default=0, help="0 means all rows")
    parser.add_argument("--initial-equity", type=float, default=100000.0)
    parser.add_argument("--recent-limit", type=int, default=20)
    args = parser.parse_args()

    report = build_report(
        mode=args.mode,
        lookback_days=max(0, int(args.lookback_days)),
        initial_equity=float(args.initial_equity),
        recent_limit=max(1, int(args.recent_limit)),
    )
    print_report(report)


if __name__ == "__main__":
    main()
