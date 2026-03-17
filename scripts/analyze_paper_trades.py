import argparse
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.data.market_data import fetch_ticker


@dataclass
class ClosedTrade:
    trade_id: str
    symbol: str
    side: str
    size: float
    entry_price: float
    exit_price: float
    pnl: float
    entry_ts: str
    exit_ts: str


def _to_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_ts(ts: str) -> datetime:
    try:
        # SQLite CURRENT_TIMESTAMP format: YYYY-MM-DD HH:MM:SS
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def _get_mark_price(symbol: str, api_url: str) -> Optional[float]:
    try:
        df = fetch_ticker(symbol=symbol, api_url=api_url)
        if df.empty:
            return None

        row = df.iloc[0]
        for col in ("mark_price", "close", "spot_price", "last_price", "price"):
            if col in df.columns:
                val = _to_float(row.get(col))
                if val > 0:
                    return val
        return None
    except Exception:
        return None


def _load_execution_rows(conn: sqlite3.Connection, mode: str, lookback_days: int) -> list[dict]:
    if lookback_days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        cutoff_s = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        query = """
            SELECT trade_id, symbol, side, size, price, event_type, status, reason, ts
            FROM execution_logs
            WHERE mode = ? AND ts >= ?
            ORDER BY id ASC
        """
        rows = conn.execute(query, (mode, cutoff_s)).fetchall()
    else:
        query = """
            SELECT trade_id, symbol, side, size, price, event_type, status, reason, ts
            FROM execution_logs
            WHERE mode = ?
            ORDER BY id ASC
        """
        rows = conn.execute(query, (mode,)).fetchall()

    out = []
    for r in rows:
        out.append(
            {
                "trade_id": r[0],
                "symbol": r[1],
                "side": str(r[2]).lower(),
                "size": _to_float(r[3]),
                "price": _to_float(r[4]),
                "event_type": str(r[5]).lower(),
                "status": str(r[6]).lower(),
                "reason": r[7],
                "ts": r[8],
            }
        )
    return out


def _build_closed_trades(execution_rows: list[dict]) -> tuple[list[ClosedTrade], set[str]]:
    by_trade: dict[str, list[dict]] = {}
    for row in execution_rows:
        by_trade.setdefault(row["trade_id"], []).append(row)

    closed: list[ClosedTrade] = []
    open_trade_ids: set[str] = set()
    for trade_id, rows in by_trade.items():
        entries = [r for r in rows if r["event_type"] == "entry"]
        exits = [r for r in rows if r["event_type"] == "exit"]
        if not entries:
            continue

        entry = entries[0]
        if not exits:
            open_trade_ids.add(trade_id)
            continue

        exit_row = exits[-1]
        side = entry["side"]
        size = abs(entry["size"])
        entry_price = entry["price"]
        exit_price = exit_row["price"]
        if side == "buy":
            pnl = (exit_price - entry_price) * size
        else:
            pnl = (entry_price - exit_price) * size

        closed.append(
            ClosedTrade(
                trade_id=trade_id,
                symbol=entry["symbol"],
                side=side,
                size=size,
                entry_price=entry_price,
                exit_price=exit_price,
                pnl=pnl,
                entry_ts=entry["ts"],
                exit_ts=exit_row["ts"],
            )
        )

    closed.sort(key=lambda x: _safe_ts(x.exit_ts))
    return closed, open_trade_ids


def _compute_metrics(closed: list[ClosedTrade], initial_equity: float) -> dict[str, float]:
    total_trades = len(closed)
    total_pnl = sum(t.pnl for t in closed)
    wins = sum(1 for t in closed if t.pnl > 0)
    losses = sum(1 for t in closed if t.pnl < 0)
    win_rate = (wins / total_trades * 100.0) if total_trades else 0.0

    gross_profit = sum(t.pnl for t in closed if t.pnl > 0)
    gross_loss_abs = -sum(t.pnl for t in closed if t.pnl < 0)
    if gross_loss_abs == 0:
        profit_factor = float("inf") if gross_profit > 0 else 0.0
    else:
        profit_factor = gross_profit / gross_loss_abs

    equity = initial_equity
    peak = initial_equity
    max_drawdown_abs = 0.0
    max_drawdown_pct = 0.0
    for t in closed:
        equity += t.pnl
        if equity > peak:
            peak = equity
        dd_abs = peak - equity
        dd_pct = (dd_abs / peak * 100.0) if peak > 0 else 0.0
        max_drawdown_abs = max(max_drawdown_abs, dd_abs)
        max_drawdown_pct = max(max_drawdown_pct, dd_pct)

    return {
        "total_trades": float(total_trades),
        "wins": float(wins),
        "losses": float(losses),
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "avg_pnl": (total_pnl / total_trades) if total_trades else 0.0,
        "profit_factor": profit_factor,
        "max_drawdown_abs": max_drawdown_abs,
        "max_drawdown_pct": max_drawdown_pct,
        "ending_equity": initial_equity + total_pnl,
    }


def _load_open_positions(conn: sqlite3.Connection, mode: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT symbol, trade_id, side, size, entry_price, stop_loss, take_profit, trailing_stop_pct, updated_at
        FROM open_position_state
        WHERE mode = ?
        ORDER BY symbol
        """,
        (mode,),
    ).fetchall()

    out = []
    for r in rows:
        out.append(
            {
                "symbol": r[0],
                "trade_id": r[1],
                "side": str(r[2]).lower(),
                "size": _to_float(r[3]),
                "entry_price": _to_float(r[4]),
                "stop_loss": _to_float(r[5]) if r[5] is not None else None,
                "take_profit": _to_float(r[6]) if r[6] is not None else None,
                "trailing_stop_pct": _to_float(r[7]) if r[7] is not None else None,
                "updated_at": r[8],
            }
        )
    return out


def _compute_open_position_upnl(open_positions: list[dict], api_url: str) -> tuple[list[dict], float]:
    by_symbol_mark: dict[str, Optional[float]] = {}
    rows: list[dict] = []
    total_upnl = 0.0

    for pos in open_positions:
        symbol = pos["symbol"]
        if symbol not in by_symbol_mark:
            by_symbol_mark[symbol] = _get_mark_price(symbol, api_url)
        mark = by_symbol_mark[symbol]

        upnl = None
        if mark is not None and mark > 0:
            if pos["side"] == "long":
                upnl = (mark - pos["entry_price"]) * pos["size"]
            else:
                upnl = (pos["entry_price"] - mark) * pos["size"]
            total_upnl += upnl

        row = dict(pos)
        row["mark_price"] = mark
        row["unrealized_pnl"] = upnl
        rows.append(row)
    return rows, total_upnl


def _daily_summary(closed: list[ClosedTrade]) -> pd.DataFrame:
    if not closed:
        return pd.DataFrame(columns=["date", "trades", "pnl", "wins", "losses", "win_rate"])

    records = []
    for t in closed:
        d = _safe_ts(t.exit_ts).date().isoformat()
        records.append({"date": d, "pnl": t.pnl, "is_win": t.pnl > 0, "is_loss": t.pnl < 0})

    df = pd.DataFrame(records)
    g = df.groupby("date", as_index=False).agg(
        trades=("pnl", "count"),
        pnl=("pnl", "sum"),
        wins=("is_win", "sum"),
        losses=("is_loss", "sum"),
    )
    g["win_rate"] = g.apply(lambda r: (r["wins"] / r["trades"] * 100.0) if r["trades"] else 0.0, axis=1)
    return g.sort_values("date")


def print_report(
    metrics: dict[str, float],
    daily: pd.DataFrame,
    closed: list[ClosedTrade],
    open_rows: list[dict],
    total_upnl: float,
    mode: str,
    db_path: Path,
    lookback_days: int,
):
    print("=" * 90)
    print("PAPER TRADING ANALYSIS REPORT")
    print("=" * 90)
    print(f"DB Path          : {db_path}")
    print(f"Mode             : {mode}")
    print(f"Lookback Days    : {lookback_days if lookback_days > 0 else 'ALL'}")
    print("-" * 90)
    print(f"Closed Trades    : {int(metrics['total_trades'])}")
    print(f"Wins / Losses    : {int(metrics['wins'])} / {int(metrics['losses'])}")
    print(f"Win Rate         : {metrics['win_rate']:.2f}%")
    print(f"Realized PnL     : {metrics['total_pnl']:.4f}")
    print(f"Avg PnL/Trade    : {metrics['avg_pnl']:.4f}")
    pf = metrics["profit_factor"]
    pf_text = "inf" if pf == float("inf") else f"{pf:.4f}"
    print(f"Profit Factor    : {pf_text}")
    print(f"Max Drawdown     : {metrics['max_drawdown_abs']:.4f} ({metrics['max_drawdown_pct']:.2f}%)")
    print(f"Ending Equity    : {metrics['ending_equity']:.4f}")
    print("-" * 90)
    print(f"Open Positions   : {len(open_rows)}")
    print(f"Unrealized PnL   : {total_upnl:.4f}")
    print("=" * 90)

    if not daily.empty:
        print("\nDAILY SUMMARY")
        print("-" * 90)
        for _, r in daily.iterrows():
            print(
                f"{r['date']} | trades={int(r['trades'])} | pnl={r['pnl']:.4f} "
                f"| wins={int(r['wins'])} | losses={int(r['losses'])} | win_rate={r['win_rate']:.2f}%"
            )

    if open_rows:
        print("\nACTIVE OPEN POSITIONS")
        print("-" * 90)
        for p in open_rows:
            mark_txt = f"{p['mark_price']:.6f}" if p["mark_price"] is not None else "N/A"
            upnl_txt = f"{p['unrealized_pnl']:.4f}" if p["unrealized_pnl"] is not None else "N/A"
            print(
                f"{p['symbol']} | {p['trade_id']} | side={p['side']} | size={p['size']:.6f} "
                f"| entry={p['entry_price']:.6f} | mark={mark_txt} | uPnL={upnl_txt}"
            )

    if closed:
        print("\nRECENT CLOSED TRADES (last 20)")
        print("-" * 90)
        for t in closed[-20:]:
            print(
                f"{t.exit_ts} | {t.trade_id} | {t.symbol} | {t.side} | "
                f"entry={t.entry_price:.6f} exit={t.exit_price:.6f} size={t.size:.6f} pnl={t.pnl:.4f}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze paper/live trade performance from state.db")
    parser.add_argument("--db-path", default="state.db", help="Path to SQLite state DB")
    parser.add_argument("--mode", default="paper", choices=["paper", "live"], help="Execution mode to analyze")
    parser.add_argument("--lookback-days", type=int, default=30, help="Number of days to analyze. 0 = all history")
    parser.add_argument("--initial-equity", type=float, default=100000.0, help="Initial equity baseline for drawdown")
    parser.add_argument(
        "--show-empty",
        action="store_true",
        help="Print report even when no trades are found in the selected window",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    settings = Settings(mode=args.mode)

    conn = sqlite3.connect(str(db_path))
    try:
        execution_rows = _load_execution_rows(conn, mode=args.mode, lookback_days=args.lookback_days)
        closed, _ = _build_closed_trades(execution_rows)
        open_positions = _load_open_positions(conn, mode=args.mode)
    finally:
        conn.close()

    if not closed and not open_positions and not args.show_empty:
        print("No closed or open trades found for selected filters. Use --show-empty to print an empty report.")
        return

    metrics = _compute_metrics(closed, initial_equity=args.initial_equity)
    daily = _daily_summary(closed)
    open_rows, total_upnl = _compute_open_position_upnl(open_positions, api_url=settings.api_url)

    print_report(
        metrics=metrics,
        daily=daily,
        closed=closed,
        open_rows=open_rows,
        total_upnl=total_upnl,
        mode=args.mode,
        db_path=db_path,
        lookback_days=args.lookback_days,
    )


if __name__ == "__main__":
    main()
