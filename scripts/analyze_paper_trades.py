import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List

import pandas as pd

# Add src to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.persistence.db import DatabaseManager
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
    strategy_name: Optional[str] = "unknown"
    status: str = "closed"

def _to_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

def _get_mark_price(symbol: str, api_url: str) -> Optional[float]:
    try:
        df = fetch_ticker(symbol=symbol, api_url=api_url)
        if df.empty:
            return None
        return float(df.iloc[0].get("mark_price") or df.iloc[0].get("price") or 0.0)
    except Exception:
        return None

def print_report(metrics: dict, daily: pd.DataFrame, closed: List[dict], open_rows: List[dict], mode: str):
    print("=" * 90)
    print(f"TRADING ANALYSIS REPORT ({mode.upper()})")
    print("=" * 90)
    print(f"Closed Trades    : {int(metrics['total_trades'])}")
    print(f"Wins / Losses    : {int(metrics['wins'])} / {int(metrics['losses'])}")
    print(f"Win Rate         : {metrics['win_rate']:.2f}%")
    print(f"Realized PnL     : {metrics['total_pnl']:.4f}")
    print(f"Profit Factor    : {metrics['profit_factor']:.4f}")
    print("-" * 90)
    print(f"Open Positions   : {len(open_rows)}")
    print("=" * 90)

    if not daily.empty:
        print("\nDAILY SUMMARY")
        print("-" * 90)
        for _, r in daily.iterrows():
            print(f"{r['date']} | trades={int(r['trades'])} | pnl={r['pnl']:.4f} | win_rate={r['win_rate']:.2f}%")

    if open_rows:
        print("\nACTIVE OPEN POSITIONS")
        print("-" * 90)
        for p in open_rows:
            print(f"{p['symbol']} | side={p['side']} | size={p['size']:.4f} | entry={p['avg_entry_price']:.4f}")

    if closed:
        print("\nRECENT CLOSED TRADES")
        print("-" * 115)
        print(f"{'Trade ID':<25} | {'Sym':<8} | {'Side':<4} | {'PnL':<10} | {'Status'}")
        print("-" * 115)
        for t in closed[:20]:
            print(f"{t['trade_id'][:25]:<25} | {t['symbol']:<8} | {t['side']:<4} | {t['pnl_raw']:>10.4f} | {t['status']}")

def main():
    parser = argparse.ArgumentParser(description="Analyze trades from PostgreSQL")
    parser.add_argument("--mode", default="paper", choices=["paper", "live"])
    args = parser.parse_args()

    settings = Settings(mode=args.mode)
    db = DatabaseManager(settings.postgres_dsn)

    # 1. Load Data
    closed_raw = db.get_trade_records(limit=100)
    open_positions = db.get_all_active_positions()

    # 2. Compute Metrics
    total_trades = len(closed_raw)
    total_pnl = sum(t["pnl_raw"] for t in closed_raw if t["pnl_raw"] is not None)
    wins = sum(1 for t in closed_raw if (t["pnl_raw"] or 0) > 0)
    losses = sum(1 for t in closed_raw if (t["pnl_raw"] or 0) < 0)
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    
    gross_profit = sum(t["pnl_raw"] for t in closed_raw if (t["pnl_raw"] or 0) > 0)
    gross_loss = abs(sum(t["pnl_raw"] for t in closed_raw if (t["pnl_raw"] or 0) < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)

    # 3. Daily Summary (Simplified)
    daily_df = pd.DataFrame() # Placeholder for now as isoformat needs parsing
    
    metrics = {
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "profit_factor": profit_factor
    }

    print_report(metrics, daily_df, closed_raw, open_positions, args.mode)

if __name__ == "__main__":
    main()
