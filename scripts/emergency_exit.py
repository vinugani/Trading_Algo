from __future__ import annotations

import argparse
from pathlib import Path
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from delta_exchange_bot.cli.professional_bot import ProfessionalTradingBot
from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.persistence.db import DatabaseManager


def _collect_symbols(settings: Settings, cli_symbols: list[str]) -> list[str]:
    symbols = {s.strip().upper() for s in settings.trade_symbols if str(s).strip()}
    symbols.update(s.strip().upper() for s in cli_symbols if str(s).strip())
    try:
        db = DatabaseManager(settings.postgres_dsn)
        for symbol in settings.trade_symbols:
            if db.get_active_position(symbol):
                symbols.add(symbol.strip().upper())
    except Exception:
        pass
    return sorted(symbols)


def main() -> None:
    parser = argparse.ArgumentParser(description="Emergency stop + flatten utility for Delta Exchange India bot")
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbols to verify/flatten. Defaults to bot symbols + local DB open positions.",
    )
    parser.add_argument("--wait-stop-seconds", type=int, default=5, help="Wait duration after writing shutdown signal")
    parser.add_argument("--no-flatten", action="store_true", help="Only request shutdown and cancel open orders")
    args = parser.parse_args()

    settings = Settings(mode="live")
    shutdown_path = Path(settings.shutdown_signal_path)
    shutdown_path.parent.mkdir(parents=True, exist_ok=True)
    shutdown_path.write_text("shutdown_requested_by_emergency_exit\n", encoding="utf-8")
    print(f"[INFO] Shutdown signal written to: {shutdown_path}")

    wait_s = max(0, int(args.wait_stop_seconds))
    if wait_s > 0:
        print(f"[INFO] Waiting {wait_s}s for running bot loop to stop gracefully...")
        time.sleep(wait_s)

    bot = ProfessionalTradingBot(settings=settings)
    symbols = _collect_symbols(settings, args.symbols.split(",") if args.symbols else [])

    cancelled = bot.cancel_open_orders()
    print(f"[INFO] Cancelled open orders: {cancelled}")

    flattened = []
    skipped = []
    failed = []

    for symbol in symbols:
        if not symbol:
            continue
        if not bot.sync_position_with_exchange(symbol, reason="emergency_exit_check"):
            failed.append(symbol)
            print(f"[ERROR] Failed to sync position for {symbol}")
            continue
        signed = bot._local_signed_size(symbol)
        if abs(signed) <= settings.position_sync_tolerance:
            skipped.append(symbol)
            print(f"[INFO] {symbol}: already flat")
            continue
        if args.no_flatten:
            skipped.append(symbol)
            print(f"[WARN] {symbol}: position detected ({signed}) but flatten disabled")
            continue
        ok = bot.flatten_position_safely(symbol)
        if ok:
            flattened.append(symbol)
            print(f"[INFO] {symbol}: flattened successfully")
        else:
            failed.append(symbol)
            print(f"[ERROR] {symbol}: flatten failed")

    # Final verification pass.
    still_open = []
    for symbol in symbols:
        if not symbol:
            continue
        if not bot.sync_position_with_exchange(symbol, reason="emergency_exit_verify"):
            still_open.append(symbol)
            continue
        if abs(bot._local_signed_size(symbol)) > settings.position_sync_tolerance:
            still_open.append(symbol)

    print("\n=== Emergency Exit Summary ===")
    print(f"Symbols checked: {len(symbols)}")
    print(f"Flattened: {flattened}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")
    print(f"Still open after verify: {still_open}")

    if still_open or failed:
        raise SystemExit(1)
    raise SystemExit(0)


if __name__ == "__main__":
    main()
