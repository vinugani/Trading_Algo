from dataclasses import dataclass
from pathlib import Path
import sys
import time
from sqlalchemy import text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from delta_exchange_bot.api.delta_client import DeltaAPIError
from delta_exchange_bot.api.delta_client import DeltaClient
from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.persistence.db import DatabaseManager


@dataclass
class CheckResult:
    name: str
    status: str
    details: str


def _ok(name: str, details: str) -> CheckResult:
    return CheckResult(name=name, status="PASS", details=details)


def _warn(name: str, details: str) -> CheckResult:
    return CheckResult(name=name, status="WARN", details=details)


def _fail(name: str, details: str) -> CheckResult:
    return CheckResult(name=name, status="FAIL", details=details)


def _print_results(results: list[CheckResult]) -> None:
    print("\n=== Delta Live Preflight Report ===")
    for row in results:
        print(f"[{row.status}] {row.name}: {row.details}")
    total = len(results)
    passed = sum(1 for r in results if r.status == "PASS")
    warned = sum(1 for r in results if r.status == "WARN")
    failed = sum(1 for r in results if r.status == "FAIL")
    print(f"\nSummary: total={total} pass={passed} warn={warned} fail={failed}")


def _check_settings(settings: Settings) -> list[CheckResult]:
    out: list[CheckResult] = []
    if settings.mode != "live":
        out.append(_warn("mode", f"Current mode={settings.mode}. Use DELTA_MODE=live for real execution."))
    else:
        out.append(_ok("mode", "Live mode configured"))

    if not settings.api_key or not settings.api_secret:
        out.append(_fail("credentials", "DELTA_API_KEY/DELTA_API_SECRET are missing"))
    else:
        out.append(_ok("credentials", "API credentials found in environment"))

    if "india.delta.exchange" not in settings.api_url and "deltaex.org" not in settings.api_url:
        out.append(_warn("api_url", f"Non-standard API URL: {settings.api_url}"))
    else:
        out.append(_ok("api_url", settings.api_url))

    risk_ok = (
        settings.max_risk_per_trade > 0
        and settings.max_risk_per_trade <= 0.05
        and settings.max_daily_loss > 0
        and settings.max_daily_loss <= 0.2
        and settings.max_leverage > 0
        and settings.max_leverage <= 25
    )
    if risk_ok:
        out.append(
            _ok(
                "risk_limits",
                (
                    f"max_risk_per_trade={settings.max_risk_per_trade}, "
                    f"max_daily_loss={settings.max_daily_loss}, max_leverage={settings.max_leverage}"
                ),
            )
        )
    else:
        out.append(_warn("risk_limits", "One or more risk limits are outside recommended range"))
    return out


def _check_database(dsn: str) -> CheckResult:
    try:
        db = DatabaseManager(dsn)
        # Try a simple query to verify connection
        with db.get_session() as session:
            session.execute(text("SELECT 1"))
        return _ok("database", "PostgreSQL connection verified")
    except Exception as exc:
        return _fail("database", f"Database connection failed: {exc}")


def _check_public_api(client: DeltaClient) -> CheckResult:
    start = time.perf_counter()
    try:
        payload = client.get_products()
        elapsed = time.perf_counter() - start
        rows = []
        if isinstance(payload, dict):
            rows = payload.get("result") or payload.get("data") or []
        count = len(rows) if isinstance(rows, list) else 0
        return _ok("public_api", f"Connected; products={count}; latency={elapsed:.3f}s")
    except Exception as exc:
        return _fail("public_api", f"Failed to reach products endpoint: {exc}")


def _check_private_api(client: DeltaClient, settings: Settings) -> list[CheckResult]:
    out: list[CheckResult] = []

    start = time.perf_counter()
    try:
        balance = client.get_account_balance()
        elapsed = time.perf_counter() - start
        keys = list(balance.keys())[:5] if isinstance(balance, dict) else []
        out.append(_ok("auth_balance", f"Authenticated; latency={elapsed:.3f}s; payload_keys={keys}"))
        rows = balance.get("result") if isinstance(balance, dict) else []
        usd_available = None
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                asset_symbol = str(row.get("asset_symbol") or "").upper()
                if asset_symbol not in {"USD", "USDT"}:
                    continue
                try:
                    usd_available = float(row.get("available_balance", 0.0))
                except (TypeError, ValueError):
                    usd_available = 0.0
                break
        if usd_available is None:
            out.append(_warn("available_balance", "Could not locate USD/USDT available balance in account payload"))
        elif usd_available <= 0:
            out.append(_fail("available_balance", "Available USD/USDT balance is 0. Fund account before live trading"))
        else:
            out.append(_ok("available_balance", f"Available USD/USDT balance={usd_available:.6f}"))
    except DeltaAPIError as exc:
        out.append(_fail("auth_balance", f"Auth failed: {exc}"))
        return out
    except Exception as exc:
        out.append(_fail("auth_balance", f"Unexpected error: {exc}"))
        return out

    try:
        position_symbol = settings.trade_symbols[0] if settings.trade_symbols else None
        positions = client.get_positions(product_id=position_symbol) if position_symbol else client.get_positions()
        rows = []
        if isinstance(positions, dict):
            rows = positions.get("result") or positions.get("data") or []
        count = len(rows) if isinstance(rows, list) else 0
        if position_symbol:
            out.append(_ok("positions", f"Fetched open positions for {position_symbol}: count={count}"))
        else:
            out.append(_ok("positions", f"Fetched open positions count={count}"))
    except Exception as exc:
        out.append(_warn("positions", f"Could not fetch positions: {exc}"))

    try:
        open_orders = client.get_open_orders()
        rows = []
        if isinstance(open_orders, dict):
            rows = open_orders.get("result") or open_orders.get("data") or []
        count = len(rows) if isinstance(rows, list) else 0
        out.append(_ok("open_orders", f"Fetched open orders count={count}"))
    except Exception as exc:
        out.append(_warn("open_orders", f"Could not fetch open orders: {exc}"))
    return out


def main() -> None:
    settings = Settings(mode="live")
    results = _check_settings(settings)
    results.append(_check_database(settings.postgres_dsn))

    from sqlalchemy import text # Required for the SELECT 1 check

    client = DeltaClient(
        api_key=settings.api_key,
        api_secret=settings.api_secret,
        api_url=settings.api_url,
        ws_url=settings.ws_url,
    )
    results.append(_check_public_api(client))

    if settings.api_key and settings.api_secret:
        results.extend(_check_private_api(client, settings))
    else:
        results.append(_warn("private_api", "Skipped private API checks because credentials are missing"))

    _print_results(results)
    failed = any(r.status == "FAIL" for r in results)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
