"""
Delta Exchange Bot — Dashboard Backend
Run from: Algo_Trading/  →  python dashboard/server.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── Project root = parent of this file ────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent   # Algo_Trading/
SRC  = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# ── Bot module imports ─────────────────────────────────────────────────────────
try:
    from delta_exchange_bot.core.settings import Settings
    from delta_exchange_bot.persistence.db import DatabaseManager
    BOT_MODULES_OK = True
    _import_error  = ""
except Exception as exc:
    BOT_MODULES_OK = False
    _import_error  = str(exc)

# ── Global bot process ─────────────────────────────────────────────────────────
_bot_proc:       Optional[subprocess.Popen] = None
_bot_start_time: Optional[float]            = None
_bot_mode:       Optional[str]              = None
_bot_strategy:   Optional[str]              = None
_anomaly_buf:    deque                      = deque(maxlen=200)

# ── FastAPI ────────────────────────────────────────────────────────────────────
app = FastAPI(title="Delta Bot Dashboard", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Serve frontend at /

# ── Helpers ────────────────────────────────────────────────────────────────────
def _settings() -> Optional[Settings]:
    if not BOT_MODULES_OK:
        return None
    try:
        return Settings()
    except Exception:
        return None

def _get_db() -> Optional[DatabaseManager]:
    s = _settings()
    if not s:
        return None
    return DatabaseManager(s.postgres_dsn)

def _bot_running() -> bool:
    return _bot_proc is not None and _bot_proc.poll() is None

def _uptime_str(secs: float) -> str:
    s = int(secs)
    h, r = divmod(s, 3600)
    m, sec = divmod(r, 60)
    return f"{h}h {m}m {sec}s" if h else (f"{m}m {sec}s" if m else f"{sec}s")

def _log_level(line: str) -> str:
    u = line.upper()
    if "[CRITICAL]" in u or "CRITICAL" in u[:40]: return "CRITICAL"
    if "[ERROR]"    in u or "ERROR"    in u[:40]: return "ERROR"
    if "[WARNING]"  in u or "WARNING"  in u[:40]: return "WARNING"
    if "[DEBUG]"    in u:                          return "DEBUG"
    return "INFO"

def _tail(path: Path, n: int = 400) -> list[str]:
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.readlines()[-n:]
    except Exception:
        return []

# ══════════════════════════════════════════════════════════════════════════════
# REQUEST MODELS
# ══════════════════════════════════════════════════════════════════════════════
class StartReq(BaseModel):
    # Mirrors professional_bot.py argparse exactly
    mode:                   str           = "paper"      # --mode paper|live
    strategy:               str           = "portfolio"  # --strategy
    cycles:                 Optional[int] = None         # --cycles (omit = run forever)
    sleep_interval:         int           = 60           # --sleep-interval
    symbols:                Optional[str] = None         # --symbols BTCUSD,ETHUSD
    metrics_port:           int           = 8000         # --metrics-port
    metrics_addr:           str           = "0.0.0.0"   # --metrics-addr
    disable_metrics_server: bool          = False        # --disable-metrics-server flag

class EmergencyReq(BaseModel):
    confirm: bool = False

# ══════════════════════════════════════════════════════════════════════════════
# STATUS
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/status")
def get_status():
    running = _bot_running()
    uptime  = (time.time() - _bot_start_time) if (running and _bot_start_time) else 0

    open_pos = 0
    try:
        db = _get_db()
        if db:
            open_pos = len(db.get_all_active_positions())
    except Exception:
        pass

    return {
        "bot_running":    running,
        "mode":           _bot_mode     if running else "stopped",
        "strategy":       _bot_strategy if running else None,
        "uptime_seconds": int(uptime),
        "uptime_human":   _uptime_str(uptime),
        "pid":            _bot_proc.pid if (running and _bot_proc) else None,
        "open_positions": open_pos,
        "modules_ok":     BOT_MODULES_OK,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }

# ══════════════════════════════════════════════════════════════════════════════
# BOT CONTROL
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/bot/start")
def start_bot(req: StartReq):
    global _bot_proc, _bot_start_time, _bot_mode, _bot_strategy

    if _bot_running():
        raise HTTPException(409, "Bot is already running")

    run_script = ROOT / "scripts" / "run_bot.py"
    if not run_script.exists():
        raise HTTPException(500, f"run_bot.py not found at {run_script}")

    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_out = log_dir / "live_bot.out.log"
    log_err = log_dir / "live_bot.err.log"

    # Builds exactly the same command you run manually, e.g.:
    #   python scripts/run_bot.py --mode paper --strategy portfolio
    #   python scripts/run_bot.py --mode live --strategy portfolio --cycles 10
    cmd = [sys.executable, str(run_script),
           "--mode",           req.mode,
           "--strategy",       req.strategy,
           "--sleep-interval", str(req.sleep_interval),
           "--metrics-port",   str(req.metrics_port),
           "--metrics-addr",   req.metrics_addr]
    if req.cycles is not None:
        cmd += ["--cycles", str(req.cycles)]
    if req.symbols:
        cmd += ["--symbols", req.symbols]
    if req.disable_metrics_server:
        cmd += ["--disable-metrics-server"]

    env = {**os.environ, "DELTA_MODE": req.mode}

    with open(log_out, "a") as fo, open(log_err, "a") as fe:
        fo.write(f"\n{'='*60}\n[UI] Bot started {datetime.now().isoformat()} mode={req.mode} strategy={req.strategy}\n{'='*60}\n")
        _bot_proc = subprocess.Popen(cmd, stdout=fo, stderr=fe, env=env, cwd=str(ROOT))

    _bot_start_time = time.time()
    _bot_mode       = req.mode
    _bot_strategy   = req.strategy

    return {"status": "started", "pid": _bot_proc.pid, "mode": req.mode, "strategy": req.strategy}


@app.post("/api/bot/stop")
def stop_bot():
    global _bot_proc
    if not _bot_running():
        raise HTTPException(409, "Bot is not running")

    # Write shutdown signal file — this is the bot's own graceful shutdown mechanism
    sig = ROOT / "logs" / "bot.shutdown"
    sig.write_text("shutdown_requested_by_dashboard\n", encoding="utf-8")

    try:
        _bot_proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        _bot_proc.terminate()
        try:
            _bot_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _bot_proc.kill()

    pid = _bot_proc.pid
    _bot_proc = None
    return {"status": "stopped", "pid": pid}


@app.post("/api/bot/emergency-stop")
def emergency_stop(req: EmergencyReq):
    global _bot_proc
    if not req.confirm:
        raise HTTPException(400, "Set confirm=true to execute emergency stop")

    was_running = _bot_running()
    sig = ROOT / "logs" / "bot.shutdown"
    try:
        sig.write_text("EMERGENCY_STOP\n", encoding="utf-8")
    except Exception:
        pass

    if _bot_proc:
        try:
            _bot_proc.kill()
            _bot_proc.wait(timeout=3)
        except Exception:
            pass
        _bot_proc = None

    _anomaly_buf.append({
        "type": "EMERGENCY_STOP",
        "ts": datetime.now(timezone.utc).isoformat(),
        "msg": "Emergency stop triggered from dashboard",
        "severity": "CRITICAL",
    })
    return {"status": "emergency_stopped", "was_running": was_running}

# ══════════════════════════════════════════════════════════════════════════════
# PREFLIGHT  (re-uses scripts/live_preflight.py logic directly)
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/preflight")
def run_preflight():
    if not BOT_MODULES_OK:
        raise HTTPException(503, f"Bot modules unavailable: {_import_error}")

    from delta_exchange_bot.api.delta_client import DeltaAPIError, DeltaClient

    results = []
    s = _settings()

    # mode
    if s and s.mode == "live":
        results.append({"name":"mode","status":"PASS","details":"Live mode configured"})
    else:
        results.append({"name":"mode","status":"WARN","details":f"mode={getattr(s,'mode','unknown')} — set DELTA_MODE=live for real trading"})

    # credentials
    if s and s.api_key and s.api_secret:
        results.append({"name":"credentials","status":"PASS","details":"API key + secret found in environment"})
    else:
        results.append({"name":"credentials","status":"FAIL","details":"DELTA_API_KEY / DELTA_API_SECRET missing"})

    # api_url
    api_url = getattr(s, "api_url", "") if s else ""
    if "india.delta.exchange" in api_url or "deltaex.org" in api_url:
        results.append({"name":"api_url","status":"PASS","details":api_url})
    else:
        results.append({"name":"api_url","status":"WARN","details":f"Non-standard URL: {api_url}"})

    # risk limits
    if s:
        ok = 0 < s.max_risk_per_trade <= 0.05 and 0 < s.max_daily_loss <= 0.20 and 0 < s.max_leverage <= 25
        results.append({"name":"risk_limits",
                        "status":"PASS" if ok else "WARN",
                        "details":f"max_risk={s.max_risk_per_trade} max_daily_loss={s.max_daily_loss} max_leverage={s.max_leverage}"})
    else:
        results.append({"name":"risk_limits","status":"WARN","details":"Could not load settings"})

    # database
    try:
        db = _get_db()
        if db:
            cnt = len(db.get_all_active_positions())
            results.append({"name":"database","status":"PASS","details":f"PostgreSQL OK — {cnt} open position(s) recovered"})
        else:
             results.append({"name":"database","status":"FAIL","details":"Could not initialize DatabaseManager"})
    except Exception as exc:
        results.append({"name":"database","status":"FAIL","details":str(exc)})

    # public API
    if s:
        client = DeltaClient(api_key="", api_secret="", api_url=s.api_url)
        t0 = time.perf_counter()
        try:
            payload = client.get_products()
            ms = (time.perf_counter() - t0) * 1000
            rows = (payload.get("result") or payload.get("data") or []) if isinstance(payload, dict) else []
            results.append({"name":"public_api","status":"PASS","details":f"Connected · {len(rows)} products · {ms:.0f}ms"})
        except Exception as exc:
            ms = (time.perf_counter() - t0) * 1000
            results.append({"name":"public_api","status":"FAIL","details":f"{exc} · {ms:.0f}ms"})

        # authenticated API
        if s.api_key and s.api_secret:
            auth_client = DeltaClient(api_key=s.api_key, api_secret=s.api_secret, api_url=s.api_url)
            t0 = time.perf_counter()
            try:
                auth_client.get_account_balance()
                ms = (time.perf_counter() - t0) * 1000
                results.append({"name":"auth_api","status":"PASS","details":f"Authenticated · {ms:.0f}ms"})
            except DeltaAPIError as exc:
                ms = (time.perf_counter() - t0) * 1000
                results.append({"name":"auth_api","status":"FAIL","details":f"{exc} · {ms:.0f}ms"})
        else:
            results.append({"name":"auth_api","status":"WARN","details":"Skipped — no API credentials"})

    passed = sum(1 for r in results if r["status"]=="PASS")
    failed = sum(1 for r in results if r["status"]=="FAIL")
    warned = sum(1 for r in results if r["status"]=="WARN")

    return {
        "results": results,
        "summary": {"total":len(results),"passed":passed,"failed":failed,"warned":warned},
        "overall": "FAIL" if failed else ("WARN" if warned else "PASS"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# ══════════════════════════════════════════════════════════════════════════════
# POSITIONS  — reads open_position_state directly from state.db
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/positions")
def get_positions():
    db = _get_db()
    if not db:
        raise HTTPException(500, "Database unavailable")
    
    try:
        rows = db.get_all_active_positions()
    except Exception as exc:
        raise HTTPException(500, str(exc))

    positions = []
    for d in rows:
        entry  = float(d.get("avg_entry_price") or 0)
        size   = float(d.get("size") or 0)
        side   = d.get("side","")
        # sl     = d.get("stop_loss")
        # tp     = d.get("take_profit")
        
        # Estimated mark — real mark would come from exchange ticker
        mark   = entry * (1.001 if side == "long" else 0.999)
        pnl    = (mark - entry) * size if side == "long" else (entry - mark) * size
        pnl_pct = (pnl / (entry * size) * 100) if (entry * size) else 0

        positions.append({
            "symbol":           d["symbol"],
            "trade_id":         d.get("trade_id",""),
            "side":             side,
            "size":             size,
            "entry_price":      entry,
            "mark_price":       round(mark, 6),
            "unrealized_pnl":   round(pnl, 4),
            "pnl_pct":          round(pnl_pct, 3),
            "mode":             "live", # Default to live for dashboard view
            "updated_at":       d.get("updated_at",""),
        })

    return {"positions": positions, "count": len(positions),
            "timestamp": datetime.now(timezone.utc).isoformat()}

# ══════════════════════════════════════════════════════════════════════════════
# EXECUTION HISTORY  — reads execution_logs from state.db
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/trades/history")
def get_history(limit: int = Query(100, ge=1, le=500)):
    db = _get_db()
    if not db:
        raise HTTPException(500, "Database unavailable")
    try:
        trades = db.get_execution_history(limit=limit)
    except Exception as exc:
        raise HTTPException(500, str(exc))

    return {"trades": trades, "count": len(trades),
            "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/trades/orders")
def get_orders(limit: int = Query(50, ge=1, le=200)):
    try:
        with _db_conn() as c:
            rows = c.execute(
                "SELECT id,trade_id,order_id,symbol,side,order_type,size,price,status,ts FROM orders ORDER BY id DESC LIMIT ?",
                (limit,)
            ).fetchall()
    except Exception as exc:
        raise HTTPException(500, str(exc))
    return {"orders": [dict(r) for r in rows]}


@app.get("/api/trades/stats")
def get_stats(mode: str = Query("paper")):
    try:
        with _db_conn() as c:
            perf = c.execute(
                "SELECT * FROM performance_metrics WHERE mode=? ORDER BY id DESC LIMIT 1", (mode,)
            ).fetchone()
            sig_cnt = c.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
            ex_cnt  = c.execute("SELECT COUNT(*) FROM execution_logs WHERE mode=?", (mode,)).fetchone()[0]
    except Exception as exc:
        raise HTTPException(500, str(exc))

    result: dict = {}
    if perf:
        result = dict(perf)
        try:
            meta = json.loads(result.pop("metadata_json") or "{}")
            result["strategy_performance"] = meta.get("strategy_performance", {})
        except Exception:
            result.pop("metadata_json", None)
    result["total_signals"] = sig_cnt
    result["total_executions"] = ex_cnt
    return result

# ══════════════════════════════════════════════════════════════════════════════
# SIGNALS  — reads signals table
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/signals")
def get_signals(limit: int = Query(40, ge=1, le=200)):
    db = _get_db()
    if not db:
        raise HTTPException(500, "Database unavailable")
    try:
        signals = db.get_signals_history(limit=limit)
    except Exception as exc:
        raise HTTPException(500, str(exc))
    return {"signals": signals, "count": len(signals),
            "timestamp": datetime.now(timezone.utc).isoformat()}

# ══════════════════════════════════════════════════════════════════════════════
# RISK  — reads DB + settings
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/risk")
def get_risk():
    s = _settings()

    open_positions: list[dict] = []
    perf: dict = {}
    try:
        with _db_conn() as c:
            open_positions = [dict(r) for r in c.execute("SELECT * FROM open_position_state").fetchall()]
            row = c.execute("SELECT realized_pnl, max_drawdown FROM performance_metrics ORDER BY id DESC LIMIT 1").fetchone()
            if row:
                perf = dict(row)
    except Exception:
        pass

    total_notional = sum(
        float(p.get("size",0)) * float(p.get("entry_price",0))
        for p in open_positions
    )
    account_equity   = 100000.0 + float(perf.get("realized_pnl", 0))
    max_drawdown     = float(perf.get("max_drawdown", 0))
    max_leverage     = float(getattr(s,"max_leverage",10.0))     if s else 10.0
    max_daily_loss   = float(getattr(s,"max_daily_loss",0.05))   if s else 0.05
    max_positions    = int(getattr(s,"max_positions",5))         if s else 5
    max_risk_trade   = float(getattr(s,"max_risk_per_trade",0.01)) if s else 0.01
    leverage         = total_notional / account_equity if account_equity > 0 else 0
    lev_pct          = min(100, leverage / max_leverage * 100) if max_leverage else 0
    risk_status      = "SAFE" if lev_pct < 50 else ("WARNING" if lev_pct < 80 else "DANGER")

    return {
        "account_equity":         round(account_equity, 2),
        "total_notional":         round(total_notional, 2),
        "current_leverage":       round(leverage, 4),
        "max_leverage":           max_leverage,
        "leverage_utilization_pct": round(lev_pct, 2),
        "max_drawdown_pct":       round(max_drawdown, 4),
        "risk_status":            risk_status,
        "open_positions_count":   len(open_positions),
        "max_positions":          max_positions,
        "max_risk_per_trade":     max_risk_trade,
        "max_daily_loss":         max_daily_loss,
        "kill_switch_active":     False,
        "circuit_breaker_open":   False,
        "timestamp":              datetime.now(timezone.utc).isoformat(),
    }

# ══════════════════════════════════════════════════════════════════════════════
# SYSTEM METRICS
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/metrics")
def get_metrics():
    s = _settings()
    db_path  = ROOT / "state.db"
    log_out  = ROOT / "logs" / "live_bot.out.log"
    log_err  = ROOT / "logs" / "live_bot.err.log"

    # Measure live API latency
    api_latency_ms: Optional[float] = None
    api_ok = False
    if BOT_MODULES_OK and s:
        from delta_exchange_bot.api.delta_client import DeltaClient
        try:
            c  = DeltaClient(api_key="", api_secret="", api_url=s.api_url)
            t0 = time.perf_counter()
            c.get_markets()
            api_latency_ms = round((time.perf_counter() - t0) * 1000, 1)
            api_ok = True
        except Exception:
            pass

    return {
        "api_connected":    api_ok,
        "api_latency_ms":   api_latency_ms,
        "websocket_enabled":bool(getattr(s,"websocket_enabled",False)) if s else False,
        "exchange_env":     getattr(s,"exchange_env","unknown")         if s else "unknown",
        "trade_symbols":    list(getattr(s,"trade_symbols",[]))         if s else [],
        "bot_running":      _bot_running(),
        "bot_pid":          _bot_proc.pid if (_bot_running() and _bot_proc) else None,
        "db_size_kb":       round(db_path.stat().st_size / 1024, 1)  if db_path.exists() else 0,
        "log_out_kb":       round(log_out.stat().st_size  / 1024, 1) if log_out.exists() else 0,
        "log_err_kb":       round(log_err.stat().st_size  / 1024, 1) if log_err.exists() else 0,
        "modules_ok":       BOT_MODULES_OK,
        "timestamp":        datetime.now(timezone.utc).isoformat(),
    }

# ══════════════════════════════════════════════════════════════════════════════
# LOGS  — reads logs/live_bot.out.log  and  logs/live_bot.err.log
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/logs")
def get_logs(
    source: str = Query("stdout", enum=["stdout","stderr","both"]),
    lines:  int = Query(300, ge=10, le=2000),
    level:  str = Query("ALL"),
):
    out_path = ROOT / "logs" / "live_bot.out.log"
    err_path = ROOT / "logs" / "live_bot.err.log"
    all_lines: list[dict] = []

    if source in ("stdout","both"):
        for ln in _tail(out_path, lines):
            all_lines.append({"source":"stdout","raw":ln.rstrip(),"level":_log_level(ln)})
    if source in ("stderr","both"):
        for ln in _tail(err_path, lines):
            all_lines.append({"source":"stderr","raw":ln.rstrip(),"level":_log_level(ln)})

    if source == "both":
        all_lines = all_lines[-lines:]
    if level != "ALL":
        all_lines = [l for l in all_lines if l["level"] == level]

    return {"lines": all_lines, "count": len(all_lines),
            "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/logs/stream")
async def stream_logs(source: str = Query("stdout", enum=["stdout","stderr"])):
    """SSE stream — real tail -f behaviour."""
    path = ROOT / "logs" / ("live_bot.out.log" if source == "stdout" else "live_bot.err.log")

    async def _gen() -> AsyncGenerator[str, None]:
        offset = path.stat().st_size if path.exists() else 0
        yield f"data: {json.dumps({'type':'connected','source':source})}\n\n"
        while True:
            await asyncio.sleep(1)
            if not path.exists():
                continue
            try:
                cur_size = path.stat().st_size
                if cur_size < offset:
                    offset = 0          # log rotated
                if cur_size > offset:
                    with open(path,"r",encoding="utf-8",errors="replace") as f:
                        f.seek(offset)
                        data = f.read()
                        offset = f.tell()
                    for line in data.splitlines():
                        if not line.strip():
                            continue
                        payload = json.dumps({
                            "type":"log","source":source,
                            "level":_log_level(line),"raw":line,
                            "ts":datetime.now(timezone.utc).isoformat(),
                        })
                        yield f"data: {payload}\n\n"
            except Exception as exc:
                yield f"data: {json.dumps({'type':'error','message':str(exc)})}\n\n"

    return StreamingResponse(
        _gen(), media_type="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"},
    )

# ══════════════════════════════════════════════════════════════════════════════
# ANOMALIES
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/anomalies")
def get_anomalies():
    anomalies = list(_anomaly_buf)
    # Scan stderr for CRITICAL lines
    err_path = ROOT / "logs" / "live_bot.err.log"
    for ln in _tail(err_path, 200):
        if any(k in ln for k in ("CRITICAL","EMERGENCY","POSITION_MISMATCH","halt_trading")):
            anomalies.append({"type":"LOG_CRITICAL","ts":datetime.now(timezone.utc).isoformat(),
                               "msg":ln.strip()[:200],"severity":"CRITICAL"})
    return {"anomalies": anomalies[-50:], "count": len(anomalies),
            "timestamp": datetime.now(timezone.utc).isoformat()}


# ══════════════════════════════════════════════════════════════════════════════
# RUN TESTS  — runs python -m pytest from project root
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/run-tests")
def run_tests():
    """Run the project test suite:  python -m pytest"""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "--tb=short", "-q"],
            capture_output=True, text=True, cwd=str(ROOT), timeout=120
        )
        output = result.stdout + result.stderr
        return {
            "returncode": result.returncode,
            "passed":     result.returncode == 0,
            "output":     output,
            "timestamp":  datetime.now(timezone.utc).isoformat(),
        }
    except subprocess.TimeoutExpired:
        return {"returncode": -1, "passed": False, "output": "Timeout after 120s"}
    except Exception as exc:
        return {"returncode": -1, "passed": False, "output": str(exc)}


# ── Static files — MUST be mounted AFTER all @app routes ─────────────────────
# Serves dashboard/index.html at  http://localhost:8080/ui/index.html
from fastapi.responses import RedirectResponse

@app.get("/")
def root():
    """Redirect root to the dashboard."""
    return RedirectResponse(url="/ui/index.html")

app.mount("/ui", StaticFiles(directory=str(ROOT / "dashboard"), html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8080, reload=False, log_level="info")
