"""
Unit tests for the real OHLCV candle pipeline in TradingEngine (C2 root-cause fix).

Covers:
  - Ticker ticks no longer produce synthetic OHLCV bars
  - First candlestick_1m message stored as in-progress only
  - In-progress candle updated (H/L/C/V merge) on same-timestamp update
  - Previous bar flushed to _ohlcv_history when a new timestamp arrives
  - Gap warning fired when new timestamp > prev + 120 s
  - Zero-price candle messages silently skipped
  - bootstrap loads closed candles, skips last row, skips zero-price rows,
    marks symbol done and does not re-bootstrap on second call
  - bootstrap gracefully handles empty REST response (no data on testnet)
  - bootstrap sets _candle_bootstrap_done even on no-data result
  - _fetch_market_snapshot uses _ohlcv_history for the "df" field
  - _fetch_market_snapshot triggers inline REST fallback when buffer empty
    but bootstrap not yet done
"""
import pytest
from collections import deque

from delta_exchange_bot.core.engine import TradingEngine
from delta_exchange_bot.core.settings import Settings


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

class DummyDB:
    def create_trade(self, d): pass
    def close_trade(self, **kw): pass
    def log_execution(self, d): pass
    def update_position(self, d): pass
    def close_position(self, s): pass
    def get_active_position(self, s): return None


def make_engine(symbols=None):
    settings = Settings(mode="paper", trade_symbols=symbols or ["BTCUSD"])
    return TradingEngine(settings, db=DummyDB())


def _candle_payload(symbol, ts, o, h, l, c, v=0.0):
    """Build a candlestick_1m WS payload dict."""
    return {
        "type": "candlestick_1m",
        "payload": {
            "symbol": symbol,
            "candle_start_time": ts,
            "open": o, "high": h, "low": l, "close": c, "volume": v,
        },
    }


# ---------------------------------------------------------------------------
# 1. Ticker ticks no longer produce synthetic OHLCV bars
# ---------------------------------------------------------------------------

def test_ticker_message_does_not_append_to_ohlcv_history():
    engine = make_engine()
    msg = {
        "type": "v2/ticker",
        "payload": {"symbol": "BTCUSD", "mark_price": "42000"},
    }
    engine._on_ws_message(msg)
    assert len(engine._ohlcv_history["BTCUSD"]) == 0


def test_ticker_message_still_appends_to_price_history():
    engine = make_engine()
    engine._on_ws_message({
        "type": "v2/ticker",
        "payload": {"symbol": "BTCUSD", "mark_price": "42000"},
    })
    assert list(engine._price_history["BTCUSD"]) == pytest.approx([42000.0])


# ---------------------------------------------------------------------------
# 2. candlestick_1m — first message: stored as in-progress, NOT in history
# ---------------------------------------------------------------------------

def test_first_candle_message_stored_as_in_progress_only():
    engine = make_engine()
    engine._on_ws_message(_candle_payload("BTCUSD", 1000, 100, 105, 99, 103, 10))
    assert len(engine._ohlcv_history["BTCUSD"]) == 0
    assert engine._candle_in_progress["BTCUSD"]["open"] == 100.0
    assert engine._candle_in_progress["BTCUSD"]["timestamp"] == 1000


# ---------------------------------------------------------------------------
# 3. Same-timestamp update merges H/L/C/V in-place
# ---------------------------------------------------------------------------

def test_same_timestamp_updates_high_low_close_volume():
    engine = make_engine()
    engine._on_ws_message(_candle_payload("BTCUSD", 1000, 100, 105, 99, 103, 10))
    # second tick: same candle, price moves higher
    engine._on_ws_message(_candle_payload("BTCUSD", 1000, 100, 110, 98, 107, 15))

    assert len(engine._ohlcv_history["BTCUSD"]) == 0  # still not closed
    ip = engine._candle_in_progress["BTCUSD"]
    assert ip["high"] == pytest.approx(110.0)
    assert ip["low"]  == pytest.approx(98.0)
    assert ip["close"] == pytest.approx(107.0)
    assert ip["volume"] == pytest.approx(15.0)
    assert ip["open"] == pytest.approx(100.0)  # open must not change


# ---------------------------------------------------------------------------
# 4. New timestamp flushes previous bar to _ohlcv_history
# ---------------------------------------------------------------------------

def test_new_timestamp_flushes_previous_bar():
    engine = make_engine()
    engine._on_ws_message(_candle_payload("BTCUSD", 1000, 100, 105, 99, 103, 10))
    engine._on_ws_message(_candle_payload("BTCUSD", 1060, 103, 108, 101, 106, 12))

    history = list(engine._ohlcv_history["BTCUSD"])
    assert len(history) == 1
    closed = history[0]
    assert closed["open"]  == pytest.approx(100.0)
    assert closed["high"]  == pytest.approx(105.0)
    assert closed["low"]   == pytest.approx(99.0)
    assert closed["close"] == pytest.approx(103.0)
    assert closed["volume"] == pytest.approx(10.0)
    assert closed["timestamp"] == 1000

    # new in-progress candle is the second message
    assert engine._candle_in_progress["BTCUSD"]["timestamp"] == 1060


def test_multiple_bars_accumulate_in_history():
    engine = make_engine()
    for i in range(4):
        engine._on_ws_message(_candle_payload("BTCUSD", 1000 + i * 60, 100 + i, 110 + i, 90 + i, 105 + i, i + 1))
    # 3 closed bars (bar[0]..bar[2]); bar[3] still in-progress
    assert len(engine._ohlcv_history["BTCUSD"]) == 3


# ---------------------------------------------------------------------------
# 5. Gap detection (>120 s between consecutive candle timestamps)
# ---------------------------------------------------------------------------

def test_gap_detected_logs_warning(caplog):
    import logging
    engine = make_engine()
    engine._on_ws_message(_candle_payload("BTCUSD", 1000,  100, 105, 99, 103))
    with caplog.at_level(logging.WARNING):
        engine._on_ws_message(_candle_payload("BTCUSD", 1300, 103, 110, 101, 108))
    assert any("candle.gap_detected" in r.message for r in caplog.records)


def test_normal_1m_gap_no_warning(caplog):
    import logging
    engine = make_engine()
    engine._on_ws_message(_candle_payload("BTCUSD", 1000, 100, 105, 99, 103))
    with caplog.at_level(logging.WARNING):
        engine._on_ws_message(_candle_payload("BTCUSD", 1060, 103, 110, 101, 108))
    gap_records = [r for r in caplog.records if "candle.gap_detected" in r.message]
    assert len(gap_records) == 0


# ---------------------------------------------------------------------------
# 6. Zero-price candles skipped
# ---------------------------------------------------------------------------

def test_zero_open_candle_skipped():
    engine = make_engine()
    engine._on_ws_message(_candle_payload("BTCUSD", 1000, 0, 0, 0, 0))
    assert len(engine._ohlcv_history["BTCUSD"]) == 0
    assert "BTCUSD" not in engine._candle_in_progress


def test_zero_close_candle_skipped():
    engine = make_engine()
    engine._on_ws_message(_candle_payload("BTCUSD", 1000, 100, 105, 99, 0))
    assert "BTCUSD" not in engine._candle_in_progress


# ---------------------------------------------------------------------------
# 7. Bootstrap: REST data loaded, last row excluded
# ---------------------------------------------------------------------------

def _make_rest_rows(n, base_ts=1000, base_price=100.0):
    """Generate n synthetic REST candle rows."""
    return [
        {
            "time": base_ts + i * 60,
            "open":   base_price + i,
            "high":   base_price + i + 5,
            "low":    base_price + i - 5,
            "close":  base_price + i + 2,
            "volume": float(i + 1),
        }
        for i in range(n)
    ]


def test_bootstrap_loads_closed_rows_skips_last(monkeypatch):
    engine = make_engine()
    rows = _make_rest_rows(5)
    monkeypatch.setattr(engine.api, "get_candles", lambda **kw: {"result": rows})

    engine._bootstrap_candle_history("BTCUSD")

    # 5 rows → 4 closed (last excluded as potentially open bar)
    assert len(engine._ohlcv_history["BTCUSD"]) == 4
    assert "BTCUSD" in engine._candle_bootstrap_done


def test_bootstrap_candles_have_valid_ohlc(monkeypatch):
    engine = make_engine()
    rows = _make_rest_rows(3)
    monkeypatch.setattr(engine.api, "get_candles", lambda **kw: {"result": rows})

    engine._bootstrap_candle_history("BTCUSD")

    for candle in engine._ohlcv_history["BTCUSD"]:
        assert candle["high"] >= candle["low"]
        assert candle["high"] != candle["low"]   # not synthetic (H≠L)
        assert candle["open"] > 0
        assert candle["close"] > 0


def test_bootstrap_skips_zero_price_rows(monkeypatch):
    engine = make_engine()
    rows = _make_rest_rows(3)
    rows[1]["open"] = 0   # inject a bad row
    monkeypatch.setattr(engine.api, "get_candles", lambda **kw: {"result": rows})

    engine._bootstrap_candle_history("BTCUSD")

    # row[0] closed, row[1] zero→skipped, row[2] is the "open" last row→excluded
    # so only row[0] makes it in
    assert len(engine._ohlcv_history["BTCUSD"]) == 1


def test_bootstrap_does_not_run_twice(monkeypatch):
    engine = make_engine()
    call_count = {"n": 0}

    def fake_candles(**kw):
        call_count["n"] += 1
        return {"result": _make_rest_rows(3)}

    monkeypatch.setattr(engine.api, "get_candles", fake_candles)

    engine._bootstrap_candle_history("BTCUSD")
    engine._bootstrap_candle_history("BTCUSD")  # second call should be a no-op

    assert call_count["n"] == 1


# ---------------------------------------------------------------------------
# 8. Bootstrap: empty REST response (testnet no-data)
# ---------------------------------------------------------------------------

def test_bootstrap_empty_response_marks_done(monkeypatch):
    engine = make_engine()
    monkeypatch.setattr(engine.api, "get_candles", lambda **kw: {"result": []})

    engine._bootstrap_candle_history("BTCUSD")

    assert "BTCUSD" in engine._candle_bootstrap_done
    assert len(engine._ohlcv_history["BTCUSD"]) == 0


def test_bootstrap_api_exception_does_not_raise(monkeypatch):
    engine = make_engine()

    def fail(**kw):
        raise RuntimeError("network error")

    monkeypatch.setattr(engine.api, "get_candles", fail)
    # Must not propagate
    engine._bootstrap_candle_history("BTCUSD")
    # Symbol NOT added to done set (retry is allowed next cycle)
    assert "BTCUSD" not in engine._candle_bootstrap_done


# ---------------------------------------------------------------------------
# 9. _fetch_market_snapshot: df column is built from _ohlcv_history
# ---------------------------------------------------------------------------

def test_fetch_market_snapshot_df_from_ohlcv_history(monkeypatch):
    engine = make_engine(["BTCUSD"])
    monkeypatch.setattr(engine.api, "get_ticker", lambda s: {"result": {"mark_price": "42000"}})

    # Pre-populate with two valid closed candles
    for i in range(2):
        engine._ohlcv_history["BTCUSD"].append({
            "open": 100 + i, "high": 110 + i, "low": 90 + i,
            "close": 105 + i, "volume": 10.0, "timestamp": 1000 + i * 60,
        })
    # Bypass bootstrap to avoid real API call
    engine._candle_bootstrap_done.add("BTCUSD")

    snap = engine._fetch_market_snapshot()
    df = snap["BTCUSD"]["df"]
    assert df is not None
    assert list(df.columns) >= ["open", "high", "low", "close"]
    assert len(df) == 2
    # Verify H ≠ L (not synthetic)
    assert (df["high"] != df["low"]).all()


def test_fetch_market_snapshot_df_is_none_when_no_candles(monkeypatch):
    engine = make_engine(["BTCUSD"])
    monkeypatch.setattr(engine.api, "get_ticker", lambda s: {"result": {"mark_price": "42000"}})
    # No candles, bootstrap already done → no inline fallback
    engine._candle_bootstrap_done.add("BTCUSD")

    snap = engine._fetch_market_snapshot()
    assert snap["BTCUSD"]["df"] is None


# ---------------------------------------------------------------------------
# 10. _fetch_market_snapshot: triggers inline REST fallback when buffer empty
# ---------------------------------------------------------------------------

def test_fetch_market_snapshot_triggers_inline_rest_fallback(monkeypatch):
    engine = make_engine(["BTCUSD"])
    monkeypatch.setattr(engine.api, "get_ticker", lambda s: {"result": {"mark_price": "50000"}})

    bootstrap_calls = {"n": 0}
    original_bootstrap = engine._bootstrap_candle_history

    def fake_bootstrap(sym):
        bootstrap_calls["n"] += 1
        # Simulate loading 2 candles
        engine._ohlcv_history[sym].append({
            "open": 200, "high": 210, "low": 190, "close": 205,
            "volume": 5, "timestamp": 9000,
        })
        engine._candle_bootstrap_done.add(sym)

    monkeypatch.setattr(engine, "_bootstrap_candle_history", fake_bootstrap)

    snap = engine._fetch_market_snapshot()

    assert bootstrap_calls["n"] == 1
    assert snap["BTCUSD"]["df"] is not None


# ---------------------------------------------------------------------------
# 11. Alternate timestamp field names handled (start_time, timestamp, time)
# ---------------------------------------------------------------------------

def test_candle_message_accepts_start_time_field():
    engine = make_engine()
    payload = {
        "type": "candlestick_1m",
        "payload": {
            "symbol": "BTCUSD",
            "start_time": 2000,
            "open": 200, "high": 210, "low": 195, "close": 205, "volume": 3,
        },
    }
    engine._on_ws_message(payload)
    assert engine._candle_in_progress["BTCUSD"]["timestamp"] == 2000


def test_candle_message_accepts_time_field():
    engine = make_engine()
    payload = {
        "type": "candlestick_1m",
        "payload": {
            "symbol": "BTCUSD",
            "time": 3000,
            "open": 300, "high": 310, "low": 295, "close": 305, "volume": 5,
        },
    }
    engine._on_ws_message(payload)
    assert engine._candle_in_progress["BTCUSD"]["timestamp"] == 3000
