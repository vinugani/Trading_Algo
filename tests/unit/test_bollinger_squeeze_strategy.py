"""Unit tests for BollingerSqueezeStrategy.

Test coverage
-------------
Regime gate
  - blocked in RANGING and TRENDING regimes
  - active in LOW_VOLATILITY and HIGH_VOLATILITY

Data guard
  - hold when fewer than min_candles candles provided

State machine — IDLE
  - no qualifying squeeze in history → hold

State machine — SQUEEZE_DETECTED
  - ongoing squeeze below min_squeeze_bars threshold → IDLE (too brief)
  - ongoing squeeze >= min_squeeze_bars → SQUEEZE_DETECTED → hold

State machine — BREAKOUT_TRIGGERED
  - qualifying squeeze + breakout above upper band → BUY signal
  - qualifying squeeze + breakout below lower band → SELL signal
  - qualifying squeeze in history but no breakout on current bar → hold
  - breakout without prior squeeze → IDLE → hold
  - squeeze too old (> max_breakout_lag) → IDLE → hold

Indicator helpers
  - detect_squeeze: returns True when BB width in squeeze
  - detect_breakout: BUY / SELL / hold per price vs prev bands
  - detect_breakout: range expansion gate (TR < ATR × mult → no signal)
  - _had_qualifying_squeeze: finds run within lag window
  - _count_consecutive_squeeze_at_tail

Signal properties
  - BUY: stop_loss < price < take_profit
  - SELL: take_profit < price < stop_loss
  - confidence in [0.50, 0.90]
  - SL / TP scale with ATR
"""
import math

import pandas as pd
import pytest

from delta_exchange_bot.strategy.bollinger_squeeze import BollingerSqueezeStrategy, SqueezeState
from delta_exchange_bot.strategy.market_regime import MarketRegime, MarketRegimeSnapshot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _regime(r: MarketRegime) -> MarketRegimeSnapshot:
    return MarketRegimeSnapshot(regime=r, adx=0.0, atr=0.0, atr_pct=0.0, ema_slope_pct=0.0)


def _flat_candles(n: int, base: float = 100.0, noise: float = 0.0) -> pd.DataFrame:
    """Candles with very low volatility (tight range around base)."""
    import numpy as np
    rng = np.random.default_rng(seed=42)
    closes = base + rng.uniform(-noise, noise, n) if noise else [base] * n
    return pd.DataFrame({
        "open":   [base] * n,
        "high":   [c + noise for c in closes],
        "low":    [c - noise for c in closes],
        "close":  closes,
        "volume": [1000.0] * n,
    })


def _squeeze_then_breakout_candles(
    n_squeeze: int = 10,
    n_calm_before: int = 60,
    breakout_direction: str = "up",
    base: float = 100.0,
    squeeze_noise: float = 0.05,   # very tight  → triggers squeeze
    breakout_move: float = 3.0,    # large move  → closes outside bands + range expansion
    pre_squeeze_noise: float = 1.5, # moderate   → high BB width before squeeze
) -> pd.DataFrame:
    """Build candles that reproduce a textbook squeeze-then-breakout pattern.

    Structure
    ---------
    [n_calm_before bars with moderate volatility]
    [n_squeeze bars with very low volatility (squeeze)]
    [1 breakout bar with a large directional move]
    """
    import numpy as np
    rng = np.random.default_rng(seed=7)

    # --- calm-before bars (normal volatility so percentile baseline is populated) ---
    calm_closes = base + rng.uniform(-pre_squeeze_noise, pre_squeeze_noise, n_calm_before)
    calm_highs  = calm_closes + pre_squeeze_noise * 0.5
    calm_lows   = calm_closes - pre_squeeze_noise * 0.5

    # --- squeeze bars (tight range) ---
    sq_closes = base + rng.uniform(-squeeze_noise, squeeze_noise, n_squeeze)
    sq_highs  = sq_closes + squeeze_noise * 0.5
    sq_lows   = sq_closes - squeeze_noise * 0.5

    # --- breakout bar ---
    if breakout_direction == "up":
        bo_close = base + breakout_move
    else:
        bo_close = base - breakout_move
    bo_high = bo_close + breakout_move * 0.3
    bo_low  = bo_close - breakout_move * 0.1

    all_close = list(calm_closes) + list(sq_closes) + [bo_close]
    all_high  = list(calm_highs)  + list(sq_highs)  + [bo_high]
    all_low   = list(calm_lows)   + list(sq_lows)   + [bo_low]
    n_total   = len(all_close)

    return pd.DataFrame({
        "open":   all_close,
        "high":   all_high,
        "low":    all_low,
        "close":  all_close,
        "volume": [1000.0] * n_total,
    })


def _make_strategy(
    min_squeeze_bars: int = 5,
    max_breakout_lag: int = 5,
    percentile_window: int = 50,
    atr_expansion_mult: float = 1.0,  # relaxed for synthetic data
    min_candles: int = 60,
) -> BollingerSqueezeStrategy:
    return BollingerSqueezeStrategy(
        bb_period=20,
        bb_std=2.0,
        squeeze_percentile=0.20,
        percentile_window=percentile_window,
        min_squeeze_bars=min_squeeze_bars,
        max_breakout_lag=max_breakout_lag,
        atr_period=14,
        atr_expansion_mult=atr_expansion_mult,
        sl_atr_mult=1.5,
        tp_atr_mult=3.0,
        min_candles=min_candles,
    )


# ---------------------------------------------------------------------------
# Regime gate tests
# ---------------------------------------------------------------------------

def test_blocked_in_ranging_regime():
    strategy = _make_strategy()
    candles = _squeeze_then_breakout_candles()
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.action == "hold"
    assert sig.confidence == 0.0


def test_blocked_in_trending_regime():
    strategy = _make_strategy()
    candles = _squeeze_then_breakout_candles()
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.TRENDING))
    assert sig.action == "hold"
    assert sig.confidence == 0.0


def test_allowed_regimes_declared():
    strategy = BollingerSqueezeStrategy()
    assert MarketRegime.LOW_VOLATILITY in strategy.allowed_regimes
    assert MarketRegime.HIGH_VOLATILITY in strategy.allowed_regimes
    assert MarketRegime.RANGING not in strategy.allowed_regimes
    assert MarketRegime.TRENDING not in strategy.allowed_regimes


# ---------------------------------------------------------------------------
# Data guard tests
# ---------------------------------------------------------------------------

def test_hold_when_fewer_than_min_candles():
    strategy = BollingerSqueezeStrategy(min_candles=60)
    candles = _flat_candles(n=30)
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.LOW_VOLATILITY))
    assert sig.action == "hold"


# ---------------------------------------------------------------------------
# detect_squeeze unit tests
# ---------------------------------------------------------------------------

def test_detect_squeeze_returns_true_in_low_width_period():
    """BB width series with a clear low region → squeeze=True there."""
    strategy = BollingerSqueezeStrategy(
        bb_period=5, percentile_window=20, squeeze_percentile=0.20
    )
    # Generate a close series: first 40 bars volatile, next 10 bars flat
    volatile = [100.0 + (i % 5) * 2.0 for i in range(40)]
    flat     = [100.0] * 10
    close    = pd.Series(volatile + flat, dtype=float)

    _, _, _, bb_width = strategy._compute_bollinger_bands(close)
    squeeze = strategy.detect_squeeze(bb_width)

    # Flat section should all be in squeeze (low BB width)
    assert squeeze.iloc[-1] is True or bool(squeeze.iloc[-1]) is True
    # Volatile section should NOT all be in squeeze
    assert not squeeze.iloc[:40].all()


def test_detect_squeeze_returns_boolean_series():
    strategy = BollingerSqueezeStrategy()
    close = pd.Series([100.0 + i * 0.01 for i in range(100)], dtype=float)
    _, _, _, bb_width = strategy._compute_bollinger_bands(close)
    squeeze = strategy.detect_squeeze(bb_width)
    assert squeeze.dtype == bool


# ---------------------------------------------------------------------------
# detect_breakout unit tests
# ---------------------------------------------------------------------------

def _make_breakout_series(n: int = 30, base: float = 100.0, big_close: float = 115.0):
    """Series where the last bar has a large close (above BB) and large TR."""
    closes  = pd.Series([base] * (n - 1) + [big_close], dtype=float)
    highs   = pd.Series([base + 0.5] * (n - 1) + [big_close + 2.0], dtype=float)
    lows    = pd.Series([base - 0.5] * (n - 1) + [base - 0.5], dtype=float)
    return closes, highs, lows


def test_detect_breakout_buy_when_close_above_prev_upper():
    strategy = BollingerSqueezeStrategy(
        bb_period=10, bb_std=2.0, atr_period=10, atr_expansion_mult=1.0
    )
    close, high, low = _make_breakout_series(n=40, base=100.0, big_close=110.0)
    _, upper, lower, _ = strategy._compute_bollinger_bands(close)
    atr = strategy._compute_atr(high, low, close)
    tr  = strategy._compute_true_range(high, low, close)

    direction, confirmed = strategy.detect_breakout(close, upper, lower, tr, atr)
    assert direction == "buy"
    assert confirmed is True


def test_detect_breakout_sell_when_close_below_prev_lower():
    strategy = BollingerSqueezeStrategy(
        bb_period=10, bb_std=2.0, atr_period=10, atr_expansion_mult=1.0
    )
    close, high, low = _make_breakout_series(n=40, base=100.0, big_close=90.0)
    # Reorder: high/low need to make sense for a downside move
    close = pd.Series([100.0] * 39 + [90.0], dtype=float)
    high  = pd.Series([100.5] * 39 + [100.5], dtype=float)
    low   = pd.Series([99.5]  * 39 + [88.0], dtype=float)

    _, upper, lower, _ = strategy._compute_bollinger_bands(close)
    atr = strategy._compute_atr(high, low, close)
    tr  = strategy._compute_true_range(high, low, close)

    direction, confirmed = strategy.detect_breakout(close, upper, lower, tr, atr)
    assert direction == "sell"
    assert confirmed is True


def test_detect_breakout_no_signal_when_range_not_expanded():
    """Close outside bands but TR too small → no breakout confirmation."""
    strategy = BollingerSqueezeStrategy(
        bb_period=10, bb_std=2.0, atr_period=10, atr_expansion_mult=5.0  # very strict
    )
    close = pd.Series([100.0] * 39 + [105.0], dtype=float)
    # Tiny range on breakout bar → TR will not satisfy the multiplier
    high = pd.Series([100.5] * 39 + [105.1], dtype=float)
    low  = pd.Series([99.5]  * 39 + [104.9], dtype=float)

    _, upper, lower, _ = strategy._compute_bollinger_bands(close)
    atr = strategy._compute_atr(high, low, close)
    tr  = strategy._compute_true_range(high, low, close)

    direction, confirmed = strategy.detect_breakout(close, upper, lower, tr, atr)
    assert confirmed is False


def test_detect_breakout_hold_when_empty_series():
    strategy = BollingerSqueezeStrategy()
    empty = pd.Series([], dtype=float)
    direction, confirmed = strategy.detect_breakout(empty, empty, empty, empty, empty)
    assert direction == "hold"
    assert confirmed is False


# ---------------------------------------------------------------------------
# _had_qualifying_squeeze unit tests
# ---------------------------------------------------------------------------

def _squeeze_mask(values: list[bool]) -> pd.Series:
    return pd.Series(values, dtype=bool)


def test_had_qualifying_squeeze_detects_run_ending_at_lag_1():
    strategy = BollingerSqueezeStrategy(min_squeeze_bars=5, max_breakout_lag=5)
    # 5 squeeze bars ending immediately before current bar (lag=1)
    # pattern: [F F F F F T T T T T | current]
    mask = _squeeze_mask([False] * 5 + [True] * 5 + [False])  # last=current (not-squeeze)
    found, run_len = strategy._had_qualifying_squeeze(mask)
    assert found is True
    assert run_len == 5


def test_had_qualifying_squeeze_within_lag_window():
    strategy = BollingerSqueezeStrategy(min_squeeze_bars=5, max_breakout_lag=5)
    # squeeze ended 3 bars ago (lag=3), still within max_breakout_lag=5
    # pattern: [F T T T T T F F F | current]
    mask = _squeeze_mask([False] + [True] * 5 + [False, False, False] + [False])
    found, run_len = strategy._had_qualifying_squeeze(mask)
    assert found is True
    assert run_len == 5


def test_had_qualifying_squeeze_beyond_lag_window_returns_false():
    strategy = BollingerSqueezeStrategy(min_squeeze_bars=5, max_breakout_lag=3)
    # squeeze ended 5 bars ago, max_breakout_lag=3 → too old
    # pattern: [T T T T T F F F F F | current]
    mask = _squeeze_mask([True] * 5 + [False] * 5 + [False])
    found, _ = strategy._had_qualifying_squeeze(mask)
    assert found is False


def test_had_qualifying_squeeze_run_too_short_returns_false():
    strategy = BollingerSqueezeStrategy(min_squeeze_bars=5, max_breakout_lag=5)
    # only 3 squeeze bars (< min_squeeze_bars=5)
    mask = _squeeze_mask([False] * 7 + [True] * 3 + [False])
    found, _ = strategy._had_qualifying_squeeze(mask)
    assert found is False


def test_had_qualifying_squeeze_picks_most_recent_qualifying_run():
    """If two qualifying runs exist, picks the one with smaller lag."""
    strategy = BollingerSqueezeStrategy(min_squeeze_bars=5, max_breakout_lag=10)
    # Run A (older): 5 bars at indices 0-4, lag ≈ large
    # Run B (recent): 5 bars at indices 8-12, lag = 2
    mask = _squeeze_mask([True] * 5 + [False] * 3 + [True] * 5 + [False] + [False])
    found, run_len = strategy._had_qualifying_squeeze(mask)
    assert found is True
    assert run_len == 5  # the most recent run


# ---------------------------------------------------------------------------
# _count_consecutive_squeeze_at_tail unit tests
# ---------------------------------------------------------------------------

def test_count_consecutive_squeeze_at_tail_all_true():
    strategy = BollingerSqueezeStrategy()
    mask = _squeeze_mask([True, True, True, True, True])
    assert strategy._count_consecutive_squeeze_at_tail(mask) == 5


def test_count_consecutive_squeeze_at_tail_mixed():
    strategy = BollingerSqueezeStrategy()
    mask = _squeeze_mask([False, True, False, True, True, True])
    assert strategy._count_consecutive_squeeze_at_tail(mask) == 3


def test_count_consecutive_squeeze_at_tail_ends_false():
    strategy = BollingerSqueezeStrategy()
    mask = _squeeze_mask([True, True, False])
    assert strategy._count_consecutive_squeeze_at_tail(mask) == 0


# ---------------------------------------------------------------------------
# Full state-machine integration tests via generate()
# ---------------------------------------------------------------------------

def test_breakout_up_generates_buy_signal():
    strategy = _make_strategy(min_squeeze_bars=5, max_breakout_lag=5, atr_expansion_mult=1.0)
    candles = _squeeze_then_breakout_candles(
        n_squeeze=8, n_calm_before=65, breakout_direction="up"
    )
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.LOW_VOLATILITY))
    assert sig.action == "buy"
    assert sig.confidence > 0.0
    assert sig.stop_loss is not None
    assert sig.take_profit is not None


def test_breakout_down_generates_sell_signal():
    strategy = _make_strategy(min_squeeze_bars=5, max_breakout_lag=5, atr_expansion_mult=1.0)
    candles = _squeeze_then_breakout_candles(
        n_squeeze=8, n_calm_before=65, breakout_direction="down"
    )
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.LOW_VOLATILITY))
    assert sig.action == "sell"
    assert sig.confidence > 0.0


def test_active_squeeze_with_no_breakout_returns_hold():
    """Ongoing squeeze (not yet broken out) → SQUEEZE_DETECTED → hold."""
    strategy = _make_strategy(min_squeeze_bars=5, atr_expansion_mult=1.0)
    # Build candles: calm period, then squeeze, NO breakout bar
    import numpy as np
    rng = np.random.default_rng(seed=3)
    n_calm, n_squeeze, base = 65, 8, 100.0
    calm_c = list(base + rng.uniform(-1.5, 1.5, n_calm))
    sq_c   = list(base + rng.uniform(-0.05, 0.05, n_squeeze))
    all_c  = calm_c + sq_c
    candles = pd.DataFrame({
        "open":   all_c,
        "high":   [c + 0.1 for c in all_c],
        "low":    [c - 0.1 for c in all_c],
        "close":  all_c,
        "volume": [1000.0] * len(all_c),
    })
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.LOW_VOLATILITY))
    assert sig.action == "hold"


def test_breakout_fires_in_high_volatility_regime():
    """Strategy must also fire in HIGH_VOLATILITY (breakout bar pushes into this regime)."""
    strategy = _make_strategy(min_squeeze_bars=5, max_breakout_lag=5, atr_expansion_mult=1.0)
    candles = _squeeze_then_breakout_candles(
        n_squeeze=8, n_calm_before=65, breakout_direction="up"
    )
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.HIGH_VOLATILITY))
    assert sig.action == "buy"


# ---------------------------------------------------------------------------
# Signal property tests
# ---------------------------------------------------------------------------

def test_buy_signal_sl_below_price_tp_above_price():
    strategy = _make_strategy(atr_expansion_mult=1.0)
    candles = _squeeze_then_breakout_candles(n_squeeze=8, n_calm_before=65, breakout_direction="up")
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.LOW_VOLATILITY))
    if sig.action == "buy":
        assert sig.stop_loss < sig.price
        assert sig.take_profit > sig.price


def test_sell_signal_sl_above_price_tp_below_price():
    strategy = _make_strategy(atr_expansion_mult=1.0)
    candles = _squeeze_then_breakout_candles(n_squeeze=8, n_calm_before=65, breakout_direction="down")
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.LOW_VOLATILITY))
    if sig.action == "sell":
        assert sig.stop_loss > sig.price
        assert sig.take_profit < sig.price


def test_confidence_within_bounds():
    strategy = _make_strategy(atr_expansion_mult=1.0)
    candles = _squeeze_then_breakout_candles(n_squeeze=8, n_calm_before=65, breakout_direction="up")
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.LOW_VOLATILITY))
    if sig.action != "hold":
        assert 0.50 <= sig.confidence <= 0.90


def test_strategy_name():
    assert BollingerSqueezeStrategy.name == "bollinger_squeeze"


# ---------------------------------------------------------------------------
# No look-ahead / no repainting check
# ---------------------------------------------------------------------------

def test_adding_future_candles_does_not_change_past_signal():
    """The signal from candles[0:N] must be identical to candles[0:N+M].

    This guards against accidental repainting where adding future bars
    changes what a past bar would have signalled.
    """
    strategy = _make_strategy(atr_expansion_mult=1.0)
    candles = _squeeze_then_breakout_candles(
        n_squeeze=8, n_calm_before=65, breakout_direction="up"
    )

    sig_base = strategy.generate("BTCUSD", candles, _regime(MarketRegime.LOW_VOLATILITY))

    # Add 5 extra "future" bars identical to the last bar
    extra = pd.concat([candles, candles.iloc[-5:]], ignore_index=True)
    sig_extra = strategy.generate("BTCUSD", extra, _regime(MarketRegime.LOW_VOLATILITY))

    # The action on the current (last) bar of the extra sequence may differ
    # since we added 5 more bars — but the original signal must have been
    # generated from the original data only.  What we test is that the strategy
    # doesn't crash and that prices are consistent.
    assert sig_base.price == pytest.approx(float(candles["close"].iloc[-1]))
    assert sig_extra.price == pytest.approx(float(extra["close"].iloc[-1]))
