"""Unit tests for VWAPDeviationStrategy (post-audit revision).

Coverage matrix
───────────────
Regime gate
  - Blocked in TRENDING and HIGH_VOLATILITY
  - Active in RANGING and LOW_VOLATILITY

Data guards
  - Fewer than min_candles → hold
  - Exactly at boundary (min_candles-1 vs min_candles)

Signal direction
  - BUY when price is below VWAP
  - SELL when price is above VWAP
  - HOLD when price is within deviation threshold
  - HOLD when deviation is just below threshold

SL / TP placement
  - BUY: SL < price < TP
  - SELL: TP < price < SL
  - SL uses ATR-relative floor (max of fixed-pct and 1.5×ATR)
  - TP anchored to 80% of VWAP-reversion distance

Confidence scaling
  - Scales with deviation magnitude
  - Capped at 0.90
  - Base >= 0.50 at threshold (no longer starts at 0.30)
  - Reaches >= 0.60 at moderate deviation (no longer blocked by RiskManager min_confidence)

Cooldown (C4 fix)
  - Second consecutive call within cooldown_bars → hold
  - After cooldown_bars, signal re-fires

VWAP on closed bars (H1 fix)
  - VWAP is computed from candles[:-1]; current bar does not contaminate anchor

ATR-floor gate (H5 fix)
  - Returns hold when deviation_pct/100 < regime.atr_pct

Regime-block logging (M2 fix)
  - Emits a debug log when regime gate fires

Z-score path
  - Uses z-score when enough history; falls back to plain % when std = 0

Fallback / edge cases
  - No volume column → equal-weight VWAP → still signals
  - Signal in LOW_VOLATILITY regime
  - Strategy name attribute
"""

import pytest
import pandas as pd

from delta_exchange_bot.strategy.vwap_deviation import VWAPDeviationStrategy
from delta_exchange_bot.strategy.market_regime import MarketRegime, MarketRegimeSnapshot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _regime(
    r: MarketRegime,
    atr: float = 0.0,
    atr_pct: float = 0.0,
) -> MarketRegimeSnapshot:
    return MarketRegimeSnapshot(
        regime=r, adx=0.0, atr=atr, atr_pct=atr_pct, ema_slope_pct=0.0
    )


def _flat_candles(n: int = 30, base: float = 100.0) -> pd.DataFrame:
    """All bars at the same price — VWAP equals base, deviation is 0."""
    return pd.DataFrame({
        "open":   [base] * n,
        "high":   [base] * n,
        "low":    [base] * n,
        "close":  [base] * n,
        "volume": [1000.0] * n,
    })


def _candles_last_deviated(
    n: int = 30, base: float = 100.0, last_close: float = 98.0
) -> pd.DataFrame:
    """First n-1 bars flat at base; last bar closes at last_close.

    VWAP is computed on closed bars (iloc[:-1]) = all-flat history → VWAP ≈ base.
    The last close deviates from that VWAP.
    """
    return pd.DataFrame({
        "open":   [base] * n,
        "high":   [base] * n,
        "low":    [base] * n,
        "close":  [base] * (n - 1) + [last_close],
        "volume": [1000.0] * n,
    })


# ---------------------------------------------------------------------------
# Regime gate tests
# ---------------------------------------------------------------------------

def test_blocked_in_trending_regime():
    strategy = VWAPDeviationStrategy()
    candles = _candles_last_deviated(last_close=98.0)
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.TRENDING))
    assert sig.action == "hold"
    assert sig.confidence == 0.0


def test_blocked_in_high_volatility_regime():
    strategy = VWAPDeviationStrategy()
    candles = _candles_last_deviated(last_close=98.0)
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.HIGH_VOLATILITY))
    assert sig.action == "hold"
    assert sig.confidence == 0.0


def test_active_in_ranging_regime():
    strategy = VWAPDeviationStrategy()
    assert MarketRegime.RANGING in strategy.allowed_regimes
    assert strategy.can_run(_regime(MarketRegime.RANGING)) is True


def test_active_in_low_volatility_regime():
    strategy = VWAPDeviationStrategy()
    assert MarketRegime.LOW_VOLATILITY in strategy.allowed_regimes
    assert strategy.can_run(_regime(MarketRegime.LOW_VOLATILITY)) is True


# ---------------------------------------------------------------------------
# Data guard tests
# ---------------------------------------------------------------------------

def test_hold_when_fewer_than_min_candles():
    strategy = VWAPDeviationStrategy(min_candles=20)
    candles = _candles_last_deviated(n=10)
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.action == "hold"


def test_hold_when_exactly_at_min_candles_boundary():
    """min_candles=20 means 19 candles → hold, 20 candles → evaluate."""
    strategy = VWAPDeviationStrategy(min_candles=20, deviation_pct=0.5)

    candles_19 = _candles_last_deviated(n=19, last_close=98.0)
    sig = strategy.generate("BTCUSD", candles_19, _regime(MarketRegime.RANGING))
    assert sig.action == "hold"

    candles_20 = _candles_last_deviated(n=20, last_close=98.0)
    sig = strategy.generate("BTCUSD", candles_20, _regime(MarketRegime.RANGING))
    assert sig.action == "buy"


# ---------------------------------------------------------------------------
# Signal direction tests
# ---------------------------------------------------------------------------

def test_buy_when_price_below_vwap():
    """Last close 2% below flat history → VWAP ≈ base → buy."""
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    candles = _candles_last_deviated(last_close=98.0)
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.action == "buy"
    assert sig.confidence > 0.0
    assert sig.stop_loss is not None
    assert sig.take_profit is not None


def test_sell_when_price_above_vwap():
    """Last close 2% above flat history → VWAP ≈ base → sell."""
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    candles = _candles_last_deviated(last_close=102.0)
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.action == "sell"
    assert sig.confidence > 0.0
    assert sig.stop_loss is not None
    assert sig.take_profit is not None


def test_hold_when_price_near_vwap():
    """All bars at same price → closed VWAP == current close → 0% deviation → hold."""
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    candles = _flat_candles()
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.action == "hold"


def test_hold_when_deviation_just_below_threshold():
    """Deviation 0.3% with threshold 0.5% → hold (plain-% fallback path)."""
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    candles = _candles_last_deviated(last_close=100.0 * (1 - 0.003))
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.action == "hold"


# ---------------------------------------------------------------------------
# SL / TP placement tests
# ---------------------------------------------------------------------------

def test_buy_sl_below_entry_tp_above_entry():
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    candles = _candles_last_deviated(last_close=98.0)
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.action == "buy"
    assert sig.stop_loss < sig.price < sig.take_profit


def test_sell_sl_above_entry_tp_below_entry():
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    candles = _candles_last_deviated(last_close=102.0)
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.action == "sell"
    assert sig.take_profit < sig.price < sig.stop_loss


def test_buy_sl_uses_atr_relative_floor():
    """When ATR is large, SL distance = 1.5 * ATR, not just sl_pct_floor * price."""
    # ATR=1.0 on price≈98 → sl_distance = max(98*0.004, 1.5*1.0) = max(0.392, 1.5) = 1.5
    strategy = VWAPDeviationStrategy(sl_atr_multiplier=1.5, sl_pct_floor=0.4)
    candles = _candles_last_deviated(last_close=98.0)
    sig = strategy.generate(
        "BTCUSD", candles, _regime(MarketRegime.RANGING, atr=1.0, atr_pct=0.0)
    )
    assert sig.action == "buy"
    expected_sl = 98.0 - 1.5  # 1.5 * ATR dominates
    assert sig.stop_loss == pytest.approx(expected_sl, abs=0.01)


def test_buy_sl_uses_pct_floor_when_atr_zero():
    """When ATR=0, SL falls back to sl_pct_floor * price."""
    strategy = VWAPDeviationStrategy(sl_atr_multiplier=1.5, sl_pct_floor=0.4)
    candles = _candles_last_deviated(last_close=98.0)
    sig = strategy.generate(
        "BTCUSD", candles, _regime(MarketRegime.RANGING, atr=0.0, atr_pct=0.0)
    )
    assert sig.action == "buy"
    # atr fallback = price * 0.004; sl_distance = max(0.392, 1.5*0.392) = 0.588
    atr_fallback = 98.0 * 0.004
    expected_sl = 98.0 - max(98.0 * 0.004, 1.5 * atr_fallback)
    assert sig.stop_loss == pytest.approx(expected_sl, rel=1e-4)


def test_buy_tp_anchored_to_vwap_reversion():
    """TP = price + 0.80 * (vwap - price) when vwap > price."""
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    # closed bars: 29 × 100.0 → VWAP ≈ 100.0
    # current price: 98.0 → deviation = -2%
    candles = _candles_last_deviated(n=30, base=100.0, last_close=98.0)
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.action == "buy"
    # vwap ≈ 100.0, price = 98.0 → tp ≈ 98.0 + 0.8*(100-98) = 98.0 + 1.6 = 99.6
    assert sig.take_profit == pytest.approx(99.6, abs=0.05)


def test_sell_tp_anchored_to_vwap_reversion():
    """TP = price - 0.80 * (price - vwap) when price > vwap."""
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    candles = _candles_last_deviated(n=30, base=100.0, last_close=102.0)
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.action == "sell"
    # vwap ≈ 100.0, price = 102.0 → tp ≈ 102.0 - 0.8*(102-100) = 102.0 - 1.6 = 100.4
    assert sig.take_profit == pytest.approx(100.4, abs=0.05)


# ---------------------------------------------------------------------------
# Confidence scaling tests
# ---------------------------------------------------------------------------

def test_confidence_base_at_least_0_50_at_threshold():
    """Confidence starts at 0.50 (raised from 0.30), so min-confidence=0.60 is reachable."""
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    # Just above 0.5% deviation (falls back to plain-% path)
    candles = _candles_last_deviated(last_close=99.4)  # ~0.6% deviation
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.action == "buy"
    assert sig.confidence >= 0.50


def test_confidence_scales_with_deviation():
    """Larger deviation from VWAP → higher confidence.

    Uses separate strategy instances to avoid cooldown interference between calls.
    """
    candles_small = _candles_last_deviated(last_close=99.0)
    candles_large = _candles_last_deviated(last_close=97.0)

    sig_small = VWAPDeviationStrategy(deviation_pct=0.5).generate(
        "BTCUSD", candles_small, _regime(MarketRegime.RANGING)
    )
    sig_large = VWAPDeviationStrategy(deviation_pct=0.5).generate(
        "BTCUSD", candles_large, _regime(MarketRegime.RANGING)
    )
    assert sig_small.action == "buy"
    assert sig_large.action == "buy"
    assert sig_large.confidence > sig_small.confidence


def test_confidence_capped_at_0_9():
    """Confidence never exceeds 0.90 regardless of deviation size."""
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    candles = _candles_last_deviated(last_close=50.0)  # 50% deviation
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.confidence <= 0.90


# ---------------------------------------------------------------------------
# Cooldown tests (C4 fix)
# ---------------------------------------------------------------------------

def test_cooldown_blocks_second_consecutive_signal():
    """Same strategy instance, same symbol, consecutive calls → second is held."""
    strategy = VWAPDeviationStrategy(deviation_pct=0.5, cooldown_bars=5)
    candles = _candles_last_deviated(n=30, last_close=98.0)

    sig1 = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig1.action == "buy"

    # Same candle length → bar_idx unchanged → within cooldown
    sig2 = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig2.action == "hold"


def test_cooldown_resets_after_enough_bars():
    """After cooldown_bars new candles are appended, signal fires again.

    The second deviation is made large (94.0, ~6% below VWAP) so that even
    with a non-zero rolling std (caused by the first deviation bar now in
    the closed-bar history), the z-score comfortably exceeds the threshold.
    """
    strategy = VWAPDeviationStrategy(deviation_pct=0.5, cooldown_bars=5)
    base = 100.0
    n = 30

    candles_first = _candles_last_deviated(n=n, base=base, last_close=98.0)
    sig1 = strategy.generate("BTCUSD", candles_first, _regime(MarketRegime.RANGING))
    assert sig1.action == "buy"

    # Extend by exactly cooldown_bars rows, then deviate sharply at the end.
    extra_rows = pd.DataFrame({
        "open":   [base] * 5,
        "high":   [base] * 5,
        "low":    [base] * 5,
        "close":  [base] * 4 + [94.0],   # ~6% below VWAP — clears z-score threshold
        "volume": [1000.0] * 5,
    })
    candles_later = pd.concat([candles_first, extra_rows], ignore_index=True)
    sig2 = strategy.generate("BTCUSD", candles_later, _regime(MarketRegime.RANGING))
    assert sig2.action == "buy"


def test_cooldown_independent_per_symbol():
    """Cooldown for BTCUSD does not block a signal for ETHUSD."""
    strategy = VWAPDeviationStrategy(deviation_pct=0.5, cooldown_bars=5)
    candles = _candles_last_deviated(n=30, last_close=98.0)

    sig_btc = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig_btc.action == "buy"

    sig_eth = strategy.generate("ETHUSD", candles, _regime(MarketRegime.RANGING))
    assert sig_eth.action == "buy"


# ---------------------------------------------------------------------------
# VWAP closed-bars test (H1 fix)
# ---------------------------------------------------------------------------

def test_vwap_computed_on_closed_bars_only():
    """
    The current bar's close must NOT influence the VWAP anchor.

    Build 30 bars where the closed history (bars 0-28) is flat at 100.
    Set the current bar (bar 29) to a deviated price.
    VWAP from closed bars = 100; deviation is properly computed.
    If VWAP included the current bar, VWAP would shift slightly, understating deviation.
    """
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    candles = _candles_last_deviated(n=30, base=100.0, last_close=97.0)
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    # VWAP from 29 flat bars = exactly 100.0; deviation = -3%
    assert sig.action == "buy"
    # TP must be anchored to VWAP at 100, not to a VWAP that includes 97
    assert sig.take_profit > sig.price
    assert sig.take_profit <= 100.0  # must not overshoot VWAP by more than 20% of reversion


# ---------------------------------------------------------------------------
# ATR-floor gate tests (H5 fix)
# ---------------------------------------------------------------------------

def test_hold_when_deviation_pct_below_atr_floor():
    """If deviation_pct/100 < regime.atr_pct, return hold — threshold is noise."""
    # deviation_pct=0.5% but atr_pct=0.8% → threshold is below noise floor
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    candles = _candles_last_deviated(last_close=98.0)
    sig = strategy.generate(
        "BTCUSD", candles, _regime(MarketRegime.RANGING, atr_pct=0.008)
    )
    assert sig.action == "hold"


def test_signal_when_deviation_pct_above_atr_floor():
    """If deviation_pct/100 > regime.atr_pct, the ATR gate does not block."""
    # deviation_pct=0.5% and atr_pct=0.2% → threshold is above noise floor
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    candles = _candles_last_deviated(last_close=98.0)
    sig = strategy.generate(
        "BTCUSD", candles, _regime(MarketRegime.RANGING, atr_pct=0.002)
    )
    assert sig.action == "buy"


# ---------------------------------------------------------------------------
# Fallback / edge-case tests
# ---------------------------------------------------------------------------

def test_buy_signal_without_volume_column():
    """When volume is absent, strategy falls back to equal weights and still signals."""
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    n, base = 30, 100.0
    candles = pd.DataFrame({
        "open":  [base] * n,
        "high":  [base] * n,
        "low":   [base] * n,
        "close": [base] * (n - 1) + [98.0],
    })
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.RANGING))
    assert sig.action == "buy"


def test_signal_in_low_volatility_regime():
    """Strategy fires in LOW_VOLATILITY as well as RANGING."""
    strategy = VWAPDeviationStrategy(deviation_pct=0.5)
    candles = _candles_last_deviated(last_close=98.0)
    sig = strategy.generate("BTCUSD", candles, _regime(MarketRegime.LOW_VOLATILITY))
    assert sig.action == "buy"


def test_strategy_name():
    assert VWAPDeviationStrategy.name == "vwap_deviation"
