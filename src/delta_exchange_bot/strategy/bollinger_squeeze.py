"""
Bollinger Band Squeeze strategy — volatility compression → expansion breakout.

Design principles
-----------------
* State machine with four explicit states: IDLE, SQUEEZE_DETECTED,
  BREAKOUT_TRIGGERED.  The active state is *derived* from candle history on
  every call so that the strategy is fully stateless and works correctly in
  both live and backtest modes with no look-ahead bias.
* Breakout confirmation requires TWO independent signals:
    1. Candle close outside the Bollinger Bands (from the *previous* bar's
       bands to avoid self-contamination of the rolling std).
    2. Range expansion: True Range > ATR × atr_expansion_mult.
* ATR-based SL/TP for volatility-adjusted position sizing.
* Conflict avoidance: active in LOW_VOLATILITY and HIGH_VOLATILITY only;
  the mean-reversion (VWAP, MR) strategies dominate RANGING markets.
"""
from __future__ import annotations

import json
import logging
import math
from enum import Enum
from typing import Optional

import pandas as pd

from delta_exchange_bot.strategy.base import CandleStrategy, Signal
from delta_exchange_bot.strategy.market_regime import MarketRegime, MarketRegimeSnapshot

logger = logging.getLogger(__name__)


def _log_structured(event: str, **fields) -> None:
    logger.debug(json.dumps({"event": event, **fields}, separators=(",", ":"), sort_keys=True))


# ---------------------------------------------------------------------------
# State machine enum
# ---------------------------------------------------------------------------

class SqueezeState(str, Enum):
    IDLE = "IDLE"
    SQUEEZE_DETECTED = "SQUEEZE_DETECTED"
    BREAKOUT_TRIGGERED = "BREAKOUT_TRIGGERED"


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class BollingerSqueezeStrategy(CandleStrategy):
    """
    Volatility compression → expansion breakout strategy.

    Detects periods where Bollinger Band width falls to a multi-bar low
    (squeeze), then trades the directional breakout when price closes
    outside the bands with expanding range.

    Regime assignment
    -----------------
    * LOW_VOLATILITY  — squeezes form here; strategy waits for breakout.
    * HIGH_VOLATILITY — breakout has just pushed price into high-vol regime;
                        strategy can still fire on the first bar of expansion.
    * RANGING / TRENDING — blocked; VWAP/MR strategies dominate there.

    State machine (derived from candle history each call)
    -----------------------------------------------------
    IDLE             → no qualifying squeeze in recent history.
    SQUEEZE_DETECTED → ≥ min_squeeze_bars consecutive squeeze bars; hold.
    BREAKOUT_TRIGGERED → qualifying squeeze ≤ max_breakout_lag bars ago AND
                         confirmed breakout on current bar → signal.

    Parameters
    ----------
    bb_period : int
        SMA period for BB middle band. Default: 20.
    bb_std : float
        Standard deviation multiplier. Default: 2.0.
    squeeze_percentile : float
        BB width is in a squeeze when it is below this rolling percentile.
        Default: 0.20 (bottom 20 % of the distribution).
    percentile_window : int
        Rolling window for percentile baseline. Default: 50.
    min_squeeze_bars : int
        Minimum consecutive bars required in squeeze before a breakout is
        considered valid. Default: 5.
    max_breakout_lag : int
        Maximum number of bars *after* the last squeeze bar within which a
        breakout must occur. Default: 5.
    atr_period : int
        Period for Wilder's ATR (SL/TP and range expansion gate). Default: 14.
    atr_expansion_mult : float
        True Range must exceed ATR × this value to confirm a breakout.
        Default: 1.2.
    sl_atr_mult : float
        Stop-loss = entry ± sl_atr_mult × ATR. Default: 1.5.
    tp_atr_mult : float
        Take-profit = entry ± tp_atr_mult × ATR. Default: 3.0
        (risk:reward ≈ 1:2).
    min_candles : int
        Minimum candles before generating any signal. Default: 60.
    """

    name = "bollinger_squeeze"
    # LOW_VOLATILITY: squeeze forms here.
    # HIGH_VOLATILITY: first breakout bar may already be in this regime.
    # Explicitly excluded: RANGING (VWAP/MR dominate) and TRENDING (TF/EMA dominate).
    allowed_regimes = {MarketRegime.LOW_VOLATILITY, MarketRegime.HIGH_VOLATILITY}

    def __init__(
        self,
        bb_period: int = 20,
        bb_std: float = 2.0,
        squeeze_percentile: float = 0.20,
        percentile_window: int = 50,
        min_squeeze_bars: int = 5,
        max_breakout_lag: int = 5,
        atr_period: int = 14,
        atr_expansion_mult: float = 1.2,
        sl_atr_mult: float = 1.5,
        tp_atr_mult: float = 3.0,
        min_candles: int = 60,
    ):
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.squeeze_percentile = squeeze_percentile
        self.percentile_window = percentile_window
        self.min_squeeze_bars = min_squeeze_bars
        self.max_breakout_lag = max_breakout_lag
        self.atr_period = atr_period
        self.atr_expansion_mult = atr_expansion_mult
        self.sl_atr_mult = sl_atr_mult
        self.tp_atr_mult = tp_atr_mult
        self.min_candles = min_candles

    # ------------------------------------------------------------------
    # Indicator computation — vectorized, no forward-looking
    # ------------------------------------------------------------------

    def _compute_bollinger_bands(
        self, close: pd.Series
    ) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
        """Return (middle, upper, lower, bb_width) as aligned Series.

        bb_width = (upper - lower) / middle — normalised so it is
        comparable across different price levels.
        """
        middle = close.rolling(window=self.bb_period, min_periods=self.bb_period).mean()
        std = close.rolling(window=self.bb_period, min_periods=self.bb_period).std(ddof=1)
        upper = middle + self.bb_std * std
        lower = middle - self.bb_std * std
        mid_safe = middle.replace(0.0, float("nan"))
        bb_width = (upper - lower) / mid_safe
        return middle, upper, lower, bb_width

    def _compute_atr(
        self, high: pd.Series, low: pd.Series, close: pd.Series
    ) -> pd.Series:
        """Wilder's ATR — consistent with MarketRegimeDetector and TrendFollowing."""
        prev_close = close.shift(1)
        tr = pd.concat(
            [
                (high - low),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        return tr.ewm(alpha=1.0 / self.atr_period, min_periods=self.atr_period, adjust=False).mean()

    def _compute_true_range(
        self, high: pd.Series, low: pd.Series, close: pd.Series
    ) -> pd.Series:
        prev_close = close.shift(1)
        return pd.concat(
            [
                (high - low),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)

    # ------------------------------------------------------------------
    # Public squeeze / breakout detection — testable independently
    # ------------------------------------------------------------------

    def detect_squeeze(self, bb_width: pd.Series) -> pd.Series:
        """Return a boolean Series: True where BB width is in the squeeze zone.

        Squeeze condition: BB width falls below the rolling
        ``squeeze_percentile`` quantile computed over ``percentile_window``
        bars.  Uses ``min_periods=bb_period`` so the first valid value
        appears as soon as a full BB is available.

        No forward-looking bias: every bar's threshold is computed using only
        data up to and including that bar.
        """
        threshold = bb_width.rolling(
            window=self.percentile_window, min_periods=self.bb_period
        ).quantile(self.squeeze_percentile)
        squeeze = (bb_width <= threshold) & bb_width.notna() & threshold.notna()
        return squeeze

    def detect_breakout(
        self,
        close: pd.Series,
        upper: pd.Series,
        lower: pd.Series,
        true_range: pd.Series,
        atr: pd.Series,
    ) -> tuple[str, bool]:
        """Check whether the *last* bar is a confirmed breakout.

        Confirmation requires both:
          1. Close outside the **previous bar's** Bollinger Bands — avoids
             the self-contamination problem where a large move widens the
             rolling std and the close never appears "outside".
          2. True Range > ATR × atr_expansion_mult — range expansion
             confirms genuine momentum rather than a noise spike.

        Returns
        -------
        direction : str
            ``'buy'``, ``'sell'``, or ``'hold'``.
        confirmed : bool
            True only when both conditions are satisfied.
        """
        if close.empty or upper.empty or lower.empty or true_range.empty or atr.empty:
            return "hold", False

        # Previous bar's bands to avoid self-contamination
        prev_upper = upper.shift(1)
        prev_lower = lower.shift(1)

        last_close = close.iloc[-1]
        last_prev_upper = prev_upper.iloc[-1]
        last_prev_lower = prev_lower.iloc[-1]
        last_tr = true_range.iloc[-1]
        last_atr = atr.iloc[-1]

        # Any NaN or non-finite value → no signal
        for v in (last_close, last_prev_upper, last_prev_lower, last_tr, last_atr):
            if not isinstance(v, (int, float)) or not math.isfinite(float(v)):
                return "hold", False

        range_expanded = float(last_tr) > float(last_atr) * self.atr_expansion_mult

        if float(last_close) > float(last_prev_upper) and range_expanded:
            return "buy", True
        if float(last_close) < float(last_prev_lower) and range_expanded:
            return "sell", True
        return "hold", False

    # ------------------------------------------------------------------
    # State machine helpers — private
    # ------------------------------------------------------------------

    def _count_consecutive_squeeze_at_tail(self, squeeze_mask: pd.Series) -> int:
        """Count consecutive True values at the *end* of squeeze_mask.

        Used to determine how long the current squeeze has lasted.
        """
        count = 0
        for val in reversed(squeeze_mask.tolist()):
            if val:
                count += 1
            else:
                break
        return count

    def _had_qualifying_squeeze(
        self, squeeze_mask: pd.Series
    ) -> tuple[bool, int]:
        """Check whether a qualifying squeeze occurred immediately before the current bar.

        A qualifying squeeze is a run of ≥ min_squeeze_bars consecutive
        squeeze bars whose *last* bar was ≤ max_breakout_lag bars ago
        (i.e., within the allowable breakout window before the current bar).

        The **current bar** (last element of squeeze_mask) is intentionally
        excluded — the breakout check is on the current bar, so we want to
        know about the squeeze that *preceded* it.

        Returns
        -------
        found : bool
        run_length : int  — length of the most-recent qualifying run.
        """
        # Exclude the current bar
        prev = squeeze_mask.iloc[:-1]
        if prev.empty:
            return False, 0

        vals = prev.tolist()
        n = len(vals)

        # Enumerate all squeeze runs in prev, right-to-left
        # Each run: (run_length, lag_to_current)
        # lag_to_current = bars from the run's *last* True bar to the current bar
        qualifying: list[tuple[int, int]] = []
        i = n - 1
        while i >= 0:
            if not vals[i]:
                i -= 1
                continue
            # Found the end of a run; walk backward to find its start
            run_end = i
            while i >= 0 and vals[i]:
                i -= 1
            run_start = i + 1
            run_len = run_end - run_start + 1
            # lag: (n-1) is the bar immediately before current; (n-1 - run_end) is
            # how many bars AFTER the run end there are in prev; +1 for the current bar
            lag_to_current = (n - 1 - run_end) + 1
            if run_len >= self.min_squeeze_bars and lag_to_current <= self.max_breakout_lag:
                qualifying.append((run_len, lag_to_current))

        if not qualifying:
            return False, 0

        # Pick the most-recent qualifying run (smallest lag)
        best = min(qualifying, key=lambda x: x[1])
        return True, best[0]

    def _derive_state(
        self,
        squeeze_mask: pd.Series,
        close: pd.Series,
        upper: pd.Series,
        lower: pd.Series,
        true_range: pd.Series,
        atr: pd.Series,
    ) -> tuple[SqueezeState, str]:
        """Derive current state from indicator data.

        Transition table
        ----------------
        current_in_squeeze=True,  consec ≥ min_squeeze_bars          → SQUEEZE_DETECTED
        current_in_squeeze=False, qualifying_squeeze + breakout       → BREAKOUT_TRIGGERED
        otherwise                                                      → IDLE
        """
        current_in_squeeze = bool(squeeze_mask.iloc[-1]) if not squeeze_mask.empty else False
        consec = self._count_consecutive_squeeze_at_tail(squeeze_mask)

        if current_in_squeeze:
            if consec >= self.min_squeeze_bars:
                return SqueezeState.SQUEEZE_DETECTED, "hold"
            return SqueezeState.IDLE, "hold"

        # Current bar is NOT in squeeze — check for breakout
        direction, breakout_confirmed = self.detect_breakout(
            close, upper, lower, true_range, atr
        )
        if breakout_confirmed:
            had_squeeze, _ = self._had_qualifying_squeeze(squeeze_mask)
            if had_squeeze:
                return SqueezeState.BREAKOUT_TRIGGERED, direction

        return SqueezeState.IDLE, "hold"

    # ------------------------------------------------------------------
    # CandleStrategy interface
    # ------------------------------------------------------------------

    def generate(
        self, symbol: str, candles: pd.DataFrame, regime: MarketRegimeSnapshot
    ) -> Signal:
        close = pd.to_numeric(
            candles.get("close", pd.Series(dtype=float)), errors="coerce"
        )
        current_price = float(close.iloc[-1]) if not close.dropna().empty else 0.0
        hold = Signal(symbol=symbol, action="hold", confidence=0.0, price=current_price)

        if not self.can_run(regime) or len(candles) < self.min_candles:
            return hold

        high = pd.to_numeric(candles.get("high", pd.Series(dtype=float)), errors="coerce")
        low = pd.to_numeric(candles.get("low", pd.Series(dtype=float)), errors="coerce")

        # --- compute indicators ---
        middle, upper, lower, bb_width = self._compute_bollinger_bands(close)
        atr = self._compute_atr(high, low, close)
        true_range = self._compute_true_range(high, low, close)

        # --- squeeze mask ---
        squeeze_mask = self.detect_squeeze(bb_width)

        # --- state machine ---
        state, direction = self._derive_state(
            squeeze_mask, close, upper, lower, true_range, atr
        )

        # --- extract scalar indicators for logging ---
        consec = self._count_consecutive_squeeze_at_tail(squeeze_mask)
        last_bb_width: Optional[float] = None
        last_upper_val: Optional[float] = None
        last_lower_val: Optional[float] = None
        last_atr_val: Optional[float] = None

        _bb_w = bb_width.dropna()
        if not _bb_w.empty:
            last_bb_width = round(float(_bb_w.iloc[-1]), 6)
        _upper = upper.dropna()
        if not _upper.empty:
            last_upper_val = round(float(_upper.iloc[-1]), 4)
        _lower = lower.dropna()
        if not _lower.empty:
            last_lower_val = round(float(_lower.iloc[-1]), 4)
        _atr = atr.dropna()
        if not _atr.empty:
            last_atr_val = float(_atr.iloc[-1])

        # --- handle states ---
        if state == SqueezeState.IDLE:
            _log_structured(
                "strategy_component",
                strategy="bollinger_squeeze",
                symbol=symbol,
                state="IDLE",
                action="hold",
                decision_reason="no_qualifying_squeeze",
                indicators={
                    "bb_width": last_bb_width,
                    "consecutive_squeeze_bars": consec,
                },
            )
            return hold

        if state == SqueezeState.SQUEEZE_DETECTED:
            _log_structured(
                "strategy_component",
                strategy="bollinger_squeeze",
                symbol=symbol,
                state="SQUEEZE_DETECTED",
                action="hold",
                decision_reason="squeeze_active_awaiting_breakout",
                indicators={
                    "bb_width": last_bb_width,
                    "consecutive_squeeze_bars": consec,
                    "upper_band": last_upper_val,
                    "lower_band": last_lower_val,
                },
            )
            return hold

        # --- BREAKOUT_TRIGGERED ---
        if last_atr_val is None or not math.isfinite(last_atr_val) or last_atr_val <= 0:
            return hold

        # Confidence: base 0.50 + up to 0.20 from band excess + up to 0.20 from ATR expansion
        prev_upper = upper.shift(1)
        prev_lower = lower.shift(1)
        ref_band = float(prev_upper.iloc[-1]) if direction == "buy" else float(prev_lower.iloc[-1])

        if math.isfinite(ref_band) and ref_band > 0:
            band_excess_pct = abs(current_price - ref_band) / ref_band * 100.0
            band_excess_contrib = min(0.20, band_excess_pct * 0.05)
        else:
            band_excess_contrib = 0.0

        last_tr_val = float(true_range.iloc[-1])
        if math.isfinite(last_tr_val) and last_atr_val > 0:
            atr_ratio = last_tr_val / last_atr_val
            atr_expansion_contrib = min(0.20, max(0.0, (atr_ratio - self.atr_expansion_mult) * 0.10))
        else:
            atr_expansion_contrib = 0.0

        confidence = min(0.90, 0.50 + band_excess_contrib + atr_expansion_contrib)

        # ATR-based SL/TP (risk:reward ≈ 1:2 with defaults sl=1.5, tp=3.0)
        if direction == "buy":
            sl = current_price - self.sl_atr_mult * last_atr_val
            tp = current_price + self.tp_atr_mult * last_atr_val
        else:
            sl = current_price + self.sl_atr_mult * last_atr_val
            tp = current_price - self.tp_atr_mult * last_atr_val

        _log_structured(
            "strategy_component",
            strategy="bollinger_squeeze",
            symbol=symbol,
            state="BREAKOUT_TRIGGERED",
            action=direction,
            decision_reason="squeeze_breakout_confirmed",
            indicators={
                "price": current_price,
                "bb_width": last_bb_width,
                "consecutive_squeeze_bars": consec,
                "atr": round(last_atr_val, 6),
                "atr_ratio": round(last_tr_val / last_atr_val, 3) if last_atr_val > 0 else None,
                "upper_band": last_upper_val,
                "lower_band": last_lower_val,
            },
            confidence=round(confidence, 4),
        )

        return Signal(
            symbol=symbol,
            action=direction,
            confidence=confidence,
            price=current_price,
            stop_loss=sl,
            take_profit=tp,
        )
