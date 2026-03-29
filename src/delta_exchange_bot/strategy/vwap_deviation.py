from __future__ import annotations

import math
from typing import Optional

import pandas as pd
import structlog

from delta_exchange_bot.strategy.base import CandleStrategy, Signal
from delta_exchange_bot.strategy.market_regime import MarketRegime, MarketRegimeSnapshot

logger = structlog.get_logger(__name__)


class VWAPDeviationStrategy(CandleStrategy):
    """
    VWAP Deviation mean-reversion strategy.

    Buys when price is statistically below VWAP (expects reversion upward).
    Sells when price is statistically above VWAP (expects reversion downward).

    Active only in RANGING and LOW_VOLATILITY regimes.

    VWAP is computed on *closed* bars only (candles[:-1]) to avoid
    price-VWAP circularity from the current live bar.

    Deviation significance is evaluated via z-score (rolling std of deviation
    series) when sufficient history exists; falls back to plain % threshold
    when candle history is too short.

    Parameters
    ----------
    deviation_pct : float
        Fallback % threshold used when z-score cannot be computed. Default: 0.5.
    zscore_threshold : float
        Minimum |z-score| required to trigger a signal. Default: 1.5.
    zscore_window : int
        Rolling window for computing deviation standard deviation. Default: 20.
    sl_atr_multiplier : float
        SL distance = max(sl_pct_floor * price, sl_atr_multiplier * ATR). Default: 1.5.
    sl_pct_floor : float
        Minimum SL as a % of price (used when ATR is unavailable). Default: 0.4.
    min_candles : int
        Minimum number of candles required before any signal. Default: 20.
    cooldown_bars : int
        Minimum bars between consecutive signals for the same symbol. Default: 5.
    """

    name = "vwap_deviation"
    allowed_regimes = {MarketRegime.RANGING, MarketRegime.LOW_VOLATILITY}

    def __init__(
        self,
        deviation_pct: float = 0.5,
        zscore_threshold: float = 1.5,
        zscore_window: int = 20,
        sl_atr_multiplier: float = 1.5,
        sl_pct_floor: float = 0.4,
        min_candles: int = 20,
        cooldown_bars: int = 5,
    ):
        self.deviation_pct = deviation_pct
        self.zscore_threshold = zscore_threshold
        self.zscore_window = zscore_window
        self.sl_atr_multiplier = sl_atr_multiplier
        self.sl_pct_floor = sl_pct_floor
        self.min_candles = min_candles
        self.cooldown_bars = cooldown_bars
        # Per-symbol bar index of the last emitted signal (cooldown tracking).
        self._last_signal_bar: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _compute_vwap(self, candles: pd.DataFrame) -> Optional[float]:
        """Compute session VWAP from closed OHLCV candles.

        Returns the final cumulative VWAP value, or None if it cannot be
        computed (e.g. all-NaN data, zero candles).
        """
        high = pd.to_numeric(candles.get("high", pd.Series(dtype=float)), errors="coerce")
        low = pd.to_numeric(candles.get("low", pd.Series(dtype=float)), errors="coerce")
        close = pd.to_numeric(candles.get("close", pd.Series(dtype=float)), errors="coerce")
        volume = pd.to_numeric(
            candles.get("volume", pd.Series(dtype=float)), errors="coerce"
        ).fillna(0.0)

        # Fall back to equal weights when volume is absent or all-zero.
        if volume.sum() == 0:
            volume = pd.Series([1.0] * len(close), index=close.index)

        typical_price = (high + low + close) / 3.0
        cum_tp_vol = (typical_price * volume).cumsum()
        cum_vol = volume.cumsum().replace(0.0, float("nan"))
        vwap_series = cum_tp_vol / cum_vol
        valid = vwap_series.dropna()
        if valid.empty:
            return None
        last = float(valid.iloc[-1])
        if not math.isfinite(last) or last <= 0:
            return None
        return last

    def _deviation_std(
        self, closed_close: pd.Series, vwap: float
    ) -> Optional[float]:
        """Compute the rolling std of % deviations over the closed-bar history.

        Returns the std (not a z-score) so the caller can normalise the
        *current* bar's deviation against it.  Returns None when there is
        insufficient history for a reliable estimate.
        """
        if len(closed_close) < self.zscore_window:
            return None
        pct_deviations = (closed_close - vwap) / vwap * 100.0
        std = float(pct_deviations.rolling(self.zscore_window).std().iloc[-1])
        if not math.isfinite(std) or std <= 0:
            return None
        return std

    # ------------------------------------------------------------------
    # CandleStrategy interface
    # ------------------------------------------------------------------

    def generate(
        self, symbol: str, candles: pd.DataFrame, regime: MarketRegimeSnapshot
    ) -> Signal:
        # Parse current (live) close once — reused for price and z-score.
        close_series = pd.to_numeric(
            candles.get("close", pd.Series(dtype=float)), errors="coerce"
        ).dropna()
        current_price = float(close_series.iloc[-1]) if not close_series.empty else 0.0
        hold = Signal(symbol=symbol, action="hold", confidence=0.0, price=current_price)

        # ── Regime gate ────────────────────────────────────────────────
        if not self.can_run(regime):
            logger.debug(
                "vwap_deviation.regime_blocked",
                symbol=symbol,
                regime=regime.regime.value,
            )
            return hold

        # ── Minimum data guard ─────────────────────────────────────────
        if len(candles) < self.min_candles:
            return hold

        # ── Cooldown gate ──────────────────────────────────────────────
        bar_idx = len(candles)
        last_signal = self._last_signal_bar.get(symbol, -(self.cooldown_bars + 1))
        if bar_idx - last_signal < self.cooldown_bars:
            return hold

        # ── Compute VWAP on closed bars only (H1 fix) ──────────────────
        closed_candles = candles.iloc[:-1]
        vwap = self._compute_vwap(closed_candles)
        if vwap is None:
            return hold

        # ── ATR-floor gate: skip if deviation threshold < noise floor (H5) ─
        if regime.atr_pct > 0 and (self.deviation_pct / 100.0) < regime.atr_pct:
            logger.debug(
                "vwap_deviation.threshold_below_atr_floor",
                symbol=symbol,
                deviation_pct=self.deviation_pct,
                atr_pct=round(regime.atr_pct * 100, 4),
            )
            return hold

        # ── Deviation and significance (H2 fix) ────────────────────────
        deviation_pct = (current_price - vwap) / vwap * 100.0  # +: above VWAP

        closed_close = pd.to_numeric(
            closed_candles.get("close", pd.Series(dtype=float)), errors="coerce"
        ).dropna()
        dev_std = self._deviation_std(closed_close, vwap)

        # Normalise the *current* bar's deviation against the closed-bar std.
        if dev_std is not None:
            z_score: Optional[float] = deviation_pct / dev_std
            threshold_met = abs(z_score) >= self.zscore_threshold
        else:
            # Fallback: plain % threshold when history too short for z-score.
            z_score = None
            threshold_met = abs(deviation_pct) >= self.deviation_pct

        if not threshold_met:
            logger.debug(
                "vwap_deviation.below_threshold",
                symbol=symbol,
                deviation_pct=round(deviation_pct, 4),
                z_score=round(z_score, 4) if z_score is not None else None,
                threshold=self.zscore_threshold if z_score is not None else self.deviation_pct,
            )
            return hold

        # ── Confidence scaling (H4 fix: base raised to 0.50) ───────────
        # z-score path: excess above zscore_threshold → faster scaling
        # fallback path: excess above deviation_pct → same formula
        if z_score is not None:
            excess = abs(z_score) - self.zscore_threshold
            norm = self.zscore_threshold
        else:
            excess = abs(deviation_pct) - self.deviation_pct
            norm = self.deviation_pct
        confidence = min(0.90, 0.50 + (excess / norm) * 0.20)

        # ── ATR-relative stop-loss (C3 + H5 fix) ──────────────────────
        atr = regime.atr if regime.atr > 0 else (current_price * self.sl_pct_floor / 100.0)
        sl_distance = max(
            current_price * self.sl_pct_floor / 100.0,
            self.sl_atr_multiplier * atr,
        )

        # ── TP anchored to VWAP reversion distance (H3 fix) ───────────
        # Target 80% of the distance back to VWAP.
        tp_distance = 0.80 * abs(vwap - current_price)
        # Ensure TP is at least sl_pct_floor to maintain a positive R/R.
        tp_distance = max(tp_distance, current_price * self.sl_pct_floor / 100.0)

        if deviation_pct < 0:
            # Price below VWAP — mean-reversion BUY
            action = "buy"
            sl = current_price - sl_distance
            tp = current_price + tp_distance
        else:
            # Price above VWAP — mean-reversion SELL
            action = "sell"
            sl = current_price + sl_distance
            tp = current_price - tp_distance

        # Mark cooldown (C4 fix).
        self._last_signal_bar[symbol] = bar_idx

        logger.info(
            "vwap_deviation.signal",
            symbol=symbol,
            action=action,
            price=current_price,
            vwap=round(vwap, 6),
            deviation_pct=round(deviation_pct, 4),
            z_score=round(z_score, 4) if z_score is not None else None,
            sl=round(sl, 6),
            tp=round(tp, 6),
            confidence=round(confidence, 4),
            regime=regime.regime.value,
            atr=round(atr, 6),
        )

        return Signal(
            symbol=symbol,
            action=action,
            confidence=confidence,
            price=current_price,
            stop_loss=sl,
            take_profit=tp,
        )
