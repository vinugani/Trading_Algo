from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import pandas as pd


class MarketRegime(str, Enum):
    TRENDING = "trending"
    RANGING = "ranging"
    HIGH_VOLATILITY = "high_volatility"
    LOW_VOLATILITY = "low_volatility"


@dataclass
class MarketRegimeSnapshot:
    regime: MarketRegime
    adx: float
    atr: float
    atr_pct: float
    ema_slope_pct: float


class MarketRegimeDetector:
    def __init__(
        self,
        adx_period: int = 14,
        atr_period: int = 14,
        ema_period: int = 20,
        trending_adx_threshold: float = 30.0,
        high_volatility_atr_pct: float = 0.01,
        low_volatility_atr_pct: float = 0.0025,
        ema_slope_threshold_pct: float = 0.0008,
    ):
        self.adx_period = adx_period
        self.atr_period = atr_period
        self.ema_period = ema_period
        self.trending_adx_threshold = trending_adx_threshold
        self.high_volatility_atr_pct = high_volatility_atr_pct
        self.low_volatility_atr_pct = low_volatility_atr_pct
        self.ema_slope_threshold_pct = ema_slope_threshold_pct

    @staticmethod
    def _to_series(candles: pd.DataFrame, col: str) -> pd.Series:
        return pd.to_numeric(candles.get(col, pd.Series(dtype=float)), errors="coerce")

    def _atr(self, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
        prev_close = close.shift(1)
        tr = pd.concat(
            [
                (high - low).abs(),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        return tr.rolling(window=self.atr_period, min_periods=self.atr_period).mean()

    def _adx(self, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
        up_move = high.diff()
        down_move = -low.diff()
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        atr = self._atr(high, low, close)
        plus_di = 100.0 * (plus_dm.rolling(self.adx_period).sum() / atr.replace(0, pd.NA))
        minus_di = 100.0 * (minus_dm.rolling(self.adx_period).sum() / atr.replace(0, pd.NA))
        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA)) * 100.0
        return dx.rolling(self.adx_period, min_periods=self.adx_period).mean()

    def detect(self, candles: pd.DataFrame) -> MarketRegimeSnapshot:
        if candles.empty:
            return MarketRegimeSnapshot(
                regime=MarketRegime.RANGING,
                adx=0.0,
                atr=0.0,
                atr_pct=0.0,
                ema_slope_pct=0.0,
            )

        high = self._to_series(candles, "high")
        low = self._to_series(candles, "low")
        close = self._to_series(candles, "close")
        df = pd.DataFrame({"high": high, "low": low, "close": close}).dropna()
        if len(df) < max(self.adx_period * 2, self.atr_period + 1, self.ema_period + 2):
            return MarketRegimeSnapshot(
                regime=MarketRegime.RANGING,
                adx=0.0,
                atr=0.0,
                atr_pct=0.0,
                ema_slope_pct=0.0,
            )

        atr_series = self._atr(df["high"], df["low"], df["close"])
        adx_series = self._adx(df["high"], df["low"], df["close"])
        ema = df["close"].ewm(span=self.ema_period, adjust=False).mean()

        atr = float(atr_series.iloc[-1]) if not pd.isna(atr_series.iloc[-1]) else 0.0
        adx = float(adx_series.iloc[-1]) if not pd.isna(adx_series.iloc[-1]) else 0.0
        price = float(df["close"].iloc[-1]) if len(df) else 0.0
        atr_pct = (atr / price) if price > 0 else 0.0

        if len(ema) >= 3:
            prev = float(ema.iloc[-3])
            curr = float(ema.iloc[-1])
            ema_slope_pct = ((curr - prev) / prev) if prev != 0 else 0.0
        else:
            ema_slope_pct = 0.0

        if atr_pct >= self.high_volatility_atr_pct:
            regime = MarketRegime.HIGH_VOLATILITY
        elif atr_pct <= self.low_volatility_atr_pct:
            regime = MarketRegime.LOW_VOLATILITY
        elif adx >= self.trending_adx_threshold and abs(ema_slope_pct) >= self.ema_slope_threshold_pct:
            regime = MarketRegime.TRENDING
        else:
            regime = MarketRegime.RANGING

        return MarketRegimeSnapshot(
            regime=regime,
            adx=adx,
            atr=atr,
            atr_pct=atr_pct,
            ema_slope_pct=ema_slope_pct,
        )
