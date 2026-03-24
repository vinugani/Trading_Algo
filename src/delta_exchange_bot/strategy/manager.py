from __future__ import annotations

import json
import logging

import pandas as pd

from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.strategy.market_regime import MarketRegime
from delta_exchange_bot.strategy.market_regime import MarketRegimeDetector

from .base import CandleStrategy
from .mean_reversion import MeanReversionStrategy
from .rsi_scalping import RSIScalpingCandleStrategy
from .trend_following import TrendFollowingStrategy

logger = logging.getLogger(__name__)


def _log_structured(event: str, **fields) -> None:
    logger.debug(json.dumps({"event": event, **fields}, separators=(",", ":"), sort_keys=True))


class StrategyManager:
    """Selects strategy by detected market regime."""

    def __init__(
        self,
        regime_detector: MarketRegimeDetector | None = None,
        rsi_scalping: CandleStrategy | None = None,
        trend_following: CandleStrategy | None = None,
        mean_reversion: CandleStrategy | None = None,
    ):
        self.regime_detector = regime_detector or MarketRegimeDetector()
        self.rsi_scalping = rsi_scalping or RSIScalpingCandleStrategy()
        self.trend_following = trend_following or TrendFollowingStrategy()
        self.mean_reversion = mean_reversion or MeanReversionStrategy()

    def _pick_candidates(self, regime: MarketRegime) -> list[CandleStrategy]:
        if regime == MarketRegime.TRENDING:
            return [self.trend_following, self.rsi_scalping]
        if regime == MarketRegime.RANGING:
            return [self.mean_reversion, self.rsi_scalping]
        if regime == MarketRegime.HIGH_VOLATILITY:
            return [self.rsi_scalping, self.trend_following]
        if regime == MarketRegime.LOW_VOLATILITY:
            return [self.mean_reversion, self.rsi_scalping]
        return [self.mean_reversion, self.rsi_scalping]

    def generate_signal(self, symbol: str, candles: pd.DataFrame) -> tuple[Signal, str, str]:
        snapshot = self.regime_detector.detect(candles)
        candidates = self._pick_candidates(snapshot.regime)
        best_hold = Signal(symbol=symbol, action="hold", confidence=0.0, price=0.0)
        best_hold_strategy = candidates[0].name if candidates else "unknown"
        candidate_results: list[dict[str, object]] = []

        logger.debug(
            "[%s] Strategy manager regime: regime=%s adx=%.2f atr=%.6f atr_pct=%.6f ema_slope_pct=%.6f candidates=%s",
            symbol,
            snapshot.regime.value,
            float(snapshot.adx),
            float(snapshot.atr),
            float(snapshot.atr_pct),
            float(snapshot.ema_slope_pct),
            [strategy.name for strategy in candidates],
        )

        for strategy in candidates:
            signal = strategy.generate(symbol=symbol, candles=candles, regime=snapshot)
            logger.debug(
                "[%s] Strategy candidate: strategy=%s action=%s confidence=%.4f",
                symbol,
                strategy.name,
                signal.action,
                float(signal.confidence),
            )
            candidate_results.append(
                {
                    "strategy": strategy.name,
                    "action": signal.action,
                    "confidence": float(signal.confidence),
                }
            )
            if signal.action != "hold":
                _log_structured(
                    "strategy_manager_decision",
                    symbol=symbol,
                    regime=snapshot.regime.value,
                    candidates=candidate_results,
                    selected_strategy=strategy.name,
                    selected_action=signal.action,
                    final_confidence=float(signal.confidence),
                )
                return signal, snapshot.regime.value, strategy.name
            if float(signal.confidence) >= float(best_hold.confidence):
                best_hold = signal
                best_hold_strategy = strategy.name

        logger.debug(
            "[%s] Strategy manager hold: regime=%s best_hold_strategy=%s best_hold_confidence=%.4f",
            symbol,
            snapshot.regime.value,
            best_hold_strategy,
            float(best_hold.confidence),
        )
        _log_structured(
            "strategy_manager_decision",
            symbol=symbol,
            regime=snapshot.regime.value,
            candidates=candidate_results,
            selected_strategy=best_hold_strategy,
            selected_action=best_hold.action,
            final_confidence=float(best_hold.confidence),
        )
        return best_hold, snapshot.regime.value, best_hold_strategy
