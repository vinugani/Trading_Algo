import logging
from typing import Any, List, Optional, Tuple

import pandas as pd

from delta_exchange_bot.strategy.base import Signal, Strategy, CandleStrategy
from delta_exchange_bot.strategy.ema_crossover import EMACrossoverStrategy
from delta_exchange_bot.strategy.momentum import MomentumStrategy
from delta_exchange_bot.strategy.rsi_scalping import RSIScalpingStrategy, RSIScalpingCandleStrategy
from delta_exchange_bot.strategy.market_regime import MarketRegimeSnapshot
from delta_exchange_bot.strategy.mean_reversion import MeanReversionStrategy
from delta_exchange_bot.strategy.trend_following import TrendFollowingStrategy

logger = logging.getLogger(__name__)


class PortfolioStrategy(Strategy):
    """
    Production-grade portfolio strategy that ensembles multiple sub-strategies.
    It aggregates signals from sub-strategies and executes trades based on a
    weighted confidence mechanism.
    """

    def __init__(self, sub_strategies: Optional[List[Tuple[Strategy, float]]] = None):
        if sub_strategies is not None:
            self.sub_strategies = sub_strategies
        else:
            self.sub_strategies = [
                (MomentumStrategy(), 0.3),
                (RSIScalpingStrategy(), 0.4),
                (EMACrossoverStrategy(), 0.3),
            ]

    def generate(self, market_data: dict[str, dict]) -> List[Signal]:
        symbol_scores: dict[str, dict] = {}
        for symbol in market_data.keys():
            symbol_scores[symbol] = {
                "buy": 0.0,
                "sell": 0.0,
                "hold": 0.0,
                "prices": [],
                "stop_losses": [],
                "take_profits": [],
                "trailing_stops": [],
            }

        for strategy, weight in self.sub_strategies:
            try:
                signals = strategy.generate(market_data)
                for sig in signals:
                    scores = symbol_scores[sig.symbol]
                    scores[sig.action] += sig.confidence * weight
                    scores["prices"].append(sig.price)
                    if sig.stop_loss is not None:
                        scores["stop_losses"].append(sig.stop_loss)
                    if sig.take_profit is not None:
                        scores["take_profits"].append(sig.take_profit)
                    if sig.trailing_stop_pct is not None:
                        scores["trailing_stops"].append(sig.trailing_stop_pct)
            except Exception as exc:
                logger.warning(
                    f"Sub-strategy {strategy.__class__.__name__} failed in PortfolioStrategy: {exc}"
                )

        final_signals: List[Signal] = []

        for symbol, scores in symbol_scores.items():
            if not scores["prices"]:
                continue

            avg_price = sum(scores["prices"]) / len(scores["prices"])

            buy_score = scores["buy"]
            sell_score = scores["sell"]
            hold_score = scores["hold"]

            best_action = "hold"
            best_score = hold_score

            # Threshold to consider a valid entry
            CONFIDENCE_THRESHOLD = 0.2

            if buy_score > sell_score and buy_score > hold_score:
                best_action = "buy"
                best_score = buy_score
            elif sell_score > buy_score and sell_score > hold_score:
                best_action = "sell"
                best_score = sell_score

            if best_score < CONFIDENCE_THRESHOLD:
                best_action = "hold"

            # Compute blended take profit and stop loss
            stop_loss = None
            if scores["stop_losses"]:
                stop_loss = sum(scores["stop_losses"]) / len(scores["stop_losses"])

            take_profit = None
            if scores["take_profits"]:
                take_profit = sum(scores["take_profits"]) / len(scores["take_profits"])

            trailing_stop_pct = None
            if scores["trailing_stops"]:
                trailing_stop_pct = sum(scores["trailing_stops"]) / len(
                    scores["trailing_stops"]
                )

            final_signals.append(
                Signal(
                    symbol=symbol,
                    action=best_action,
                    confidence=best_score,
                    price=avg_price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    trailing_stop_pct=trailing_stop_pct,
                )
            )

        return final_signals


class CandlePortfolioStrategy(CandleStrategy):
    """
    Production-grade portfolio strategy for Pandas DataFrame candles.
    Aggregates signals from underlying candle strategies based on weights.
    """

    def __init__(self, sub_strategies: Optional[List[Tuple[CandleStrategy, float]]] = None):
        super().__init__(name="Portfolio")
        if sub_strategies is not None:
            self.sub_strategies = sub_strategies
        else:
            self.sub_strategies = [
                (TrendFollowingStrategy(), 0.3),
                (MeanReversionStrategy(), 0.3),
                (RSIScalpingCandleStrategy(), 0.4),
            ]

    def generate(
        self, symbol: str, candles: pd.DataFrame, regime: MarketRegimeSnapshot
    ) -> Signal:

        if candles.empty:
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=0.0)

        buy_score = 0.0
        sell_score = 0.0
        hold_score = 0.0

        current_price = float(candles["close"].iloc[-1])
        valid_signals = []

        for strategy, weight in self.sub_strategies:
            try:
                sig = strategy.generate(symbol=symbol, candles=candles, regime=regime)
                if sig.action == "buy":
                    buy_score += sig.confidence * weight
                elif sig.action == "sell":
                    sell_score += sig.confidence * weight
                else:
                    hold_score += sig.confidence * weight
                valid_signals.append(sig)
            except Exception as exc:
                logger.warning(
                    f"Sub-strategy {strategy.name} failed in CandlePortfolioStrategy: {exc}"
                )

        if not valid_signals:
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=current_price)

        best_action = "hold"
        best_score = hold_score
        CONFIDENCE_THRESHOLD = 0.2

        if buy_score > sell_score and buy_score > hold_score:
            best_action = "buy"
            best_score = buy_score
        elif sell_score > buy_score and sell_score > hold_score:
            best_action = "sell"
            best_score = sell_score

        if best_score < CONFIDENCE_THRESHOLD:
            best_action = "hold"

        # Calculate blended stop loss / take profit / trailing stops across valid signals
        sls = [s.stop_loss for s in valid_signals if s.stop_loss is not None]
        tps = [s.take_profit for s in valid_signals if s.take_profit is not None]
        trls = [
            s.trailing_stop_pct for s in valid_signals if s.trailing_stop_pct is not None
        ]

        stop_loss = sum(sls) / len(sls) if sls else None
        take_profit = sum(tps) / len(tps) if tps else None
        trailing_stop_pct = sum(trls) / len(trls) if trls else None

        return Signal(
            symbol=symbol,
            action=best_action,
            confidence=best_score,
            price=current_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_stop_pct=trailing_stop_pct,
        )
