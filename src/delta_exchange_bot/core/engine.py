import time
import logging
import uuid
from collections import defaultdict, deque
from typing import Dict, Optional

from delta_exchange_bot.api.delta_client import DeltaClient
from delta_exchange_bot.execution.order_execution_engine import OrderExecutionEngine
from delta_exchange_bot.execution.order_manager import OrderManager
from delta_exchange_bot.risk.risk_manager import RiskManager
from delta_exchange_bot.persistence.db import StateDB
from delta_exchange_bot.strategy.ema_crossover import EMACrossoverStrategy
from delta_exchange_bot.strategy.momentum import MomentumStrategy
from delta_exchange_bot.strategy.rsi_scalping import RSIScalpingStrategy
from delta_exchange_bot.strategy.portfolio import PortfolioStrategy
from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.core.settings import Settings

logger = logging.getLogger(__name__)


class TradingEngine:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.api = DeltaClient(settings.api_key, settings.api_secret, settings.api_url)
        self.execution_engine = OrderExecutionEngine(self.api)
        self.order_manager = OrderManager()
        self.risk_manager = RiskManager(max_positions=settings.max_positions)
        self.db = StateDB(settings.state_db_path)
        self.strategy = self._build_strategy(settings.strategy_name)
        self.current_equity = 100000.0
        self.positions: dict[str, dict] = {} # symbol -> {side, size, entry_time, entry_price}
        self._price_history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=50))

    @staticmethod
    def _build_strategy(strategy_name: str):
        normalized = strategy_name.strip().lower()
        if normalized == "momentum":
            return MomentumStrategy()
        if normalized == "rsi_scalping":
            return RSIScalpingStrategy()
        if normalized == "ema_crossover":
            return EMACrossoverStrategy()
        if normalized == "portfolio":
            return PortfolioStrategy()
        raise ValueError(f"Unsupported strategy_name={strategy_name}")

    @staticmethod
    def _safe_float(value) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _extract_price(self, ticker: dict) -> float:
        if not isinstance(ticker, dict):
            return 0.0

        nested = ticker.get("result")
        if isinstance(nested, dict):
            ticker_data = nested
        else:
            ticker_data = ticker

        for candidate in (
            ticker_data.get("mark_price"),
            ticker_data.get("close"),
            ticker_data.get("last_price"),
            ticker_data.get("price"),
            ticker_data.get("spot_price"),
            ticker.get("mark_price"),
            ticker.get("close"),
            ticker.get("last_price"),
            ticker.get("price"),
        ):
            parsed = self._safe_float(candidate)
            if parsed > 0:
                return parsed
        return 0.0

    def _fetch_market_snapshot(self) -> Dict[str, dict]:
        market_data = {}
        for symbol in self.settings.trade_symbols:
            try:
                ticker = self.api.get_ticker(symbol)
                price = self._extract_price(ticker)
            except Exception as exc:
                logger.warning("Could not fetch ticker for %s: %s", symbol, exc)
                price = 0.0
                ticker = {}

            if price > 0:
                self._price_history[symbol].append(price)

            history = list(self._price_history[symbol])
            market_data[symbol] = {"prices": history, "ticker": ticker}
        return market_data

    def _update_local_position(self, symbol: str, side: str, size: float, price: float = 0.0) -> float:
        delta = size if side == "buy" else -size
        current = self.positions.get(symbol, {"size": 0.0})
        new_size = current["size"] + delta
        
        if abs(new_size) < 1e-12:
            current_pos = self.positions.pop(symbol, None)
            if current_pos and "trade_id" in current_pos:
                self.db.close_trade_record(trade_id=current_pos["trade_id"], exit_price=price)
            return 0.0
        else:
            if current["size"] == 0:
                # New position
                trade_id = f"{symbol}-{uuid.uuid4().hex[:8]}"
                side_str = "long" if new_size > 0 else "short"
                self.positions[symbol] = {
                    "trade_id": trade_id,
                    "side": side_str,
                    "size": new_size,
                    "entry_time": time.time(),
                    "entry_price": price
                }
                self.db.upsert_trade_record(
                    trade_id=trade_id,
                    symbol=symbol,
                    side=side_str,
                    size=abs(new_size),
                    entry_price=price,
                    strategy_name=getattr(self.strategy, "name", self.settings.strategy_name)
                )
            else:
                self.positions[symbol]["size"] = new_size
                # Optionally update size in trade_record if partial fill/add (simplified here)
            return new_size

    @staticmethod
    def _is_opening_trade(side: str, updated_position: float) -> bool:
        if updated_position == 0:
            return False
        if side == "buy":
            return updated_position > 0
        return updated_position < 0

    def _register_trade_protection(self, signal: Signal, side: str, size: float):
        position_side = "long" if side == "buy" else "short"
        if signal.stop_loss is not None:
            self.execution_engine.place_stop_loss(signal.symbol, position_side, size=size, stop_price=signal.stop_loss)
        if signal.take_profit is not None:
            self.execution_engine.place_take_profit(signal.symbol, position_side, size=size, target_price=signal.take_profit)
        if signal.trailing_stop_pct is not None:
            self.execution_engine.set_trailing_stop(
                signal.symbol,
                position_side,
                size=size,
                trail_pct=signal.trailing_stop_pct,
                entry_price=signal.price,
            )

    def _process_protection_triggers(self, market_data: Dict[str, dict]):
        for symbol, series in market_data.items():
            prices = series.get("prices", [])
            if not prices:
                continue
            current_price = prices[-1]
            if current_price <= 0:
                continue

            try:
                triggered = self.execution_engine.on_price_update(symbol, current_price)
            except Exception as exc:
                logger.exception("Protection check failed for %s: %s", symbol, exc)
                continue

            if not triggered:
                continue

            exit_side = triggered["exit_side"]
            exit_size = float(triggered["size"])
            exit_price = float(triggered.get("trigger_price", current_price))
            updated_position = self._update_local_position(symbol, exit_side, exit_size, exit_price)
            self.db.save_trade(symbol, exit_side, exit_size, exit_price)
            if updated_position == 0:
                self.execution_engine.clear_protection(symbol)
            logger.info("Protection exit executed (%s mode): %s", self.settings.mode, triggered)

    def _execute_signal(self, signal: Signal):
        if signal.action == "hold":
            return

        if not self.risk_manager.assess(len(self.positions), self.current_equity):
            logger.warning("Risk check failed; skipping signal %s", signal)
            return

        side = signal.action.lower()
        size = min(self.settings.order_size, self.current_equity * 0.01)
        previous_position = self.positions.get(signal.symbol, 0.0)

        try:
            if self.settings.mode == "live":
                if signal.price > 0:
                    order = self.execution_engine.execute_limit_order(
                        symbol=signal.symbol,
                        side=side,
                        size=size,
                        price=signal.price,
                    )
                else:
                    order = self.execution_engine.execute_market_order(
                        symbol=signal.symbol,
                        side=side,
                        size=size,
                    )
            else:
                order = self.order_manager.place_order(signal.symbol, side, size, signal.price)
        except Exception as exc:
            logger.exception("Order placement failed for %s %s: %s", signal.symbol, side, exc)
            return

        updated_position = self._update_local_position(signal.symbol, side, size, signal.price)
        if self.settings.mode == "live":
            if updated_position == 0:
                self.execution_engine.clear_protection(signal.symbol)
            elif self._is_opening_trade(side, updated_position):
                self._register_trade_protection(signal, side=side, size=abs(updated_position))

        self.db.save_trade(signal.symbol, side, size, signal.price)
        logger.info("Placed order %s", order)

    def _check_time_based_close(self):
        """Close positions that have been open longer than max_holding_time_s."""
        now = time.time()
        max_time = self.settings.max_holding_time_s
        for symbol, pos in list(self.positions.items()):
            elapsed = now - pos["entry_time"]
            if elapsed > max_time:
                logger.warning("Closing %s due to max holding time: %s > %s", symbol, elapsed, max_time)
                exit_side = "sell" if pos["size"] > 0 else "buy"
                self.execution_engine.execute_market_order(
                    symbol=symbol,
                    side=exit_side,
                    size=abs(pos["size"]),
                    reduce_only=True
                )
                self.positions.pop(symbol)
                self.db.save_trade(symbol, exit_side, abs(pos["size"]), 0.0) # Price unknown here, usually updated by trigger

    def run(self, max_iterations: Optional[int] = None):
        logger.info("Starting trading engine in %s mode", self.settings.mode)
        iteration = 0
        while max_iterations is None or iteration < max_iterations:
            market_data = self._fetch_market_snapshot()
            
            # Funding awareness
            if self.settings.enable_funding_awareness:
                for symbol, data in market_data.items():
                    ticker = data.get("ticker", {})
                    funding_rate = ticker.get("result", {}).get("funding_rate") or ticker.get("funding_rate")
                    if funding_rate:
                        fr = float(funding_rate)
                        if abs(fr) > self.settings.funding_alert_threshold:
                            logger.warning("High funding rate for %s: %.4f%%", symbol, fr * 100)

            self._process_protection_triggers(market_data)
            self._check_time_based_close()
            signals = self.strategy.generate(market_data)

            for signal in signals:
                self._execute_signal(signal)

            iteration += 1
            time.sleep(self.settings.trade_frequency_s)

        logger.info("Trading engine stopped after %s iterations", iteration)
