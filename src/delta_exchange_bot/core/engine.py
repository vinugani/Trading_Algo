import logging
import asyncio
import contextlib
import time
import uuid
import pandas as pd
from collections import defaultdict, deque
from typing import Dict, Optional

from delta_exchange_bot.api.delta_client import DeltaClient
from delta_exchange_bot.execution.order_execution_engine import OrderExecutionEngine
from delta_exchange_bot.execution.order_manager import OrderManager
from delta_exchange_bot.risk.risk_manager import RiskManager
from delta_exchange_bot.persistence.db import DatabaseManager
from delta_exchange_bot.strategy.ema_crossover import EMACrossoverStrategy
from delta_exchange_bot.strategy.momentum import MomentumStrategy
from delta_exchange_bot.strategy.rsi_scalping import RSIScalpingStrategy
from delta_exchange_bot.strategy.portfolio import PortfolioStrategy
from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.api.websocket_manager import WebSocketManager
from delta_exchange_bot.services.reconciliation_service import ReconciliationService
from delta_exchange_bot.strategy.enhanced_rsi import EnhancedRSIScalping

logger = logging.getLogger(__name__)


class TradingEngine:
    def __init__(self, settings: Settings, db: Optional[DatabaseManager] = None):
        self.settings = settings
        self.api = DeltaClient(settings.api_key, settings.api_secret, settings.api_url)
        self.execution_engine = OrderExecutionEngine(self.api)
        self.order_manager = OrderManager()
        self.risk_manager = RiskManager(
            max_positions=settings.max_positions,
            max_risk_per_trade=settings.max_risk_per_trade,
            max_daily_loss_pct=settings.max_daily_loss,
        )
        self.db = db or DatabaseManager(settings.postgres_dsn)
        self.strategy = self._build_strategy(settings.strategy_name)
        self.current_equity = 100000.0
        self.positions: dict[str, dict] = {} # symbol -> {side, size, entry_time, entry_price}
        self._price_history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=50))
        self._market_data_degraded = False
        
        # WebSocket Setup
        self.ws_manager = WebSocketManager(
            ws_url=settings.ws_url,
            api_key=settings.api_key,
            api_secret=settings.api_secret,
            on_message=self._on_ws_message,
            on_connect=self._on_ws_connect,
            on_disconnect=self._on_ws_disconnect,
            on_alert=self._on_ws_alert,
            ping_interval_s=settings.websocket_ping_interval_s,
            ping_timeout_s=settings.websocket_ping_timeout_s,
            stale_after_s=settings.websocket_stale_after_s,
        )
        for symbol in settings.trade_symbols:
            self.ws_manager.add_subscription("v2/ticker", [symbol])
        
        # New OHLCV buffer: symbol -> list of dicts {o, h, l, c, v, t}
        self._ohlcv_history: dict[str, deque[dict]] = defaultdict(lambda: deque(maxlen=100))
            
        # Reconciliation Setup
        self.reconciliation_service = ReconciliationService(
            api=self.api,
            db=self.db,
            symbols=settings.trade_symbols
        )

    @staticmethod
    def _build_strategy(strategy_name: str):
        normalized = strategy_name.strip().lower()
        if normalized == "enhanced_rsi":
            return EnhancedRSIScalping()
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

    def _on_ws_connect(self):
        self._market_data_degraded = False
        logger.info("Realtime market data restored on WebSocket feed")

    def _on_ws_disconnect(self, reason: str = ""):
        self._market_data_degraded = True
        logger.warning("Realtime market data WebSocket disconnected: %s", reason or "unknown reason")

    def _on_ws_alert(self, level: str, message: str, context: dict | None = None):
        log_fn = logger.warning if level.lower() == "warning" else logger.info
        if context:
            log_fn("Market data alert: %s | %s", message, context)
        else:
            log_fn("Market data alert: %s", message)

    def _on_ws_message(self, data: dict):
        """Handle incoming WebSocket messages."""
        msg_type = data.get("type")
        if msg_type == "v2/ticker":
            payload = data.get("payload", {})
            symbol = payload.get("symbol")
            price = self._extract_price(payload)
            if symbol and price > 0:
                self._price_history[symbol].append(price)
                
                # Append to OHLCV buffer (Simulating candles from ticker for simplicity in this turn)
                # In a real setup, we should subscribe to candles channel instead.
                self._ohlcv_history[symbol].append({
                    "open": price, "high": price, "low": price, "close": price, 
                    "volume": float(payload.get("volume", 0)), 
                    "timestamp": payload.get("timestamp")
                })
                # In a production bot, we might trigger strategy logic here 
                # or in a separate loop that checks the latest data.
        
        elif msg_type == "executions":
            self._handle_execution_report(data)

    def _fetch_market_snapshot(self) -> Dict[str, dict]:
        """Fetch latest prices and build market data snapshot."""
        market_data = {}
        use_rest_fallback = self._market_data_degraded or not self.ws_manager.is_healthy
        for symbol in self.settings.trade_symbols:
            history = list(self._price_history[symbol])
            if use_rest_fallback or not history:
                try:
                    ticker = self.api.get_ticker(symbol)
                    price = self._extract_price(ticker)
                    if price > 0:
                        self._price_history[symbol].append(price)
                        history = list(self._price_history[symbol])
                except Exception as e:
                    logger.error(f"Error fetching fallback price for {symbol}: {e}")
                    continue

            market_data[symbol] = {
                "prices": history, 
                "ticker": {},
                "df": pd.DataFrame(list(self._ohlcv_history[symbol])) if self._ohlcv_history[symbol] else None
            }
        return market_data

    def _handle_execution_report(self, data: dict):
        """Processes execution reports to update trade and position state."""
        payload = data.get("payload", {})
        symbol = payload.get("symbol")
        side = payload.get("side", "").lower()
        size = abs(float(payload.get("size", 0)))
        price = float(payload.get("avg_price", 0))
        state = payload.get("state", "").lower() # e.g. 'filled', 'partially_filled'
        
        logger.info(f"EXECUTION REPORT: {symbol} {side} {size} @ {price} ({state})")
        
        # Log to execution_logs
        self.db.log_execution({
            "execution_id": f"ws-{uuid.uuid4().hex[:8]}",
            "symbol": symbol,
            "trade_id": payload.get("client_order_id"), # Best effort mapping
            "event_type": "ws_execution",
            "side": side,
            "size": size,
            "price": price,
            "status": state
        })

        if state == "filled":
            # Update local position and database
            new_pos_size = self._update_local_position(symbol, side, size, price)
            logger.info(f"Position for {symbol} updated via WS execution. New size: {new_pos_size}")

    def _update_local_position(self, symbol: str, side: str, size: float, price: float = 0.0) -> float:
        delta = size if side == "buy" else -size
        current = self.positions.get(symbol, {"size": 0.0})
        new_size = current["size"] + delta
        
        if abs(new_size) < 1e-12:
            current_pos = self.positions.pop(symbol, None)
            if current_pos and "trade_id" in current_pos:
                self.db.close_trade(trade_id=current_pos["trade_id"], exit_price=price)
                self.db.close_position(symbol)
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
                self.db.create_trade({
                    "trade_id": trade_id,
                    "symbol": symbol,
                    "side": side_str,
                    "size": abs(new_size),
                    "entry_price": price,
                    "strategy_name": getattr(self.strategy, "name", self.settings.strategy_name)
                })
                self.db.update_position({
                    "symbol": symbol,
                    "trade_id": trade_id,
                    "side": side_str,
                    "size": abs(new_size),
                    "avg_entry_price": price
                })
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
            if updated_position == 0:
                self.execution_engine.clear_protection(symbol)
            
            self.db.log_execution({
                "execution_id": f"exec-{uuid.uuid4().hex[:8]}",
                "trade_id": triggered.get("trade_id"),
                "symbol": symbol,
                "event_type": "protection_trigger",
                "side": exit_side,
                "size": exit_size,
                "price": exit_price,
                "status": "filled"
            })
            logger.info("Protection exit executed (%s mode): %s", self.settings.mode, triggered)

    def _execute_signal(self, signal: Signal):
        if signal.action == "hold":
            return

        # Assess risk and get calculated position size
        risk_result = self.risk_manager.assess_signal(
            signal={"price": signal.price, "stop_loss": signal.stop_loss, "confidence": signal.confidence},
            current_positions=len(self.positions),
            balance=self.current_equity
        )
        
        if not risk_result["allowed"]:
            logger.warning(f"Risk check failed for {signal.symbol}: {risk_result['reason']}")
            return

        side = signal.action.lower()
        size = risk_result["size"]
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
                        max_slippage_pct=self.settings.max_slippage_pct
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

        self.db.log_execution({
            "execution_id": f"exec-{uuid.uuid4().hex[:8]}",
            "symbol": signal.symbol,
            "event_type": "signal_execution",
            "side": side,
            "size": size,
            "price": signal.price,
            "status": "submitted"
        })
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
                self.db.log_execution({
                    "execution_id": f"exec-{uuid.uuid4().hex[:8]}",
                    "symbol": symbol,
                    "event_type": "time_based_close",
                    "side": exit_side,
                    "size": abs(pos["size"]),
                    "price": 0.0,
                    "status": "submitted"
                })

    async def run(self, max_iterations: Optional[int] = None):
        logger.info("Starting trading engine in %s mode", self.settings.mode)
        await self.ws_manager.connect()
        
        # Start Reconciliation background task
        recon_task = asyncio.create_task(self.reconciliation_service.start())
        
        iteration = 0
        try:
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
                await asyncio.sleep(self.settings.trade_frequency_s)
        finally:
            recon_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await recon_task
            await self.ws_manager.disconnect()
            logger.info("Trading engine stopped after %s iterations", iteration)
