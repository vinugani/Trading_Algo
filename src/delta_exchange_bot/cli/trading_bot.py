import argparse
import logging
import time
import uuid
from typing import Optional

import pandas as pd

from delta_exchange_bot.api.delta_client import DeltaClient
from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.data.market_data import fetch_candles
from delta_exchange_bot.execution.order_execution_engine import OrderExecutionEngine
from delta_exchange_bot.monitoring.prometheus_exporter import PrometheusMetricsExporter
from delta_exchange_bot.persistence.db import StateDB
from delta_exchange_bot.risk.risk_management import MAX_DAILY_LOSS, MAX_LEVERAGE, MAX_RISK_PER_TRADE
from delta_exchange_bot.risk.risk_management import calculate_position_size, validate_trade
from delta_exchange_bot.strategy.base import Signal, Strategy
from delta_exchange_bot.strategy.ema_crossover import EMACrossoverStrategy
from delta_exchange_bot.strategy.momentum import MomentumStrategy
from delta_exchange_bot.strategy.rsi_scalping import RSIScalpingStrategy

logger = logging.getLogger(__name__)


class MainTradingBot:
    DEFAULT_STOP_LOSS_PCT = 0.004
    DEFAULT_TAKE_PROFIT_PCT = 0.008
    DEFAULT_TRAILING_STOP_PCT = 0.004

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = DeltaClient(settings.api_key, settings.api_secret, settings.api_url)
        live_client = self.client if settings.mode == "live" else None
        self.execution_engine = OrderExecutionEngine(live_client)
        self.db = StateDB(settings.state_db_path)
        self.metrics = PrometheusMetricsExporter()
        self.strategy = self._build_strategy(settings.strategy_name)
        self.account_equity = 100000.0
        self.start_of_day_equity = self.account_equity
        self._open_positions: dict[str, dict] = {}
        self._open_notional = 0.0
        self._last_no_trade_reason: Optional[str] = None
        self._load_open_positions_from_db()
        self._update_drawdown_metric()
        self._update_total_pnl_metric()

    @staticmethod
    def _build_strategy(strategy_name: str) -> Strategy:
        normalized = strategy_name.strip().lower()
        if normalized == "momentum":
            return MomentumStrategy()
        if normalized == "rsi_scalping":
            return RSIScalpingStrategy()
        if normalized == "ema_crossover":
            return EMACrossoverStrategy()
        raise ValueError(f"Unsupported strategy_name={strategy_name}")

    def fetch_market_data(self, symbol: str) -> pd.DataFrame:
        start = time.perf_counter()
        try:
            return fetch_candles(symbol, "1m", api_url=self.settings.api_url)
        except Exception:
            self.metrics.record_api_error("/v2/history/candles")
            raise
        finally:
            self.metrics.observe_api_latency("/v2/history/candles", time.perf_counter() - start)

    @staticmethod
    def calculate_indicators(candles: pd.DataFrame) -> dict[str, float]:
        if candles.empty or "close" not in candles.columns:
            return {}

        close = pd.to_numeric(candles["close"], errors="coerce").dropna()
        if close.empty:
            return {}

        high = pd.to_numeric(candles.get("high", pd.Series(dtype=float)), errors="coerce")
        low = pd.to_numeric(candles.get("low", pd.Series(dtype=float)), errors="coerce")
        volume = pd.to_numeric(candles.get("volume", pd.Series(dtype=float)), errors="coerce")

        ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1] if len(close) >= 20 else float("nan")

        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        
        # Wilder's Smoothing is exactly equivalent to EMA with alpha = 1 / period
        avg_gain = gain.ewm(alpha=1/14, adjust=False, min_periods=14).mean().iloc[-1]
        avg_loss = loss.ewm(alpha=1/14, adjust=False, min_periods=14).mean().iloc[-1]
        
        if pd.isna(avg_gain) or pd.isna(avg_loss):
            rsi = float("nan")
        elif avg_loss == 0:
            rsi = 100.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))

        # Session VWAP from available candle history.
        if not volume.empty and len(high) == len(close) and len(low) == len(close):
            typical_price = (high + low + close) / 3.0
            cum_volume = volume.cumsum().replace(0, pd.NA)
            vwap_series = (typical_price * volume).cumsum() / cum_volume
            vwap = vwap_series.iloc[-1] if not vwap_series.empty else float("nan")
        else:
            vwap = float("nan")

        # ATR(14) from OHLC candles.
        if len(high) == len(close) and len(low) == len(close) and len(close) >= 14:
            prev_close = close.shift(1)
            tr = pd.concat(
                [
                    (high - low).abs(),
                    (high - prev_close).abs(),
                    (low - prev_close).abs(),
                ],
                axis=1,
            ).max(axis=1)
            atr14 = tr.rolling(window=14, min_periods=14).mean().iloc[-1]
        else:
            atr14 = float("nan")

        return {
            "price": float(close.iloc[-1]),
            "ema20": float(ema20) if not pd.isna(ema20) else float("nan"),
            "rsi": float(rsi) if not pd.isna(rsi) else float("nan"),
            "vwap": float(vwap) if not pd.isna(vwap) else float("nan"),
            "atr14": float(atr14) if not pd.isna(atr14) else float("nan"),
        }

    def generate_strategy_signal(self, symbol: str, candles: pd.DataFrame) -> Signal:
        prices = pd.to_numeric(candles.get("close", pd.Series(dtype=float)), errors="coerce").dropna().tolist()
        if not prices:
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=0.0)

        market_snapshot = {symbol: {"prices": prices}}
        signals = self.strategy.generate(market_snapshot)
        if not signals:
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=float(prices[-1]))
        return signals[0]

    def _recalculate_open_notional(self) -> None:
        self._open_notional = sum(abs(pos["size"] * pos["entry_price"]) for pos in self._open_positions.values())

    def _log_no_trade_reason(self, symbol: str, reason: str, *, details: Optional[str] = None) -> None:
        if details:
            logger.info("No-trade for %s: reason=%s details=%s", symbol, reason, details)
        else:
            logger.info("No-trade for %s: reason=%s", symbol, reason)

    def _update_drawdown_metric(self) -> None:
        if self.start_of_day_equity <= 0:
            self.metrics.set_drawdown(0.0)
            return
        drawdown_pct = max(0.0, (self.start_of_day_equity - self.account_equity) / self.start_of_day_equity * 100.0)
        self.metrics.set_drawdown(drawdown_pct)

    def _update_total_pnl_metric(self) -> None:
        if hasattr(self.metrics, "set_total_pnl"):
            self.metrics.set_total_pnl(self.account_equity - self.start_of_day_equity)

    def _load_open_positions_from_db(self) -> None:
        restored = self.db.load_open_position_state(mode=self.settings.mode)
        self._open_positions = restored
        self._recalculate_open_notional()
        if not restored:
            return

        # Re-register protections after restart so lifecycle continues with same trade_id.
        for symbol, pos in restored.items():
            position_side = str(pos.get("side", "")).lower()
            size = float(pos.get("size", 0.0) or 0.0)
            trade_id = pos.get("trade_id")
            if size <= 0 or position_side not in {"long", "short"}:
                continue
            stop_loss = pos.get("stop_loss")
            if stop_loss is not None:
                self.execution_engine.place_stop_loss(
                    symbol,
                    position_side,
                    size=size,
                    stop_price=float(stop_loss),
                    trade_id=trade_id,
                )
            take_profit = pos.get("take_profit")
            if take_profit is not None:
                self.execution_engine.place_take_profit(
                    symbol,
                    position_side,
                    size=size,
                    target_price=float(take_profit),
                    trade_id=trade_id,
                )
            trailing_stop_pct = pos.get("trailing_stop_pct")
            entry_price = float(pos.get("entry_price", 0.0) or 0.0)
            if trailing_stop_pct is not None and entry_price > 0:
                self.execution_engine.set_trailing_stop(
                    symbol,
                    position_side,
                    size=size,
                    trail_pct=float(trailing_stop_pct),
                    entry_price=entry_price,
                    trade_id=trade_id,
                )

    @staticmethod
    def _new_trade_id(symbol: str) -> str:
        return f"{symbol}-{uuid.uuid4().hex}"

    @staticmethod
    def _extract_exchange_order_id(order_response: Optional[dict]) -> Optional[str]:
        if not isinstance(order_response, dict):
            return None
        result = order_response.get("result")
        if isinstance(result, dict):
            for key in ("id", "order_id"):
                value = result.get(key)
                if value is not None:
                    return str(value)
        for key in ("id", "order_id"):
            value = order_response.get(key)
            if value is not None:
                return str(value)
        return None

    def _process_protection_triggers(self, symbol: str, current_price: float) -> None:
        if symbol not in self._open_positions:
            return

        triggered = self.execution_engine.on_price_update(symbol, current_price)
        if not triggered:
            return

        logger.warning("Protection trigger executed for %s: %s", symbol, triggered)
        open_position = self._open_positions.pop(symbol, None)
        trade_id = triggered.get("trade_id") or (open_position or {}).get("trade_id") or self._new_trade_id(symbol)
        execution_id = f"{trade_id}:exit"
        exit_side = str(triggered.get("exit_side", "unknown"))
        exit_size = float(triggered.get("size", (open_position or {}).get("size", 0.0) or 0.0))
        entry_price = float((open_position or {}).get("entry_price", 0.0) or 0.0)
        exit_price = float(triggered.get("trigger_price", current_price))
        position_side = str((open_position or {}).get("side", "")).lower()
        if position_side == "long":
            pnl = (exit_price - entry_price) * exit_size
        elif position_side == "short":
            pnl = (entry_price - exit_price) * exit_size
        else:
            pnl = 0.0
        self.account_equity += pnl
        self.metrics.record_trade(pnl)
        self._update_drawdown_metric()
        self._update_total_pnl_metric()
        self.db.save_execution(
            trade_id=trade_id,
            execution_id=execution_id,
            symbol=symbol,
            side=exit_side,
            size=exit_size,
            price=exit_price,
            event_type="exit",
            order_type="market_order",
            mode=self.settings.mode,
            status="filled",
            reason=str(triggered.get("reason", "protection")),
            client_order_id=triggered.get("client_order_id"),
            exchange_order_id=triggered.get("exchange_order_id"),
            metadata={"trigger_payload": triggered},
        )
        self.db.remove_open_position_state(symbol)
        self._recalculate_open_notional()

    @classmethod
    def _default_stop_loss(cls, action: str, price: float) -> Optional[float]:
        if price <= 0:
            return None
        action_n = action.lower()
        if action_n == "buy":
            return price * (1.0 - cls.DEFAULT_STOP_LOSS_PCT)
        if action_n == "sell":
            return price * (1.0 + cls.DEFAULT_STOP_LOSS_PCT)
        return None

    @classmethod
    def _with_default_protection(cls, signal: Signal) -> Signal:
        action = signal.action.lower()
        if action not in {"buy", "sell"} or signal.price <= 0:
            return signal

        stop_loss = signal.stop_loss if signal.stop_loss is not None else cls._default_stop_loss(action, signal.price)
        if action == "buy":
            take_profit = signal.take_profit if signal.take_profit is not None else signal.price * (1.0 + cls.DEFAULT_TAKE_PROFIT_PCT)
        else:
            take_profit = signal.take_profit if signal.take_profit is not None else signal.price * (1.0 - cls.DEFAULT_TAKE_PROFIT_PCT)
        trailing_stop_pct = signal.trailing_stop_pct
        if trailing_stop_pct is None:
            trailing_stop_pct = cls.DEFAULT_TRAILING_STOP_PCT

        return Signal(
            symbol=signal.symbol,
            action=signal.action,
            confidence=signal.confidence,
            price=signal.price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_stop_pct=trailing_stop_pct,
        )

    def validate_risk(self, signal: Signal) -> tuple[bool, float]:
        self._last_no_trade_reason = None
        signal = self._with_default_protection(signal)
        if signal.stop_loss is None or signal.price <= 0:
            self._last_no_trade_reason = "invalid_signal_protection_or_price"
            return False, 0.0

        size = calculate_position_size(
            account_equity=self.account_equity,
            entry_price=signal.price,
            stop_loss_price=signal.stop_loss,
            current_open_notional=self._open_notional,
        )
        if size <= 0:
            self._last_no_trade_reason = "position_size_is_zero_after_risk_sizing"
            return False, 0.0

        is_valid = validate_trade(
            account_equity=self.account_equity,
            start_of_day_equity=self.start_of_day_equity,
            entry_price=signal.price,
            stop_loss_price=signal.stop_loss,
            position_size=size,
            current_open_notional=self._open_notional,
        )
        if not is_valid:
            if self.start_of_day_equity > 0:
                daily_loss_pct = max(
                    0.0,
                    (self.start_of_day_equity - self.account_equity) / self.start_of_day_equity,
                )
            else:
                daily_loss_pct = 0.0
            risk_amount = abs(signal.price - signal.stop_loss) * abs(size)
            risk_limit = self.account_equity * MAX_RISK_PER_TRADE
            projected_notional = abs(size) * signal.price + max(0.0, self._open_notional)
            projected_leverage = projected_notional / self.account_equity if self.account_equity > 0 else float("inf")

            if daily_loss_pct > MAX_DAILY_LOSS:
                self._last_no_trade_reason = (
                    f"daily_loss_limit_exceeded current={daily_loss_pct:.4f} limit={MAX_DAILY_LOSS:.4f}"
                )
            elif risk_amount > risk_limit:
                self._last_no_trade_reason = (
                    f"risk_per_trade_exceeded current={risk_amount:.4f} limit={risk_limit:.4f}"
                )
            elif projected_leverage > MAX_LEVERAGE:
                self._last_no_trade_reason = (
                    f"leverage_limit_exceeded current={projected_leverage:.4f} limit={MAX_LEVERAGE:.4f}"
                )
            else:
                self._last_no_trade_reason = "validate_trade_rejected_without_specific_threshold_breach"
        return is_valid, size

    @staticmethod
    def _is_filled_order(order: Optional[dict], assume_market_filled: bool) -> bool:
        if not isinstance(order, dict):
            return False
        result = order.get("result")
        order_payload = result if isinstance(result, dict) else order
        status = str(
            order_payload.get("status")
            or order_payload.get("state")
            or order_payload.get("order_state")
            or ""
        ).lower()
        if status in {"filled", "closed", "complete", "executed"}:
            return True
        if status in {"open", "new", "pending", "submitted", "partially_filled"}:
            return False
        # Some API responses omit status fields; treat success as filled to avoid
        # missing protection registration for immediately accepted orders.
        return order.get("success") is not False or assume_market_filled

    def execute_order(self, signal: Signal, size: float) -> Optional[dict]:
        signal = self._with_default_protection(signal)
        side = signal.action.lower()
        if side not in {"buy", "sell"}:
            return None

        trade_id = self._new_trade_id(signal.symbol)
        entry_execution_id = f"{trade_id}:entry"
        entry_client_order_id = f"{trade_id}-entry"

        if self.settings.mode != "live":
            logger.info(
                "Paper order: symbol=%s side=%s size=%.6f price=%.6f",
                signal.symbol,
                side,
                size,
                signal.price,
            )
            self._open_positions[signal.symbol] = {
                "trade_id": trade_id,
                "side": "long" if side == "buy" else "short",
                "size": size,
                "entry_price": signal.price,
                "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit,
                "trailing_stop_pct": signal.trailing_stop_pct,
            }
            self.db.save_execution(
                trade_id=trade_id,
                execution_id=entry_execution_id,
                symbol=signal.symbol,
                side=side,
                size=size,
                price=signal.price,
                event_type="entry",
                order_type="paper_limit",
                mode=self.settings.mode,
                status="filled",
                client_order_id=entry_client_order_id,
                metadata={"strategy": self.settings.strategy_name, "signal_confidence": signal.confidence},
            )
            self.db.upsert_open_position_state(
                symbol=signal.symbol,
                trade_id=trade_id,
                side=self._open_positions[signal.symbol]["side"],
                size=size,
                entry_price=signal.price,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                trailing_stop_pct=signal.trailing_stop_pct,
                mode=self.settings.mode,
            )
            position_side = self._open_positions[signal.symbol]["side"]
            if signal.stop_loss is not None:
                self.execution_engine.place_stop_loss(
                    signal.symbol,
                    position_side,
                    size=size,
                    stop_price=signal.stop_loss,
                    trade_id=trade_id,
                )
            if signal.take_profit is not None:
                self.execution_engine.place_take_profit(
                    signal.symbol,
                    position_side,
                    size=size,
                    target_price=signal.take_profit,
                    trade_id=trade_id,
                )
            if signal.trailing_stop_pct is not None and signal.price > 0:
                self.execution_engine.set_trailing_stop(
                    signal.symbol,
                    position_side,
                    size=size,
                    trail_pct=signal.trailing_stop_pct,
                    entry_price=signal.price,
                    trade_id=trade_id,
                )
            self._recalculate_open_notional()
            return {"paper": True}

        order_type = "limit_order" if signal.price > 0 else "market_order"
        if signal.price > 0:
            start = time.perf_counter()
            try:
                order = self.execution_engine.execute_limit_order(
                    symbol=signal.symbol,
                    side=side,
                    size=size,
                    price=signal.price,
                    client_order_id=entry_client_order_id,
                )
            except Exception:
                self.metrics.record_api_error("/v2/orders")
                raise
            finally:
                self.metrics.observe_api_latency("/v2/orders", time.perf_counter() - start)
        else:
            start = time.perf_counter()
            try:
                order = self.execution_engine.execute_market_order(
                    symbol=signal.symbol,
                    side=side,
                    size=size,
                    client_order_id=entry_client_order_id,
                )
            except Exception:
                self.metrics.record_api_error("/v2/orders")
                raise
            finally:
                self.metrics.observe_api_latency("/v2/orders", time.perf_counter() - start)

        is_filled = self._is_filled_order(order, assume_market_filled=(order_type == "market_order"))
        status = "filled" if is_filled else "submitted"
        position_side = "long" if side == "buy" else "short"
        self.db.save_execution(
            trade_id=trade_id,
            execution_id=entry_execution_id,
            symbol=signal.symbol,
            side=side,
            size=size,
            price=signal.price,
            event_type="entry",
            order_type=order_type,
            mode=self.settings.mode,
            status=status,
            client_order_id=entry_client_order_id,
            exchange_order_id=self._extract_exchange_order_id(order),
            metadata={"strategy": self.settings.strategy_name, "signal_confidence": signal.confidence},
        )
        if not is_filled:
            logger.info("Order for %s accepted but not filled yet; waiting for fill before opening local position", signal.symbol)
            return order

        position_side = "long" if side == "buy" else "short"
        if signal.stop_loss is not None:
            self.execution_engine.place_stop_loss(
                signal.symbol,
                position_side,
                size=size,
                stop_price=signal.stop_loss,
                trade_id=trade_id,
            )
        if signal.take_profit is not None:
            self.execution_engine.place_take_profit(
                signal.symbol,
                position_side,
                size=size,
                target_price=signal.take_profit,
                trade_id=trade_id,
            )
        if signal.trailing_stop_pct is not None and signal.price > 0:
            self.execution_engine.set_trailing_stop(
                signal.symbol,
                position_side,
                size=size,
                trail_pct=signal.trailing_stop_pct,
                entry_price=signal.price,
                trade_id=trade_id,
            )

        self._open_positions[signal.symbol] = {
            "trade_id": trade_id,
            "side": position_side,
            "size": size,
            "entry_price": signal.price,
            "stop_loss": signal.stop_loss,
            "take_profit": signal.take_profit,
            "trailing_stop_pct": signal.trailing_stop_pct,
        }
        self.db.upsert_open_position_state(
            symbol=signal.symbol,
            trade_id=trade_id,
            side=position_side,
            size=size,
            entry_price=signal.price,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            trailing_stop_pct=signal.trailing_stop_pct,
            mode=self.settings.mode,
        )
        self._recalculate_open_notional()
        return order

    def process_symbol(self, symbol: str) -> None:
        candles = self.fetch_market_data(symbol)
        indicators = self.calculate_indicators(candles)
        if not indicators:
            logger.warning("No usable market data for %s", symbol)
            self._log_no_trade_reason(symbol, "market_data_unavailable")
            return

        logger.info(
            "Indicators %s: price=%.6f ema20=%s rsi=%s vwap=%s atr14=%s",
            symbol,
            indicators["price"],
            f"{indicators['ema20']:.6f}" if pd.notna(indicators["ema20"]) else "nan",
            f"{indicators['rsi']:.2f}" if pd.notna(indicators["rsi"]) else "nan",
            f"{indicators['vwap']:.6f}" if pd.notna(indicators["vwap"]) else "nan",
            f"{indicators['atr14']:.6f}" if pd.notna(indicators["atr14"]) else "nan",
        )

        self._process_protection_triggers(symbol, indicators["price"])
        if symbol in self._open_positions:
            trade_id = self._open_positions[symbol].get("trade_id")
            logger.info("Skipping new entry for %s because position is already open", symbol)
            self._log_no_trade_reason(symbol, "existing_open_position", details=f"trade_id={trade_id}")
            return

        signal = self._with_default_protection(self.generate_strategy_signal(symbol, candles))
        logger.info("Signal %s: action=%s confidence=%.4f", symbol, signal.action, signal.confidence)
        if signal.action == "hold":
            self._log_no_trade_reason(symbol, "strategy_signal_hold")
            return

        ok, size = self.validate_risk(signal)
        if not ok:
            reason = self._last_no_trade_reason or "unknown_risk_rejection"
            logger.warning("Risk validation failed for %s: %s", symbol, reason)
            self._log_no_trade_reason(symbol, "risk_validation_failed", details=reason)
            return

        order = self.execute_order(signal, size)
        logger.info("Order execution response for %s: %s", symbol, order)

    def run(self, max_cycles: Optional[int] = None, sleep_interval_s: int = 60) -> None:
        logger.info("Starting main trading bot. mode=%s strategy=%s", self.settings.mode, self.settings.strategy_name)
        cycle = 0
        while max_cycles is None or cycle < max_cycles:
            cycle_start = time.time()
            for symbol in self.settings.trade_symbols:
                try:
                    self.process_symbol(symbol)
                except Exception as exc:
                    self.metrics.record_api_error("process_symbol")
                    logger.exception("Processing failed for %s: %s", symbol, exc)

            cycle += 1
            elapsed = time.time() - cycle_start
            to_sleep = max(0, sleep_interval_s - elapsed)
            if to_sleep > 0:
                time.sleep(to_sleep)

        logger.info("Main trading bot stopped after %s cycles", cycle)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Main Delta Exchange trading bot")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--strategy", choices=["momentum", "rsi_scalping", "ema_crossover"], default=None)
    parser.add_argument("--cycles", type=int, default=None, help="Optional number of loop cycles")
    parser.add_argument("--sleep-interval", type=int, default=60, help="Loop sleep interval in seconds (default: 60)")
    parser.add_argument("--metrics-port", type=int, default=8000, help="Prometheus metrics port")
    parser.add_argument("--metrics-addr", default="0.0.0.0", help="Prometheus bind address")
    parser.add_argument("--disable-metrics-server", action="store_true", help="Disable metrics HTTP exporter")
    args = parser.parse_args()

    kwargs = {"mode": args.mode}
    if args.strategy:
        kwargs["strategy_name"] = args.strategy
    settings = Settings(**kwargs)
    bot = MainTradingBot(settings)
    if not args.disable_metrics_server:
        bot.metrics.start_server(port=args.metrics_port, addr=args.metrics_addr)
        logger.info("Prometheus metrics exporter started on %s:%s", args.metrics_addr, args.metrics_port)
    bot.run(max_cycles=args.cycles, sleep_interval_s=args.sleep_interval)


if __name__ == "__main__":
    main()
