from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import time
import uuid
from datetime import datetime
from collections import defaultdict
from pathlib import Path
from typing import Optional

import pandas as pd

from delta_exchange_bot.api.delta_client import DeltaAPIError
from delta_exchange_bot.api.delta_client import DeltaClient
from delta_exchange_bot.core.safety import APICircuitBreaker
from delta_exchange_bot.core.safety import CircuitBreakerConfig
from delta_exchange_bot.core.safety import SafetyController
from delta_exchange_bot.core.settings import Settings
from delta_exchange_bot.data.market_data import fetch_candles
from delta_exchange_bot.data.realtime_market_data import RealtimeMarketDataService
from delta_exchange_bot.execution.fee_manager import FeeConfig
from delta_exchange_bot.execution.fee_manager import FeeManager
from delta_exchange_bot.execution.order_execution_engine import OrderExecutionEngine
from delta_exchange_bot.monitoring.prometheus_exporter import PrometheusMetricsExporter
from delta_exchange_bot.persistence.db import DatabaseManager
from delta_exchange_bot.risk.advanced_risk_manager import AdvancedRiskConfig
from delta_exchange_bot.risk.advanced_risk_manager import AdvancedRiskManager
from delta_exchange_bot.risk.risk_management import calculate_position_size
from delta_exchange_bot.risk.risk_management import validate_trade
from delta_exchange_bot.strategy.manager import StrategyManager
from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.strategy.ema_crossover import EMACrossoverStrategy
from delta_exchange_bot.strategy.momentum import MomentumStrategy
from delta_exchange_bot.strategy.rsi_scalping import RSIScalpingStrategy
from delta_exchange_bot.strategy.portfolio import PortfolioStrategy, CandlePortfolioEngineAdapter
from delta_exchange_bot.utils.logging import configure_logging

logger = logging.getLogger(__name__)


class ProfessionalTradingBot:
    DEFAULT_STOP_LOSS_PCT = 0.004
    DEFAULT_TAKE_PROFIT_PCT = 0.008
    DEFAULT_TRAILING_STOP_PCT = 0.004
    POSITION_STATE_TOLERANCE = 1e-8

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = DeltaClient(
            api_key=settings.api_key,
            api_secret=settings.api_secret,
            api_url=settings.api_url,
            ws_url=settings.ws_url,
        )
        live_client = self.client if settings.mode == "live" else None
        self.execution_engine = OrderExecutionEngine(live_client)
        self.execution_engine.default_spread_threshold_pct = settings.spread_threshold_pct
        self.execution_engine.default_slippage_threshold_pct = settings.max_slippage_pct
        self.execution_engine.default_chunk_size = settings.order_chunk_size
        self.fee_manager = FeeManager(
            FeeConfig(
                maker_fee_rate=settings.maker_fee_rate,
                taker_fee_rate=settings.taker_fee_rate,
            )
        )

        self.db = DatabaseManager(settings.postgres_dsn)
        self.metrics = PrometheusMetricsExporter()
        self.strategy_manager = StrategyManager()
        self.legacy_strategy = self._build_legacy_strategy(settings.strategy_name)
        self.candle_portfolio_adapter = CandlePortfolioEngineAdapter()

        risk_config = AdvancedRiskConfig(
            max_risk_per_trade=settings.max_risk_per_trade,
            max_daily_loss=settings.max_daily_loss,
            max_leverage=settings.max_leverage,
            max_asset_exposure=settings.max_asset_exposure,
        )
        self.advanced_risk = AdvancedRiskManager(risk_config)
        breaker = APICircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=settings.api_circuit_breaker_failure_threshold,
                cooldown_seconds=settings.api_circuit_breaker_cooldown_s,
            )
        )
        self.safety = SafetyController(breaker=breaker, daily_loss_limit=settings.max_daily_loss)

        self.market_data_service: Optional[RealtimeMarketDataService] = None
        if settings.websocket_enabled:
            self.market_data_service = RealtimeMarketDataService(
                ws_url=settings.ws_url,
                api_url=settings.api_url,
                symbols=settings.trade_symbols,
                reconnect_interval_s=settings.websocket_reconnect_interval_s,
                fallback_poll_interval_s=settings.websocket_fallback_poll_interval_s,
                ping_interval_s=settings.websocket_ping_interval_s,
                ping_timeout_s=settings.websocket_ping_timeout_s,
                stale_after_s=settings.websocket_stale_after_s,
            )
            self.market_data_service.add_listener(self._on_realtime_price)

        self._latest_price_cache: dict[str, float] = {}
        self._local_cache_positions: dict[str, dict] = {}
        self._exchange_state_positions: dict[str, dict] = {}
        self._open_positions = self._local_cache_positions
        self._last_position_sync_monotonic: dict[str, float] = defaultdict(float)
        self._open_notional_by_symbol: dict[str, float] = defaultdict(float)
        self._open_notional_total = 0.0

        self.account_equity = 100000.0
        self.start_of_day_equity = self.account_equity
        self._peak_equity = self.account_equity
        self._wins = 0
        self._losses = 0
        self._gross_profit = 0.0
        self._gross_loss = 0.0
        self._strategy_perf: dict[str, dict[str, float]] = defaultdict(
            lambda: {"trades": 0.0, "wins": 0.0, "pnl": 0.0}
        )
        self._last_no_trade_reason: Optional[str] = None
        self._kill_switch_triggered = False
        self._trading_paused = False
        self._pause_reason: Optional[str] = None
        self._stop_requested = False
        self._shutdown_signal_path = Path(self.settings.shutdown_signal_path)
        self._shutdown_signal_path.parent.mkdir(parents=True, exist_ok=True)

        self._load_open_positions_from_db()
        if self.settings.mode == "live":
            self._initialize_live_equity()
            self.startup_safety_check()
        elif self.settings.mode in {"paper", "backtest"}:
            # Also fetch real balance in paper/backtest so kill-switch thresholds
            # are calibrated against actual account size, not a fictional $100,000.
            try:
                payload = self.client.get_account_balance()
                balance = self._extract_available_usd_balance(payload)
                if balance > 0:
                    self.account_equity = balance
                    self.start_of_day_equity = balance
                    self._peak_equity = balance
                    logger.info(
                        "Paper/backtest equity initialised from exchange balance=%.2f",
                        balance,
                    )
            except Exception as exc:
                logger.warning(
                    "Could not fetch real balance for paper mode — using default %.2f: %s",
                    self.account_equity,
                    exc,
                )
        self._update_metrics_from_equity()

    @staticmethod
    def _build_legacy_strategy(strategy_name: str):
        normalized = strategy_name.strip().lower()
        if normalized == "momentum":
            return MomentumStrategy()
        if normalized == "rsi_scalping":
            return RSIScalpingStrategy()
        if normalized == "ema_crossover":
            return EMACrossoverStrategy()
        if normalized == "portfolio":
            return PortfolioStrategy()
        return RSIScalpingStrategy()

    def _load_open_positions_from_db(self) -> None:
        restored = {}
        for pos in self.db.get_all_active_positions():
            symbol = self._normalize_symbol(pos.get("symbol"))
            if not symbol:
                continue
            trade_id = pos.get("trade_id")
            # Restore the original entry timestamp so max_holding_time_s
            # is measured from actual entry, not from bot restart time.
            entry_ts = self.db.get_trade_entry_time(trade_id) if trade_id else None
            restored[symbol] = {
                "symbol": symbol,
                "trade_id": trade_id,
                "side": pos.get("side"),
                "size": float(pos.get("size", 0.0) or 0.0),
                "entry_price": float(pos.get("avg_entry_price", 0.0) or 0.0),
                "stop_loss": pos.get("stop_loss"),
                "take_profit": pos.get("take_profit"),
                "stop_order_id": pos.get("stop_order_id"),
                "tp_order_id": pos.get("tp_order_id"),
                "entry_ts": entry_ts,
                "source": "db_restore",
            }
        self._local_cache_positions = dict(restored)
        self._open_positions = self._local_cache_positions
        self._recalculate_open_notional()

    def _position_notional_usd(self, symbol: str, size_contracts: float, entry_price: float) -> float:
        """Convert exchange-reported position size (in contracts) to USD notional.

        Delta Exchange India reports positions in contracts, not in base-asset units.
        For inverse perpetuals (BTCUSD, ETHUSD, SOLUSD) each contract = 1 USD of
        exposure, so notional = size_contracts × contract_value_usd.
        For linear/quanto contracts contract_value is in base asset, so
        notional = size_contracts × contract_value_btc × price.
        Falls back to size × price when product metadata is unavailable.
        """
        if size_contracts <= 0 or entry_price <= 0:
            return 0.0
        row = self.client._get_product_row(symbol)
        if not isinstance(row, dict):
            return size_contracts * entry_price
        cv_raw = row.get("contract_value")
        try:
            cv = float(cv_raw) if cv_raw is not None else 0.0
        except (TypeError, ValueError):
            cv = 0.0
        if cv <= 0:
            return size_contracts * entry_price
        contract_type = str(row.get("contract_type") or "").lower()
        if "inverse" in contract_type:
            # Inverse perp: contract_value is in USD → notional = contracts × USD/contract
            return size_contracts * cv
        # Linear/quanto: contract_value is in base asset → notional = contracts × base × price
        return size_contracts * cv * entry_price

    def _recalculate_open_notional(self) -> None:
        by_symbol: dict[str, float] = defaultdict(float)
        total = 0.0
        for symbol, pos in self._open_positions.items():
            size = abs(float(pos.get("size", 0.0) or 0.0))
            entry = float(pos.get("entry_price", 0.0) or 0.0)
            notion = self._position_notional_usd(symbol, size, entry)
            by_symbol[symbol] += notion
            total += notion
        self._open_notional_by_symbol = by_symbol
        self._open_notional_total = total

    def request_shutdown(self, reason: str) -> None:
        self._stop_requested = True
        self._pause_reason = reason
        self._trading_paused = True
        try:
            self._shutdown_signal_path.write_text("shutdown_requested\n", encoding="utf-8")
        except Exception:
            pass
        logger.warning("Shutdown requested: reason=%s", reason)

    def halt_trading(self, reason: str) -> None:
        self._trading_paused = True
        self._pause_reason = reason
        logger.critical("Trading paused: reason=%s", reason)

    def resume_trading(self) -> None:
        self._trading_paused = False
        self._pause_reason = None
        logger.warning("Trading resumed after manual safety pause")

    def _shutdown_requested_via_file(self) -> bool:
        try:
            return self._shutdown_signal_path.exists()
        except Exception:
            return False

    def _clear_shutdown_signal(self) -> None:
        try:
            if self._shutdown_signal_path.exists():
                self._shutdown_signal_path.unlink()
        except Exception:
            return

    @staticmethod
    def _extract_rows(payload: dict) -> list[dict]:
        rows = payload.get("result") or payload.get("data") or []
        if not isinstance(rows, list):
            return []
        return [row for row in rows if isinstance(row, dict)]

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return str(symbol or "").strip().upper()

    @staticmethod
    def _extract_symbol_from_position_row(row: dict) -> str:
        value = row.get("symbol") or row.get("product_symbol") or row.get("product_id") or ""
        return str(value).strip().upper()

    @staticmethod
    def _extract_signed_size_from_position_row(row: dict) -> float:
        raw_size = row.get("size")
        if raw_size is None:
            raw_size = row.get("position_size")
        if raw_size is None:
            raw_size = row.get("net_size")
        if raw_size is None:
            raw_size = row.get("net_quantity")
        try:
            size = float(raw_size or 0.0)
        except (TypeError, ValueError):
            return 0.0
        side = str(row.get("side") or row.get("direction") or "").lower().strip()
        if side in {"short", "sell"} and size > 0:
            return -size
        if side in {"long", "buy"} and size < 0:
            return abs(size)
        return size

    @staticmethod
    def _extract_entry_price_from_position_row(row: dict) -> float:
        for key in (
            "entry_price",
            "avg_entry_price",
            "average_entry_price",
            "avg_price",
            "mark_price",
            "price",
        ):
            value = row.get(key)
            if value is None:
                continue
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                return parsed
        return 0.0

    def _local_signed_size(self, symbol: str) -> float:
        local = self._local_cache_positions.get(symbol)
        if not local:
            return 0.0
        size = float(local.get("size", 0.0) or 0.0)
        if str(local.get("side", "")).lower() == "short":
            size *= -1.0
        return size

    def _exchange_signed_size(self, symbol: str) -> float:
        snapshot = self._exchange_state_positions.get(symbol)
        if not snapshot:
            return 0.0
        return float(snapshot.get("signed_size", 0.0) or 0.0)

    def _fetch_exchange_position_snapshot(self, symbol: str) -> Optional[dict]:
        if self.settings.mode != "live":
            return {"symbol": symbol, "signed_size": 0.0, "size": 0.0, "side": "flat", "entry_price": 0.0}
        start = time.perf_counter()
        try:
            payload = self.client.get_positions(product_id=symbol)
            self.safety.breaker.record_success()
        except Exception:
            self.safety.breaker.record_failure()
            self.metrics.record_api_error("/v2/positions")
            return None
        finally:
            self.metrics.observe_api_latency("/v2/positions", time.perf_counter() - start)

        rows = self._extract_rows(payload)
        symbol_u = self._normalize_symbol(symbol)
        net_signed_size = 0.0
        weighted_entry = 0.0
        for row in rows:
            row_symbol = self._extract_symbol_from_position_row(row)
            if row_symbol and not row_symbol.isdigit() and row_symbol != symbol_u:
                continue
            signed = self._extract_signed_size_from_position_row(row)
            if abs(signed) <= self.settings.position_sync_tolerance:
                continue
            entry = self._extract_entry_price_from_position_row(row)
            net_signed_size += signed
            if entry > 0:
                weighted_entry += abs(signed) * entry

        abs_size = abs(net_signed_size)
        if abs_size <= self.settings.position_sync_tolerance:
            return {
                "symbol": symbol_u,
                "signed_size": 0.0,
                "size": 0.0,
                "side": "flat",
                "entry_price": 0.0,
                "fetched_at": time.time(),
            }

        side = "long" if net_signed_size > 0 else "short"
        entry_price = (weighted_entry / abs_size) if weighted_entry > 0 else 0.0
        return {
            "symbol": symbol_u,
            "signed_size": net_signed_size,
            "size": abs_size,
            "side": side,
            "entry_price": entry_price,
            "fetched_at": time.time(),
        }

    @staticmethod
    def _extract_underlying_asset_symbol(symbol: str) -> str:
        """Return the base asset from a trade symbol. BTCUSD → BTC, ETHUSD → ETH.

        The testnet /v2/positions endpoint requires either product_id or
        underlying_asset_symbol — it rejects calls with no parameters.
        This helper extracts the base so we can query per-underlying.
        """
        symbol_u = symbol.upper()
        for quote in ("USDT", "USD", "BTC", "ETH", "USDC"):
            if symbol_u.endswith(quote):
                return symbol_u[: -len(quote)]
        return symbol_u

    def _fetch_all_exchange_position_snapshots(self) -> Optional[dict[str, dict]]:
        if self.settings.mode != "live":
            return {}

        # The testnet /v2/positions endpoint requires underlying_asset_symbol or
        # product_id — a bare call (no params) returns HTTP 400 on testnet.
        # Query per unique underlying asset and merge, which works on both
        # testnet and live exchange.
        underlyings = {
            self._extract_underlying_asset_symbol(s)
            for s in self.settings.trade_symbols
        }

        all_rows: list[dict] = []
        for underlying in sorted(underlyings):
            start = time.perf_counter()
            try:
                payload = self.client.get_positions(underlying_asset_symbol=underlying)
                self.safety.breaker.record_success()
            except Exception:
                self.safety.breaker.record_failure()
                self.metrics.record_api_error("/v2/positions")
                return None
            finally:
                self.metrics.observe_api_latency("/v2/positions", time.perf_counter() - start)
            all_rows.extend(self._extract_rows(payload))

        grouped_rows: dict[str, list[dict]] = defaultdict(list)
        for row in all_rows:
            symbol = self._extract_symbol_from_position_row(row)
            if not symbol:
                continue
            grouped_rows[symbol].append(row)

        snapshots: dict[str, dict] = {}
        for symbol, symbol_rows in grouped_rows.items():
            net_signed_size = 0.0
            weighted_entry = 0.0
            for row in symbol_rows:
                signed = self._extract_signed_size_from_position_row(row)
                if abs(signed) <= self.settings.position_sync_tolerance:
                    continue
                entry = self._extract_entry_price_from_position_row(row)
                net_signed_size += signed
                if entry > 0:
                    weighted_entry += abs(signed) * entry
            abs_size = abs(net_signed_size)
            if abs_size <= self.settings.position_sync_tolerance:
                continue
            side = "long" if net_signed_size > 0 else "short"
            entry_price = (weighted_entry / abs_size) if weighted_entry > 0 else 0.0
            snapshots[symbol] = {
                "symbol": symbol,
                "signed_size": net_signed_size,
                "size": abs_size,
                "side": side,
                "entry_price": entry_price,
                "fetched_at": time.time(),
            }
        return snapshots

    def _tracked_position_symbols(self, *, include_exchange_cache: bool = True) -> set[str]:
        symbols = {self._normalize_symbol(s) for s in self.settings.trade_symbols}
        symbols.update(self._normalize_symbol(s) for s in self._local_cache_positions.keys())
        if include_exchange_cache:
            symbols.update(self._normalize_symbol(s) for s in self._exchange_state_positions.keys())
        return {symbol for symbol in symbols if symbol}

    def _log_position_snapshot(self, symbol: str, *, reason: str) -> None:
        symbol_u = self._normalize_symbol(symbol)
        local = self._local_cache_positions.get(symbol_u)
        exchange = self._exchange_state_positions.get(symbol_u)
        logger.info(
            "Position snapshot symbol=%s reason=%s exchange=%s local=%s",
            symbol_u,
            reason,
            exchange or {"symbol": symbol_u, "side": "flat", "size": 0.0, "entry_price": 0.0},
            local or {"symbol": symbol_u, "side": "flat", "size": 0.0, "entry_price": 0.0},
        )

    def _apply_exchange_snapshot(
        self,
        symbol: str,
        snapshot: dict,
        *,
        reason: str,
        preferred_trade_id: Optional[str] = None,
        preferred_strategy_name: Optional[str] = None,
        protection_signal: Optional[Signal] = None,
    ) -> bool:
        symbol_u = self._normalize_symbol(symbol)
        self._exchange_state_positions[symbol_u] = snapshot
        previous = self._local_cache_positions.get(symbol_u, {})
        self._log_position_snapshot(symbol_u, reason=f"{reason}:before_apply")

        signed_size = float(snapshot.get("signed_size", 0.0) or 0.0)
        if abs(signed_size) <= self.settings.position_sync_tolerance:
            if symbol_u in self._local_cache_positions:
                logger.warning(
                    "Position sync flattening local cache symbol=%s reason=%s previous_size=%s",
                    symbol_u,
                    reason,
                    previous.get("size"),
                )
            self._local_cache_positions.pop(symbol_u, None)
            self.db.close_position(symbol_u)
            self.execution_engine.clear_protection(symbol_u)
            self._recalculate_open_notional()
            self._log_position_snapshot(symbol_u, reason=f"{reason}:after_flatten")
            logger.info("Position sync event symbol=%s exchange_size=0 reason=%s", symbol_u, reason)
            return True

        side = "long" if signed_size > 0 else "short"
        abs_size = abs(signed_size)
        entry_price = float(snapshot.get("entry_price", 0.0) or 0.0)
        if entry_price <= 0:
            entry_price = float(previous.get("entry_price", 0.0) or previous.get("avg_entry_price", 0.0) or 0.0)
        stop_loss = previous.get("stop_loss")
        take_profit = previous.get("take_profit")
        trailing_stop_pct = previous.get("trailing_stop_pct")
        if protection_signal is not None:
            if stop_loss is None:
                stop_loss = protection_signal.stop_loss
            if take_profit is None:
                take_profit = protection_signal.take_profit
            if trailing_stop_pct is None:
                trailing_stop_pct = protection_signal.trailing_stop_pct

        # FIX: When a position is discovered via exchange-sync (e.g. after a
        # testnet lag caused the post-entry confirmation to fail) and the
        # previous local state had no SL/TP recorded, apply default percentage-
        # based protections so the position is never left unprotected.
        if stop_loss is None and entry_price > 0:
            stop_loss = round(
                entry_price * (1 - self.DEFAULT_STOP_LOSS_PCT)
                if side == "long"
                else entry_price * (1 + self.DEFAULT_STOP_LOSS_PCT),
                8,
            )
            logger.warning(
                "Applied default stop_loss for exchange-synced position symbol=%s side=%s entry=%s sl=%s",
                symbol_u, side, entry_price, stop_loss,
            )
        if take_profit is None and entry_price > 0:
            take_profit = round(
                entry_price * (1 + self.DEFAULT_TAKE_PROFIT_PCT)
                if side == "long"
                else entry_price * (1 - self.DEFAULT_TAKE_PROFIT_PCT),
                8,
            )
            logger.warning(
                "Applied default take_profit for exchange-synced position symbol=%s side=%s entry=%s tp=%s",
                symbol_u, side, entry_price, take_profit,
            )
        if trailing_stop_pct is None:
            trailing_stop_pct = self.DEFAULT_TRAILING_STOP_PCT

        # Safety gate: refuse to adopt an exchange-synced position whose notional
        # exceeds max_leverage × account_equity.  An orphaned 1 BTC position on a
        # $106 account causes a $400+ loss on a 0.6% move, which immediately blows
        # through the 5% daily loss limit and fires the kill switch.
        if entry_price > 0 and self.account_equity > 0:
            synced_notional = self._position_notional_usd(symbol_u, abs_size, entry_price)
            max_allowed_notional = self.account_equity * self.settings.max_leverage
            if synced_notional > max_allowed_notional:
                logger.critical(
                    "Exchange-synced position REJECTED — notional=%.2f exceeds "
                    "max_allowed=%.2f (equity=%.2f × max_leverage=%.1f). "
                    "The position exists on the exchange but the account cannot "
                    "safely absorb its risk. Close it manually on the exchange.",
                    synced_notional,
                    max_allowed_notional,
                    self.account_equity,
                    self.settings.max_leverage,
                )
                self._log_position_snapshot(symbol_u, reason=f"{reason}:rejected_over_notional")
                return False

        trade_id = (
            preferred_trade_id
            or str(previous.get("trade_id") or "")
            or self._new_trade_id(symbol_u)
        )
        strategy_name = (
            preferred_strategy_name
            or str(previous.get("strategy_name") or "")
            or "exchange_synced"
        )
        realized_accum = float(previous.get("realized_pnl_accum", 0.0) or 0.0)
        entry_order_type = str(previous.get("entry_order_type", "market_order"))

        local_state = {
            "trade_id": trade_id,
            "side": side,
            "size": abs_size,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_stop_pct": trailing_stop_pct,
            "strategy_name": strategy_name,
            "realized_pnl_accum": realized_accum,
            "entry_order_type": entry_order_type,
            "source": "exchange_sync",
        }
        self._local_cache_positions[symbol_u] = local_state
        self.db.save_trade(
            trade_id=trade_id,
            symbol=symbol_u,
            side=side,
            size=abs_size,
            entry_price=entry_price,
            strategy_name=strategy_name,
            metadata={"source": "exchange_sync", "reason": reason},
        )
        self.db.update_position({
            "symbol": symbol_u,
            "trade_id": trade_id,
            "side": side,
            "size": abs_size,
            "avg_entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit
        })
        self._refresh_protection_for_symbol(symbol_u)
        self._recalculate_open_notional()
        self._log_position_snapshot(symbol_u, reason=f"{reason}:after_apply")
        logger.info(
            "Position sync event symbol=%s side=%s size=%s entry=%s reason=%s",
            symbol_u,
            side,
            abs_size,
            entry_price,
            reason,
        )
        return True

    def _refresh_protection_for_symbol(self, symbol: str) -> None:
        position = self._local_cache_positions.get(symbol)
        if not position:
            self.execution_engine.clear_protection(symbol)
            return
        size = abs(float(position.get("size", 0.0) or 0.0))
        if size <= self.settings.position_sync_tolerance:
            self.execution_engine.clear_protection(symbol)
            return
        side = str(position.get("side", "long")).lower()
        trade_id = str(position.get("trade_id") or self._new_trade_id(symbol))
        entry_price = float(position.get("entry_price", 0.0) or 0.0)

        stop_loss = position.get("stop_loss")
        if stop_loss is not None:
            self.execution_engine.place_stop_loss(
                symbol=symbol,
                position_side=side,
                size=size,
                stop_price=float(stop_loss),
                trade_id=trade_id,
            )
        take_profit = position.get("take_profit")
        if take_profit is not None:
            self.execution_engine.place_take_profit(
                symbol=symbol,
                position_side=side,
                size=size,
                target_price=float(take_profit),
                trade_id=trade_id,
            )
        # After (re-)placing protection orders, restore any exchange order IDs
        # that were persisted before a restart.  This prevents duplicate orders:
        # the newly placed orders' IDs are overwritten with the stored IDs so
        # that only the original exchange orders are tracked for cancellation.
        stored_stop_id = position.get("stop_order_id")
        stored_tp_id = position.get("tp_order_id")
        if stored_stop_id or stored_tp_id:
            self.execution_engine.restore_protection_order_ids(
                symbol=symbol,
                stop_order_id=stored_stop_id,
                tp_order_id=stored_tp_id,
            )
        trailing = position.get("trailing_stop_pct")
        if trailing is not None and entry_price > 0:
            self.execution_engine.set_trailing_stop(
                symbol=symbol,
                position_side=side,
                size=size,
                trail_pct=float(trailing),
                entry_price=entry_price,
                trade_id=trade_id,
            )

    def sync_position_with_exchange(
        self,
        symbol: str,
        *,
        reason: str = "runtime",
        preferred_trade_id: Optional[str] = None,
        preferred_strategy_name: Optional[str] = None,
        protection_signal: Optional[Signal] = None,
    ) -> bool:
        symbol_u = self._normalize_symbol(symbol)
        snapshot = self._fetch_exchange_position_snapshot(symbol_u)
        if snapshot is None:
            logger.critical("POSITION_SYNC_FAILED symbol=%s reason=%s", symbol_u, reason)
            return False
        return self._apply_exchange_snapshot(
            symbol_u,
            snapshot,
            reason=reason,
            preferred_trade_id=preferred_trade_id,
            preferred_strategy_name=preferred_strategy_name,
            protection_signal=protection_signal,
        )

    def reconcile_positions_with_exchange(self, *, reason: str = "cycle_reconciliation") -> bool:
        if self.settings.mode != "live":
            return True
        snapshots = self._fetch_all_exchange_position_snapshots()
        if snapshots is None:
            logger.critical("POSITION_RECONCILIATION_FAILED reason=%s", reason)
            return False

        tracked_symbols = self._tracked_position_symbols(include_exchange_cache=False)
        tracked_symbols.update(snapshots.keys())
        self._exchange_state_positions = dict(snapshots)
        logger.info(
            "Starting position reconciliation reason=%s tracked_symbols=%s exchange_positions=%s",
            reason,
            sorted(tracked_symbols),
            sorted(snapshots.keys()),
        )
        if not snapshots:
            logger.warning("Exchange returned no open positions during reconciliation reason=%s", reason)

        for symbol in sorted(tracked_symbols):
            snapshot = snapshots.get(symbol)
            if snapshot is None:
                snapshot = {
                    "symbol": symbol,
                    "signed_size": 0.0,
                    "size": 0.0,
                    "side": "flat",
                    "entry_price": 0.0,
                    "fetched_at": time.time(),
                }
            if not self._apply_exchange_snapshot(symbol, snapshot, reason=reason):
                return False

        logger.info("Position reconciliation completed reason=%s", reason)
        # FIX: Mark all reconciled symbols as freshly synced so that the
        # per-symbol pre_symbol_cycle call (which uses a separate API endpoint
        # and may return stale/different data on testnet) is suppressed for
        # min_interval_s seconds.  This prevents reconciliation results from
        # being immediately overwritten by a stale per-symbol snapshot.
        now = time.monotonic()
        for symbol in tracked_symbols:
            self._last_position_sync_monotonic[self._normalize_symbol(symbol)] = now
        return True

    def _sync_position_if_due(self, symbol: str, reason: str, min_interval_s: float = 2.0) -> bool:
        symbol_u = self._normalize_symbol(symbol)
        now = time.monotonic()
        last = float(self._last_position_sync_monotonic.get(symbol_u, 0.0) or 0.0)
        if (now - last) < max(0.1, float(min_interval_s)):
            return True
        ok = self.sync_position_with_exchange(symbol_u, reason=reason)
        if ok:
            self._last_position_sync_monotonic[symbol_u] = now
        return ok

    def validate_position_consistency(self, symbol: str) -> bool:
        symbol_u = self._normalize_symbol(symbol)
        self._log_position_snapshot(symbol_u, reason="consistency_check")
        local_signed = self._local_signed_size(symbol_u)
        exchange_signed = self._exchange_signed_size(symbol_u)
        mismatch = self.safety.detect_position_mismatch(
            local_size=local_signed,
            exchange_size=exchange_signed,
            tolerance=self.settings.position_sync_tolerance,
        )
        if not mismatch:
            return True
        logger.critical(
            "POSITION_MISMATCH_DETECTED symbol=%s local_size=%s exchange_size=%s",
            symbol_u,
            local_signed,
            exchange_signed,
        )
        logger.critical("FORCED_RESYNC_TRIGGERED symbol=%s", symbol_u)
        if not self.sync_position_with_exchange(symbol_u, reason="forced_resync"):
            self.halt_trading(f"position_sync_failed:{symbol_u}")
            return False
        local_after = self._local_signed_size(symbol_u)
        exchange_after = self._exchange_signed_size(symbol_u)
        still_bad = self.safety.detect_position_mismatch(
            local_size=local_after,
            exchange_size=exchange_after,
            tolerance=self.settings.position_sync_tolerance,
        )
        if still_bad:
            self.halt_trading(f"unresolved_position_mismatch:{symbol_u}")
            return False
        return True

    def _validate_pre_execution(self, symbol: str, side: str) -> tuple[bool, float]:
        symbol_u = self._normalize_symbol(symbol)
        if not self.sync_position_with_exchange(symbol_u, reason="pre_trade"):
            self.halt_trading(f"pre_trade_sync_failed:{symbol_u}")
            return False, 0.0
        if not self.validate_position_consistency(symbol_u):
            return False, 0.0
        signed = self._local_signed_size(symbol_u)
        if abs(signed) > self.settings.position_sync_tolerance:
            local_side = "buy" if signed > 0 else "sell"
            if local_side != side:
                self._last_no_trade_reason = "conflicting_live_position"
                logger.warning(
                    "Pre-trade validation blocked symbol=%s side=%s existing_signed_size=%s",
                    symbol_u,
                    side,
                    signed,
                )
                return False, signed
            self._last_no_trade_reason = "existing_open_position"
            return False, signed
        return True, signed

    def _validate_post_execution(
        self,
        *,
        symbol: str,
        side: str,
        before_signed: float,
        filled: bool,
    ) -> bool:
        symbol_u = self._normalize_symbol(symbol)

        # Paper mode: position is tracked locally only; exchange always returns
        # size=0.  Skip exchange sync entirely — local state is already correct.
        if self.settings.mode != "live":
            return True

        # FIX: Save local position state before polling the exchange.
        # Testnet has 3-10s propagation lag — repeated sync calls in the retry
        # loop would otherwise flatten the just-registered position (because
        # exchange still returns size=0), wiping out the SL/TP values that
        # _register_entry_position already stored in the local cache.
        _saved_local = dict(self._local_cache_positions.get(symbol_u) or {})

        retries = max(1, int(self.settings.position_sync_retries))
        delay_s = max(0.1, float(self.settings.position_sync_retry_delay_s))
        for attempt in range(1, retries + 1):
            if not self.sync_position_with_exchange(symbol_u, reason=f"post_trade_attempt_{attempt}"):
                if attempt == retries:
                    self.halt_trading(f"post_trade_sync_failed:{symbol_u}")
                    return False
                time.sleep(delay_s)
                continue
            if not filled:
                return self.validate_position_consistency(symbol_u)
            after_signed = self._local_signed_size(symbol_u)
            if side == "buy" and after_signed > before_signed + self.settings.position_sync_tolerance:
                return self.validate_position_consistency(symbol_u)
            if side == "sell" and after_signed < before_signed - self.settings.position_sync_tolerance:
                return self.validate_position_consistency(symbol_u)
            if attempt < retries:
                time.sleep(delay_s)
                continue
            # All retries exhausted and exchange still hasn't reflected the new
            # position.  This is typically an exchange settlement lag (3-10 s on
            # Delta India).  Log it as critical but do NOT hard-halt — the order
            # was accepted by the exchange (is_filled=True) so halting here would
            # leave an orphaned live position with no local tracking.
            #
            # FIX: Restore the pre-validation local state so the position and its
            # SL/TP protections are NOT lost just because the exchange hasn't
            # settled yet.  The next cycle's reconciliation will overwrite this
            # with the authoritative exchange state once it propagates.
            if _saved_local:
                self._local_cache_positions[symbol_u] = _saved_local
                self._refresh_protection_for_symbol(symbol_u)
                logger.warning(
                    "POST_EXECUTION_POSITION_NOT_CONFIRMED — restored pre-validation "
                    "local state symbol=%s side=%s retries=%s",
                    symbol_u,
                    side,
                    retries,
                )
            # When before==after==0 the exchange simply hasn't settled yet
            # (testnet latency).  The order was accepted; CRITICAL is misleading
            # here.  Downgrade to WARNING — the next reconciliation cycle picks it up.
            if before_signed == 0.0 and after_signed == 0.0:
                logger.warning(
                    "POST_EXECUTION_PENDING symbol=%s side=%s retries=%s — "
                    "order accepted; exchange confirmation pending (settlement latency). "
                    "Next cycle reconciliation will confirm.",
                    symbol_u,
                    side,
                    retries,
                )
                return True
            logger.critical(
                "POST_EXECUTION_POSITION_NOT_CONFIRMED symbol=%s side=%s "
                "before=%s after=%s retries=%s — order was accepted; continuing "
                "with local position state. Verify on exchange dashboard.",
                symbol_u,
                side,
                before_signed,
                after_signed,
                retries,
            )
            # Return True so the position stays tracked locally. A subsequent
            # cycle's pre_symbol_cycle sync will reconcile once exchange settles.
            return True
        return False

    def cancel_open_orders(self, symbol: Optional[str] = None) -> int:
        if self.settings.mode != "live":
            return 0
        try:
            payload = self.client.get_open_orders()
            self.safety.breaker.record_success()
        except Exception:
            self.metrics.record_api_error("/v2/orders?status=open")
            self.safety.breaker.record_failure()
            return 0
        rows = self._extract_rows(payload)
        cancelled = 0
        for row in rows:
            row_symbol = self._normalize_symbol(row.get("product_id") or row.get("symbol") or row.get("product_symbol"))
            if symbol and row_symbol != self._normalize_symbol(symbol):
                continue
            order_id = row.get("id") or row.get("order_id")
            if not order_id:
                continue
            try:
                self.client.cancel_order(order_id=str(order_id), symbol=row_symbol or symbol)
                cancelled += 1
            except Exception:
                self.metrics.record_api_error("/v2/orders/cancel")
                self.safety.breaker.record_failure()
        if cancelled > 0:
            logger.warning("Cancelled open orders count=%s symbol=%s", cancelled, symbol or "ALL")
        return cancelled

    def flatten_position_safely(self, symbol: str) -> bool:
        symbol_u = self._normalize_symbol(symbol)
        if self.settings.mode != "live":
            return True
        if not self.sync_position_with_exchange(symbol_u, reason="flatten_pre"):
            return False
        signed = self._local_signed_size(symbol_u)
        if abs(signed) <= self.settings.position_sync_tolerance:
            self.cancel_open_orders(symbol_u)
            return True
        side = "sell" if signed > 0 else "buy"
        size = abs(signed)
        client_order_id = self._safe_client_order_id(f"{symbol_u}-{uuid.uuid4().hex[:10]}-emergency-exit")
        logger.warning(
            "Flattening position symbol=%s side=%s size=%s via market reduce_only order",
            symbol_u,
            side,
            size,
        )
        start = time.perf_counter()
        try:
            self.execution_engine.execute_market_order(
                symbol=symbol_u,
                side=side,
                size=size,
                reduce_only=True,
                client_order_id=client_order_id,
            )
            self.safety.breaker.record_success()
        except Exception:
            self.metrics.record_api_error("/v2/orders")
            self.safety.breaker.record_failure()
            return False
        finally:
            self.metrics.observe_api_latency("/v2/orders", time.perf_counter() - start)

        retries = max(1, int(self.settings.position_sync_retries))
        delay_s = max(0.2, float(self.settings.position_sync_retry_delay_s))
        for _ in range(retries):
            time.sleep(delay_s)
            if not self.sync_position_with_exchange(symbol_u, reason="flatten_verify"):
                continue
            remaining = abs(self._local_signed_size(symbol_u))
            if remaining <= self.settings.position_sync_tolerance:
                self.cancel_open_orders(symbol_u)
                logger.warning("Flatten success symbol=%s", symbol_u)
                return True
        logger.critical("Flatten failed symbol=%s remaining_size=%s", symbol_u, abs(self._local_signed_size(symbol_u)))
        return False

    def startup_safety_check(self) -> None:
        if self.settings.mode != "live":
            return
        if not self.reconcile_positions_with_exchange(reason="startup"):
            self.halt_trading("startup_position_reconciliation_failed")
            return

        symbols = self._tracked_position_symbols()

        if self.settings.cancel_leftover_orders_on_startup:
            cancelled = self.cancel_open_orders()
            if cancelled > 0:
                logger.warning("Startup safety cancelled leftover open orders count=%s", cancelled)

        for symbol in sorted(symbols):
            if symbol and not self.validate_position_consistency(symbol):
                self.halt_trading(f"startup_position_mismatch:{symbol}")

    def _on_realtime_price(self, symbol: str, price: float) -> None:
        self._latest_price_cache[symbol] = float(price)
        if self.settings.mode == "live" and symbol in self._local_cache_positions:
            # FIX: Use 15s min_interval to match the pre_symbol_cycle window.
            # The 2s window was too short — reconciliation stamps the sync time,
            # but the WebSocket callback fires ~2s later, passes the check, hits
            # the exchange (testnet still shows flat due to propagation lag) and
            # flattens the just-confirmed position, causing the re-ordering loop.
            self._sync_position_if_due(symbol, reason="realtime_price", min_interval_s=15.0)
            self.validate_position_consistency(symbol)
        triggered = self.execution_engine.on_price_update(symbol, float(price))
        if triggered:
            self._handle_exit(symbol=symbol, current_price=float(price), triggered=triggered)

    @staticmethod
    def _new_trade_id(symbol: str) -> str:
        return f"{symbol}-{uuid.uuid4().hex}"

    @staticmethod
    def _extract_available_usd_balance(payload: dict) -> float:
        rows = payload.get("result") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            return 0.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            asset = str(row.get("asset_symbol") or "").upper()
            if asset not in {"USD", "USDT"}:
                continue
            try:
                return max(0.0, float(row.get("available_balance", 0.0)))
            except (TypeError, ValueError):
                return 0.0
        return 0.0

    def _initialize_live_equity(self) -> None:
        from datetime import timezone as _tz
        today = datetime.now(_tz.utc).strftime("%Y-%m-%d")

        # Fetch current balance from exchange first.
        balance = 0.0
        try:
            payload = self.client.get_account_balance()
            balance = self._extract_available_usd_balance(payload)
        except Exception:
            pass

        if balance > 0:
            self.account_equity = balance
            self._peak_equity = max(self._peak_equity, balance)

        # Restore today's start_of_day_equity from DB so the daily kill switch
        # is not defeated by a bot restart mid-day.
        saved_sod = self.db.get_float_state("start_of_day_equity", date_str=today)
        if saved_sod is not None and saved_sod > 0:
            self.start_of_day_equity = saved_sod
            logger.info(
                "Restored start_of_day_equity=%.2f from DB (today=%s) — "
                "daily kill switch baseline preserved across restart",
                saved_sod, today,
            )
        elif balance > 0:
            # First startup today — record current balance as the day's baseline.
            self.start_of_day_equity = balance
            self.db.set_float_state("start_of_day_equity", balance, date_str=today)
            logger.info(
                "Initialized start_of_day_equity=%.2f for today=%s",
                balance, today,
            )

    def _refresh_live_equity(self) -> None:
        if self.settings.mode != "live":
            return
        try:
            payload = self.client.get_account_balance()
            balance = self._extract_available_usd_balance(payload)
            if balance > 0:
                self.account_equity = balance
                self._peak_equity = max(self._peak_equity, balance)
        except Exception as exc:
            logger.warning(
                "_refresh_live_equity failed — kill-switch will use stale equity=%.2f: %s",
                self.account_equity,
                exc,
            )

    @staticmethod
    def _safe_client_order_id(raw: str, max_len: int = 32) -> str:
        value = str(raw)
        if len(value) <= max_len:
            return value
        digest = uuid.uuid5(uuid.NAMESPACE_DNS, value).hex[:8]
        prefix_len = max(1, max_len - 9)
        return f"{value[:prefix_len]}-{digest}"

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

    @classmethod
    def _with_default_protection(cls, signal: Signal) -> Signal:
        action = signal.action.lower()
        if action not in {"buy", "sell"}:
            return signal
        if signal.price <= 0:
            return signal

        stop_loss = signal.stop_loss
        take_profit = signal.take_profit
        trailing = signal.trailing_stop_pct
        if stop_loss is None:
            if action == "buy":
                stop_loss = signal.price * (1.0 - cls.DEFAULT_STOP_LOSS_PCT)
            else:
                stop_loss = signal.price * (1.0 + cls.DEFAULT_STOP_LOSS_PCT)
        if take_profit is None:
            if action == "buy":
                take_profit = signal.price * (1.0 + cls.DEFAULT_TAKE_PROFIT_PCT)
            else:
                take_profit = signal.price * (1.0 - cls.DEFAULT_TAKE_PROFIT_PCT)
        if trailing is None:
            trailing = cls.DEFAULT_TRAILING_STOP_PCT

        return Signal(
            symbol=signal.symbol,
            action=signal.action,
            confidence=signal.confidence,
            price=signal.price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_stop_pct=trailing,
        )

    def fetch_market_data(self, symbol: str) -> pd.DataFrame:
        start = time.perf_counter()
        try:
            candles = fetch_candles(symbol, "1m", api_url=self.settings.api_url)
            self.safety.breaker.record_success()
            return self._inject_realtime_price(symbol, candles)
        except Exception:
            self.metrics.record_api_error("/v2/history/candles")
            self.safety.breaker.record_failure()
            raise
        finally:
            self.metrics.observe_api_latency("/v2/history/candles", time.perf_counter() - start)

    def _inject_realtime_price(self, symbol: str, candles: pd.DataFrame) -> pd.DataFrame:
        latest_price = self._latest_price_cache.get(symbol)
        if latest_price is None or candles.empty:
            return candles
        out = candles.copy()
        last_idx = out.index[-1]
        for col in ("close", "high", "low", "open"):
            if col not in out.columns:
                continue
            current = float(out.at[last_idx, col]) if pd.notna(out.at[last_idx, col]) else latest_price
            if col == "high":
                out.at[last_idx, col] = max(current, latest_price)
            elif col == "low":
                out.at[last_idx, col] = min(current, latest_price)
            else:
                out.at[last_idx, col] = latest_price
        return out

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
        ema12_series = close.ewm(span=12, adjust=False).mean() if len(close) >= 12 else pd.Series(dtype=float)
        ema26_series = close.ewm(span=26, adjust=False).mean() if len(close) >= 26 else pd.Series(dtype=float)
        if not ema12_series.empty and not ema26_series.empty:
            macd_series = ema12_series - ema26_series
            macd = macd_series.iloc[-1]
            if len(macd_series.dropna()) >= 9:
                macd_signal = macd_series.ewm(span=9, adjust=False).mean().iloc[-1]
                macd_histogram = macd - macd_signal
            else:
                macd_signal = float("nan")
                macd_histogram = float("nan")
        else:
            macd = float("nan")
            macd_signal = float("nan")
            macd_histogram = float("nan")
        delta = close.diff()
        gains = delta.clip(lower=0)
        losses = -delta.clip(upper=0)
        avg_gain = gains.rolling(window=14, min_periods=14).mean().iloc[-1]
        avg_loss = losses.rolling(window=14, min_periods=14).mean().iloc[-1]
        if pd.isna(avg_gain) or pd.isna(avg_loss):
            rsi = float("nan")
        elif avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))

        if not volume.empty and len(high) == len(close) and len(low) == len(close):
            typical_price = (high + low + close) / 3.0
            cum_volume = volume.cumsum().replace(0, pd.NA)
            vwap_series = (typical_price * volume).cumsum() / cum_volume
            vwap = vwap_series.iloc[-1] if not vwap_series.empty else float("nan")
        else:
            vwap = float("nan")

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
            "macd": float(macd) if not pd.isna(macd) else float("nan"),
            "macd_signal": float(macd_signal) if not pd.isna(macd_signal) else float("nan"),
            "macd_histogram": float(macd_histogram) if not pd.isna(macd_histogram) else float("nan"),
        }

    def _generate_legacy_signal(self, symbol: str, candles: pd.DataFrame) -> Signal:
        prices = pd.to_numeric(candles.get("close", pd.Series(dtype=float)), errors="coerce").dropna().tolist()
        if not prices:
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=0.0)
        market_snapshot = {symbol: {"prices": prices}}
        signals = self.legacy_strategy.generate(market_snapshot)
        return signals[0] if signals else Signal(symbol=symbol, action="hold", confidence=0.0, price=float(prices[-1]))

    def generate_strategy_signal(self, symbol: str, candles: pd.DataFrame) -> tuple[Signal, str, str]:
        strategy_name = self.settings.strategy_name.lower()
        if strategy_name == "candle_portfolio":
            # Delegate to CandlePortfolioEngineAdapter (VWAP Deviation + Bollinger Squeeze +
            # TrendFollowing + MeanReversion + RSIScalpingCandle, all with real OHLCV candles).
            # Candles here come from REST fetch_candles() so H≠L≠C — no synthetic data.
            market_data = {symbol: {"df": candles}}
            signals = self.candle_portfolio_adapter.generate(market_data)
            if signals:
                sig = signals[0]
                logger.debug(
                    "[%s] CandlePortfolio output: action=%s confidence=%.4f",
                    symbol, sig.action, float(sig.confidence),
                )
                return sig, "candle_portfolio", "candle_portfolio"
            current_price = float(candles["close"].iloc[-1]) if not candles.empty else 0.0
            return Signal(symbol=symbol, action="hold", confidence=0.0, price=current_price), "candle_portfolio", "candle_portfolio"
        if strategy_name == "portfolio":
            signal = self._generate_legacy_signal(symbol, candles)
            logger.debug(
                "[%s] Portfolio strategy output: action=%s confidence=%.4f",
                symbol,
                signal.action,
                float(signal.confidence),
            )
            return signal, "portfolio", "portfolio"
        if self.settings.enable_strategy_portfolio:
            signal, regime, strategy_name = self.strategy_manager.generate_signal(symbol=symbol, candles=candles)
            logger.debug(
                "[%s] Regime strategy output: regime=%s strategy=%s action=%s confidence=%.4f",
                symbol,
                regime,
                strategy_name,
                signal.action,
                float(signal.confidence),
            )
            return signal, regime, strategy_name
        signal = self._generate_legacy_signal(symbol, candles)
        logger.debug(
            "[%s] Legacy strategy output: strategy=%s action=%s confidence=%.4f",
            symbol,
            self.settings.strategy_name,
            signal.action,
            float(signal.confidence),
        )
        return signal, "legacy", self.settings.strategy_name

    def _save_signal(self, signal: Signal, strategy_name: str, regime: str, indicators: dict[str, float]) -> str:
        signal_id = f"{signal.symbol}-{uuid.uuid4().hex}"
        self.db.save_signal(
            signal_id=signal_id,
            strategy_name=strategy_name,
            regime=regime,
            symbol=signal.symbol,
            action=signal.action,
            confidence=float(signal.confidence),
            price=float(signal.price),
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            trailing_stop_pct=signal.trailing_stop_pct,
            metadata={"indicators": indicators},
        )
        logger.info(
            "Signal Generated: %s | %s | Action: %s | Confidence: %.4f | Price: %.2f | Strategy: %s",
            signal_id,
            signal.symbol,
            signal.action,
            float(signal.confidence),
            signal.price,
            strategy_name,
        )
        return signal_id

    def _log_no_trade_reason(self, symbol: str, reason: str, *, details: Optional[str] = None) -> None:
        if details:
            logger.info("No-trade for %s: reason=%s details=%s", symbol, reason, details)
        else:
            logger.info("No-trade for %s: reason=%s", symbol, reason)
        self._structured_log(
            "trade_rejected",
            symbol=symbol,
            reason=reason,
            details=details,
        )

    @staticmethod
    def _sanitize_log_value(value):
        if isinstance(value, dict):
            return {str(key): ProfessionalTradingBot._sanitize_log_value(val) for key, val in value.items()}
        if isinstance(value, (list, tuple)):
            return [ProfessionalTradingBot._sanitize_log_value(item) for item in value]
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        return value

    def _structured_log(self, event: str, **fields) -> None:
        payload = {"event": event}
        payload.update({key: self._sanitize_log_value(val) for key, val in fields.items()})
        logger.info(json.dumps(payload, separators=(",", ":"), sort_keys=True))

    def _log_execution_state(
        self,
        *,
        symbol: str,
        signal: Optional[Signal],
        reason: str,
        details: Optional[str] = None,
    ) -> None:
        action = signal.action if signal is not None else None
        confidence = float(signal.confidence) if signal is not None else None
        if signal is not None:
            logger.info(
                "Execution gate for %s: action=%s confidence=%.4f reason=%s details=%s",
                symbol,
                signal.action,
                float(signal.confidence),
                reason,
                details or "",
            )
        else:
            logger.info("Execution gate for %s: reason=%s details=%s", symbol, reason, details or "")
        self._structured_log(
            "execution_gate",
            symbol=symbol,
            action=action,
            confidence=confidence,
            reason=reason,
            details=details,
        )

    def _maybe_force_paper_test_entry(
        self,
        *,
        symbol: str,
        signal: Signal,
        regime: str,
        strategy_name: str,
    ) -> tuple[Signal, bool]:
        threshold = max(0.0, min(1.0, float(self.settings.paper_force_buy_confidence_threshold)))
        if self.settings.mode == "live" or threshold <= 0:
            return signal, False
        if signal.action.lower() != "hold":
            return signal, False
        if float(signal.confidence) <= threshold:
            return signal, False

        forced_signal = self._with_default_protection(
            Signal(
                symbol=signal.symbol,
                action="buy",
                confidence=float(signal.confidence),
                price=float(signal.price),
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                trailing_stop_pct=signal.trailing_stop_pct,
            )
        )
        details = (
            f"forced_buy_for_paper_test threshold={threshold:.4f} "
            f"regime={regime} strategy={strategy_name}"
        )
        logger.warning("[%s] Paper fallback forcing BUY for execution test: %s", symbol, details)
        self._structured_log(
            "paper_execution_fallback",
            symbol=symbol,
            original_action=signal.action,
            forced_action="buy",
            confidence=float(signal.confidence),
            threshold=threshold,
            regime=regime,
            strategy=strategy_name,
        )
        return forced_signal, True

    def _validate_risk(self, signal: Signal, indicators: dict[str, float]) -> tuple[bool, float]:
        self._last_no_trade_reason = None
        signal = self._with_default_protection(signal)
        if signal.action not in {"buy", "sell"}:
            self._last_no_trade_reason = "invalid_signal_action"
            return False, 0.0
        if signal.price <= 0:
            self._last_no_trade_reason = "invalid_signal_price"
            return False, 0.0
        if signal.stop_loss is None:
            self._last_no_trade_reason = "missing_stop_loss"
            return False, 0.0

        if self.safety.check_daily_loss_kill_switch(self.account_equity, self.start_of_day_equity):
            self._last_no_trade_reason = "daily_kill_switch_triggered"
            self._kill_switch_triggered = True
            return False, 0.0

        symbol_notional = self._open_notional_by_symbol.get(signal.symbol, 0.0)
        atr = indicators.get("atr14", float("nan"))
        if not math.isfinite(float(atr)):
            atr = signal.price * 0.003

        if self.settings.enable_advanced_risk:
            size = self.advanced_risk.dynamic_position_size(
                account_equity=self.account_equity,
                entry_price=signal.price,
                atr=float(atr),
                signal_confidence=signal.confidence,
                current_asset_notional=symbol_notional,
            )
            projected_notional = self._open_notional_total + (size * signal.price)
            leverage_after = projected_notional / self.account_equity if self.account_equity > 0 else float("inf")
            is_valid = self.advanced_risk.validate_trade(
                account_equity=self.account_equity,
                start_of_day_equity=self.start_of_day_equity,
                asset_notional_after_trade=symbol_notional + (size * signal.price),
                total_notional_after_trade=projected_notional,
                leverage_after_trade=leverage_after,
            )
        else:
            size = calculate_position_size(
                account_equity=self.account_equity,
                entry_price=signal.price,
                stop_loss_price=signal.stop_loss,
                current_open_notional=self._open_notional_total,
                max_risk_per_trade=self.settings.max_risk_per_trade,
                max_leverage=self.settings.max_leverage,
            )
            is_valid = validate_trade(
                account_equity=self.account_equity,
                start_of_day_equity=self.start_of_day_equity,
                entry_price=signal.price,
                stop_loss_price=signal.stop_loss,
                position_size=size,
                current_open_notional=self._open_notional_total,
                max_risk_per_trade=self.settings.max_risk_per_trade,
                max_leverage=self.settings.max_leverage,
                max_daily_loss=self.settings.max_daily_loss,
            )

        if size <= 0:
            self._last_no_trade_reason = "position_size_zero"
            return False, 0.0
        if not is_valid:
            self._last_no_trade_reason = "risk_validation_failed"
            return False, 0.0
        return True, float(size)

    def _parse_best_bid_ask(self, payload: dict, fallback_price: float) -> tuple[float, float]:
        best_bid = fallback_price * 0.999
        best_ask = fallback_price * 1.001
        result = payload.get("result", payload.get("data", payload)) if isinstance(payload, dict) else {}
        if isinstance(result, dict):
            bids = result.get("bids") or result.get("buy") or []
            asks = result.get("asks") or result.get("sell") or []
            if bids:
                top_bid = bids[0]
                if isinstance(top_bid, (list, tuple)) and top_bid:
                    best_bid = float(top_bid[0])
                elif isinstance(top_bid, dict):
                    best_bid = float(top_bid.get("price", best_bid))
            if asks:
                top_ask = asks[0]
                if isinstance(top_ask, (list, tuple)) and top_ask:
                    best_ask = float(top_ask[0])
                elif isinstance(top_ask, dict):
                    best_ask = float(top_ask.get("price", best_ask))
        if best_bid <= 0:
            best_bid = fallback_price * 0.999
        if best_ask <= 0:
            best_ask = fallback_price * 1.001
        return best_bid, best_ask

    def _fetch_best_bid_ask(self, symbol: str, fallback_price: float) -> tuple[float, float]:
        if self.settings.mode != "live" or self.client is None:
            return fallback_price * 0.999, fallback_price * 1.001
        start = time.perf_counter()
        try:
            payload = self.client.get_orderbook(symbol)
            self.safety.breaker.record_success()
            return self._parse_best_bid_ask(payload, fallback_price)
        except Exception:
            self.safety.breaker.record_failure()
            self.metrics.record_api_error("/v2/l2orderbook")
            return fallback_price * 0.999, fallback_price * 1.001
        finally:
            self.metrics.observe_api_latency("/v2/l2orderbook", time.perf_counter() - start)

    def _is_filled_order(self, order: Optional[dict]) -> bool:
        if not isinstance(order, dict):
            return False
        result = order.get("result")
        payload = result if isinstance(result, dict) else order
        status = str(payload.get("status") or payload.get("state") or payload.get("order_state") or "").lower()
        if status in {"filled", "closed", "complete", "executed"}:
            return True
        if status in {"open", "new", "pending", "submitted", "partially_filled"}:
            return False
        return order.get("success") is not False

    def _record_order_row(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        size: float,
        status: str,
        trade_id: str,
        price: Optional[float] = None,
        response: Optional[dict] = None,
        client_order_id: Optional[str] = None,
    ) -> None:
        self.db.save_order_record(
            symbol=symbol,
            side=side,
            order_type=order_type,
            size=size,
            price=price,
            status=status,
            trade_id=trade_id,
            order_id=self._extract_exchange_order_id(response),
            client_order_id=client_order_id,
            metadata={"raw_response": response or {}},
        )

    def _register_entry_position(
        self,
        *,
        symbol: str,
        side: str,
        size: float,
        signal: Signal,
        trade_id: str,
        strategy_name: str,
        entry_order_type: str = "market_order",
    ) -> None:
        position_side = "long" if side == "buy" else "short"
        self._open_positions[symbol] = {
            "trade_id": trade_id,
            "side": position_side,
            "size": size,
            "entry_price": signal.price,
            "entry_ts": time.time(),
            "stop_loss": signal.stop_loss,
            "take_profit": signal.take_profit,
            "trailing_stop_pct": signal.trailing_stop_pct,
            "strategy_name": strategy_name,
            "entry_order_type": entry_order_type,
            "realized_pnl_accum": 0.0,
            "source": "local_entry",
        }
        self.db.upsert_open_position_state(
            symbol=symbol,
            trade_id=trade_id,
            side=position_side,
            size=size,
            entry_price=signal.price,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            trailing_stop_pct=signal.trailing_stop_pct,
            mode=self.settings.mode,
        )
        self.db.upsert_trade_record(
            trade_id=trade_id,
            symbol=symbol,
            side=position_side,
            size=size,
            entry_price=signal.price,
            strategy_name=strategy_name,
            metadata={"entry_order_type": entry_order_type}
        )
        if signal.stop_loss is not None:
            self.execution_engine.place_stop_loss(
                symbol=symbol,
                position_side=position_side,
                size=size,
                stop_price=signal.stop_loss,
                trade_id=trade_id,
            )
        if signal.take_profit is not None:
            self.execution_engine.place_take_profit(
                symbol=symbol,
                position_side=position_side,
                size=size,
                target_price=signal.take_profit,
                trade_id=trade_id,
            )
        if signal.trailing_stop_pct is not None and signal.price > 0:
            self.execution_engine.set_trailing_stop(
                symbol=symbol,
                position_side=position_side,
                size=size,
                trail_pct=signal.trailing_stop_pct,
                entry_price=signal.price,
                trade_id=trade_id,
            )
        # Persist exchange order IDs so they survive a restart
        stop_oid, tp_oid = self.execution_engine.get_protection_order_ids(symbol)
        if stop_oid or tp_oid:
            self.db.upsert_open_position_state(
                symbol=symbol,
                trade_id=trade_id,
                side=position_side,
                size=size,
                entry_price=signal.price,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                trailing_stop_pct=signal.trailing_stop_pct,
                stop_order_id=stop_oid,
                tp_order_id=tp_oid,
                mode=self.settings.mode,
            )
        self._recalculate_open_notional()

    def _execute_live_entry(self, signal: Signal, size: float, trade_id: str) -> tuple[dict, bool, str]:
        side = signal.action.lower()
        client_order_prefix = self._safe_client_order_id(f"{trade_id}-entry")
        if self.settings.enable_smart_order_routing and hasattr(self.execution_engine, "execute_smart_order"):
            best_bid, best_ask = self._fetch_best_bid_ask(signal.symbol, signal.price)
            responses = self.execution_engine.execute_smart_order(
                symbol=signal.symbol,
                side=side,
                size=size,
                reference_price=signal.price,
                best_bid=best_bid,
                best_ask=best_ask,
                spread_threshold_pct=self.settings.spread_threshold_pct,
                max_slippage_pct=self.settings.max_slippage_pct,
                chunk_size=self.settings.order_chunk_size,
                max_retries_per_chunk=self.settings.max_retries_per_chunk,
                client_order_id_prefix=client_order_prefix,
            )
            final_resp = responses[-1] if responses else {"success": False}
            order_type = "market_or_limit_smart"
            for idx, response in enumerate(responses, start=1):
                self._record_order_row(
                    symbol=signal.symbol,
                    side=side,
                    order_type=order_type,
                    size=size / max(1, len(responses)),
                    status="filled" if self._is_filled_order(response) else "submitted",
                    trade_id=trade_id,
                    price=signal.price,
                    response=response,
                    client_order_id=self._safe_client_order_id(f"{client_order_prefix}-{idx}"),
                )
            return final_resp, self._is_filled_order(final_resp), order_type

        if signal.price > 0:
            order = self.execution_engine.execute_limit_order(
                symbol=signal.symbol,
                side=side,
                size=size,
                price=signal.price,
                client_order_id=client_order_prefix,
            )
            order_type = "limit_order"
        else:
            order = self.execution_engine.execute_market_order(
                symbol=signal.symbol,
                side=side,
                size=size,
                client_order_id=client_order_prefix,
            )
            order_type = "market_order"
        self._record_order_row(
            symbol=signal.symbol,
            side=side,
            order_type=order_type,
            size=size,
            status="filled" if self._is_filled_order(order) else "submitted",
            trade_id=trade_id,
            price=signal.price,
            response=order,
            client_order_id=client_order_prefix,
        )
        return order, self._is_filled_order(order), order_type

    def execute_order(self, signal: Signal, size: float, strategy_name: str, regime: str) -> Optional[dict]:
        signal = self._with_default_protection(signal)
        side = signal.action.lower()
        if side not in {"buy", "sell"}:
            self._log_execution_state(
                symbol=signal.symbol,
                signal=signal,
                reason="execute_order_skipped_invalid_action",
            )
            return None
        trade_id = self._new_trade_id(signal.symbol)
        entry_execution_id = f"{trade_id}:entry"
        entry_client_order_id = self._safe_client_order_id(f"{trade_id}-entry")
        requested_order_type = "market_or_limit_smart" if self.settings.enable_smart_order_routing else "limit_order"
        self._structured_log(
            "execution_started",
            symbol=signal.symbol,
            trade_id=trade_id,
            action=side,
            confidence=float(signal.confidence),
            size=float(size),
            strategy=strategy_name,
            regime=regime,
            mode=self.settings.mode,
        )

        if self.settings.mode != "live":
            self.db.save_trade(
                trade_id=trade_id,
                symbol=signal.symbol,
                side=side,
                size=size,
                price=signal.price,
                strategy_name=strategy_name,
                metadata={"entry_order_type": "paper_limit"},
            )
            self._record_order_row(
                symbol=signal.symbol,
                side=side,
                order_type="paper_limit",
                size=size,
                status="filled",
                trade_id=trade_id,
                price=signal.price,
                response={"paper": True},
                client_order_id=entry_client_order_id,
            )
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
                metadata={"strategy": strategy_name, "regime": regime, "signal_confidence": signal.confidence},
            )
            self._register_entry_position(
                symbol=signal.symbol,
                side=side,
                size=size,
                signal=signal,
                trade_id=trade_id,
                strategy_name=strategy_name,
                entry_order_type="paper_limit",
            )
            self._structured_log(
                "paper_execution_simulated",
                symbol=signal.symbol,
                trade_id=trade_id,
                action=side,
                confidence=float(signal.confidence),
                size=float(size),
                order_type="paper_limit",
                db_writes=["trades", "orders", "execution_logs", "positions"],
            )
            return {"paper": True, "trade_id": trade_id}

        if not self.safety.can_trade():
            self._last_no_trade_reason = "api_circuit_breaker_open"
            self._log_no_trade_reason(signal.symbol, "api_circuit_breaker_open")
            return None

        pre_ok, before_signed = self._validate_pre_execution(signal.symbol, side)
        if not pre_ok:
            return None

        self.db.save_trade(
            trade_id=trade_id,
            symbol=signal.symbol,
            side=side,
            size=size,
            price=signal.price,
            strategy_name=strategy_name,
            metadata={"entry_order_type": requested_order_type},
        )

        start = time.perf_counter()
        try:
            order, is_filled, actual_order_type = self._execute_live_entry(signal=signal, size=size, trade_id=trade_id)
            self.safety.breaker.record_success()
        except Exception:
            self.metrics.record_api_error("/v2/orders")
            self.metrics.record_order_failure()
            self.safety.breaker.record_failure()
            raise
        finally:
            self.metrics.observe_api_latency("/v2/orders", time.perf_counter() - start)

        status = "filled" if is_filled else "submitted"
        self.db.save_execution(
            trade_id=trade_id,
            execution_id=entry_execution_id,
            symbol=signal.symbol,
            side=side,
            size=size,
            price=signal.price,
            event_type="entry",
            order_type=actual_order_type if self.settings.enable_smart_order_routing else requested_order_type,
            mode=self.settings.mode,
            status=status,
            client_order_id=entry_client_order_id,
            exchange_order_id=self._extract_exchange_order_id(order),
            metadata={
                "strategy": strategy_name,
                "regime": regime,
                "signal_confidence": signal.confidence,
                "fees": {
                    "entry_fee_estimate": self.fee_manager.calculate_entry_fee(
                        signal.price,
                        size,
                        actual_order_type,
                    ),
                    "order_type": actual_order_type,
                },
            },
        )
        self.sync_position_with_exchange(
            signal.symbol,
            reason="post_entry_sync",
            preferred_trade_id=trade_id,
            preferred_strategy_name=strategy_name,
            protection_signal=signal,
        )
        current = self._local_cache_positions.get(signal.symbol)
        if current:
            current["entry_order_type"] = actual_order_type
            current["realized_pnl_accum"] = float(current.get("realized_pnl_accum", 0.0) or 0.0)
        if is_filled and current:
            self.db.upsert_open_position_state(
                symbol=signal.symbol,
                trade_id=current["trade_id"],
                side=current["side"],
                size=float(current["size"]),
                entry_price=float(current["entry_price"]),
                stop_loss=current.get("stop_loss"),
                take_profit=current.get("take_profit"),
                trailing_stop_pct=current.get("trailing_stop_pct"),
                mode=self.settings.mode,
            )
        if not self._validate_post_execution(
            symbol=signal.symbol,
            side=side,
            before_signed=before_signed,
            filled=is_filled,
        ):
            return None
        self._structured_log(
            "execution_completed",
            symbol=signal.symbol,
            trade_id=trade_id,
            action=side,
            filled=bool(is_filled),
            order_type=actual_order_type if self.settings.enable_smart_order_routing else requested_order_type,
            mode=self.settings.mode,
        )
        return order

    def _cancel_open_orders_for_symbol(self, symbol: str) -> None:
        self.cancel_open_orders(symbol)

    @staticmethod
    def _new_exit_execution_id(trade_id: str) -> str:
        return f"{trade_id}:exit:{uuid.uuid4().hex[:8]}"

    def _handle_exit(self, symbol: str, current_price: float, triggered: dict) -> None:
        open_pos = self._open_positions.get(symbol)
        if open_pos is None:
            return

        trade_id = str(triggered.get("trade_id") or open_pos.get("trade_id") or self._new_trade_id(symbol))
        execution_id = self._new_exit_execution_id(trade_id)
        requested_size = float(triggered.get("size", open_pos.get("size", 0.0)) or 0.0)
        pre_size = abs(float(open_pos.get("size", 0.0) or 0.0))
        entry_price = float(open_pos.get("entry_price", 0.0) or 0.0)
        side = str(open_pos.get("side", "")).lower()
        strategy_name = str(open_pos.get("strategy_name", "unknown"))
        realized_accum = float(open_pos.get("realized_pnl_accum", 0.0) or 0.0)
        entry_order_type = str(open_pos.get("entry_order_type", "market_order"))

        if self.settings.mode == "live":
            self.sync_position_with_exchange(symbol, reason="post_exit_execution")
            self.validate_position_consistency(symbol)

        remaining_size = abs(self._local_signed_size(symbol))
        if pre_size > self.settings.position_sync_tolerance:
            realized_size = max(0.0, pre_size - remaining_size)
        else:
            realized_size = max(0.0, requested_size)
        if realized_size <= self.settings.position_sync_tolerance:
            realized_size = max(0.0, requested_size)

        exit_side = str(triggered.get("exit_side", "sell"))
        exit_price = float(triggered.get("trigger_price", current_price))
        if side == "long":
            gross_pnl = (exit_price - entry_price) * realized_size
        elif side == "short":
            gross_pnl = (entry_price - exit_price) * realized_size
        else:
            gross_pnl = 0.0

        exit_order_type = "market_order"

        # Build trade dict for unified fee calculation (entry + exit + funding)
        trade_dict: dict = {
            "entry_price": entry_price,
            "exit_price": exit_price,
            "size": realized_size,
            "entry_order_type": entry_order_type,
            "exit_order_type": exit_order_type,
        }
        if self.settings.enable_funding_awareness and self.client is not None:
            entry_ts = open_pos.get("entry_ts")
            holding_s = (time.time() - entry_ts) if entry_ts else 0.0
            if holding_s > 0:
                fetched_rate = self.client.get_funding_rate(symbol)
                if fetched_rate is not None:
                    trade_dict["funding_rate"] = fetched_rate
                    trade_dict["holding_seconds"] = holding_s

        total_fee = self.fee_manager.calculate_total_fee(trade_dict)
        funding_cost = 0.0
        if "funding_rate" in trade_dict:
            notional = entry_price * realized_size
            funding_cost = self.fee_manager.calculate_funding_cost(
                notional, trade_dict["funding_rate"], trade_dict["holding_seconds"]
            )
            if funding_cost > 0:
                logger.info(
                    "Funding cost symbol=%s funding_rate=%.6f holding_s=%.0f funding_cost=%.4f",
                    symbol, trade_dict["funding_rate"], trade_dict["holding_seconds"], funding_cost,
                )

        real_pnl = gross_pnl - total_fee
        logger.info(
            "Fee calculation symbol=%s trade_id=%s total_fee=%s "
            "funding_cost=%s gross_pnl=%s real_pnl=%s",
            symbol,
            trade_id,
            total_fee,
            funding_cost,
            gross_pnl,
            real_pnl,
        )

        # Register realized PnL with the advanced risk manager first so the
        # daily kill-switch denominator (start_of_day_equity) is always correct.
        self.advanced_risk.register_realized_pnl(real_pnl)
        if self.settings.mode == "live":
            # In live mode, fetch the authoritative balance from the exchange
            # immediately instead of applying an internal delta.  The naive
            # "+= real_pnl" approach makes account_equity temporarily negative
            # (e.g. $106 account - $425 loss = -$318) which fires the daily
            # kill switch prematurely, before the next cycle's refresh can
            # correct it.
            self._refresh_live_equity()
        else:
            # Paper/backtest: no exchange to query, simulate the PnL delta.
            self.account_equity += real_pnl
        self._update_metrics_from_equity()

        is_flat = remaining_size <= self.settings.position_sync_tolerance
        if not is_flat:
            local = self._local_cache_positions.get(symbol)
            if local is not None:
                local["realized_pnl_accum"] = realized_accum + real_pnl
                self.db.upsert_open_position_state(
                    symbol=symbol,
                    trade_id=local["trade_id"],
                    side=local["side"],
                    size=float(local["size"]),
                    entry_price=float(local["entry_price"]),
                    stop_loss=local.get("stop_loss"),
                    take_profit=local.get("take_profit"),
                    trailing_stop_pct=local.get("trailing_stop_pct"),
                    mode=self.settings.mode,
                )
        else:
            self._update_trade_stats(pnl=realized_accum + real_pnl, strategy_name=strategy_name)
            self.db.close_trade_record(trade_id=trade_id, exit_price=exit_price, net_pnl=realized_accum + real_pnl)

        self.db.save_execution(
            trade_id=trade_id,
            execution_id=execution_id,
            symbol=symbol,
            side=exit_side,
            size=realized_size,
            price=exit_price,
            event_type="exit",
            order_type=exit_order_type,
            mode=self.settings.mode,
            status="filled" if is_flat else "partial_filled",
            reason=str(triggered.get("reason", "protection")),
            client_order_id=triggered.get("client_order_id"),
            exchange_order_id=triggered.get("exchange_order_id"),
            metadata={
                "trigger_payload": triggered,
                "strategy": strategy_name,
                "gross_pnl": gross_pnl,
                "real_pnl": real_pnl,
                "fees": {
                    "entry_fee": self.fee_manager.calculate_entry_fee(entry_price, realized_size, entry_order_type),
                    "exit_fee": self.fee_manager.calculate_exit_fee(exit_price, realized_size, exit_order_type),
                    "total_fee": total_fee,
                },
                "remaining_size": remaining_size,
            },
        )
        self._record_order_row(
            symbol=symbol,
            side=exit_side,
            order_type=exit_order_type,
            size=realized_size,
            status="filled" if is_flat else "partial_filled",
            trade_id=trade_id,
            price=exit_price,
            response=triggered.get("order"),
            client_order_id=triggered.get("client_order_id"),
        )
        self._recalculate_open_notional()

        if self.settings.mode == "live" and is_flat and self.safety.should_auto_cancel_orders_if_flat(0.0):
            self.cancel_open_orders(symbol)
        self._save_performance_metrics()

    def _update_trade_stats(self, pnl: float, strategy_name: str) -> None:
        if pnl > 0:
            self._wins += 1
            self._gross_profit += pnl
        elif pnl < 0:
            self._losses += 1
            self._gross_loss += abs(pnl)
        self._strategy_perf[strategy_name]["trades"] += 1
        if pnl > 0:
            self._strategy_perf[strategy_name]["wins"] += 1
        self._strategy_perf[strategy_name]["pnl"] += pnl
        self.metrics.record_trade(pnl)

    def _update_metrics_from_equity(self) -> None:
        self._peak_equity = max(self._peak_equity, self.account_equity)
        drawdown = 0.0
        if self._peak_equity > 0:
            drawdown = max(0.0, ((self._peak_equity - self.account_equity) / self._peak_equity) * 100.0)
        self.metrics.set_drawdown(drawdown)
        self.metrics.set_total_pnl(self.account_equity - self.start_of_day_equity)

    def _save_performance_metrics(self) -> None:
        total_trades = self._wins + self._losses
        win_rate = (self._wins / total_trades * 100.0) if total_trades > 0 else 0.0
        profit_factor = self._gross_profit / self._gross_loss if self._gross_loss > 0 else (999.0 if self._gross_profit > 0 else 0.0)
        drawdown_pct = self.metrics.drawdown._value.get() if hasattr(self.metrics.drawdown, "_value") else 0.0
        self.db.save_performance_metrics(
            mode=self.settings.mode,
            total_trades=total_trades,
            win_rate=win_rate,
            profit_factor=profit_factor,
            max_drawdown=float(drawdown_pct),
            realized_pnl=self.account_equity - self.start_of_day_equity,
            unrealized_pnl=0.0,
            metadata={"strategy_performance": self._strategy_perf},
        )

    def _position_mismatch_detected(self, symbol: str) -> bool:
        symbol_u = self._normalize_symbol(symbol)
        if not self.sync_position_with_exchange(symbol_u, reason="consistency_check"):
            self.halt_trading(f"position_sync_failed:{symbol_u}")
            return True
        return not self.validate_position_consistency(symbol_u)

    async def process_symbol(self, symbol: str) -> None:
        if self._kill_switch_triggered:
            self._log_execution_state(symbol=symbol, signal=None, reason="kill_switch_triggered")
            return
        if self._stop_requested or self._shutdown_requested_via_file():
            self._stop_requested = True
            self._log_execution_state(symbol=symbol, signal=None, reason="stop_requested")
            return
        if self._trading_paused:
            self._last_no_trade_reason = self._pause_reason or "trading_paused"
            self._log_no_trade_reason(symbol, self._last_no_trade_reason)
            return
        if not self.safety.can_trade():
            self._last_no_trade_reason = "api_circuit_breaker_open"
            self._log_no_trade_reason(symbol, "api_circuit_breaker_open")
            return

        if self.settings.mode == "live":
            # FIX: Use _sync_position_if_due instead of a bare sync call.
            # If reconciliation ran within the last 10 s, the per-symbol call
            # is skipped — this prevents testnet propagation lag from causing
            # a freshly-reconciled position to be immediately flattened by a
            # stale per-symbol snapshot, which was the root cause of the
            # repeated re-ordering loop and daily kill-switch triggering.
            if not self._sync_position_if_due(symbol, reason="pre_symbol_cycle", min_interval_s=10.0):
                self.halt_trading(f"pre_cycle_sync_failed:{symbol}")
                self._log_no_trade_reason(symbol, "pre_cycle_sync_failed")
                return
            if not self.validate_position_consistency(symbol):
                self._last_no_trade_reason = "position_mismatch"
                self._log_no_trade_reason(symbol, "position_mismatch")
                return

        try:
            logger.info(f"[{symbol}] Fetching candles...")
            candles = await asyncio.to_thread(self.fetch_market_data, symbol)
            logger.info(f"[{symbol}] Successfully fetched {len(candles)} candles.")
        except Exception as exc:
            logger.warning(f"Market data fetch failed for {symbol}: {exc}")
            self._log_no_trade_reason(symbol, "market_data_fetch_failed", details=str(exc))
            return
        
        indicators = self.calculate_indicators(candles)
        if not indicators:
            logger.warning(f"[{symbol}] Could not calculate indicators (missing data).")
            self._last_no_trade_reason = "market_data_unavailable"
            self._log_no_trade_reason(symbol, "market_data_unavailable")
            return
        
        price = indicators.get("price", 0.0)
        logger.info(
            "[%s] Indicators calculated: price=%.2f ema20=%s rsi=%s macd=%s macd_signal=%s vwap=%s atr14=%s",
            symbol,
            price,
            f"{indicators['ema20']:.4f}" if pd.notna(indicators.get("ema20")) else "nan",
            f"{indicators['rsi']:.2f}" if pd.notna(indicators.get("rsi")) else "nan",
            f"{indicators['macd']:.6f}" if pd.notna(indicators.get("macd")) else "nan",
            f"{indicators['macd_signal']:.6f}" if pd.notna(indicators.get("macd_signal")) else "nan",
            f"{indicators['vwap']:.4f}" if pd.notna(indicators.get("vwap")) else "nan",
            f"{indicators['atr14']:.4f}" if pd.notna(indicators.get("atr14")) else "nan",
        )
        self._structured_log(
            "indicator_snapshot",
            symbol=symbol,
            price=price,
            ema20=indicators.get("ema20"),
            rsi=indicators.get("rsi"),
            vwap=indicators.get("vwap"),
            atr14=indicators.get("atr14"),
            macd=indicators.get("macd"),
            macd_signal=indicators.get("macd_signal"),
            macd_histogram=indicators.get("macd_histogram"),
        )

        self._on_realtime_price(symbol, price)
        if symbol in self._open_positions:
            # Always check SL/TP/trailing stop even when not entering a new trade.
            # This is the primary exit path for paper mode where WS may be unreliable.
            current_price = indicators.get("price", 0.0)
            if current_price > 0:
                triggered = self.execution_engine.on_price_update(symbol, current_price)
                if triggered:
                    logger.info(
                        "[%s] Protection triggered at price=%.4f reason=%s",
                        symbol, current_price, triggered.get("reason", "unknown")
                    )
                    self._handle_exit(
                        symbol=symbol,
                        current_price=current_price,
                        triggered=triggered,
                    )

            # Force-close positions held longer than max_holding_time_s
            open_pos = self._open_positions.get(symbol)
            if open_pos:
                entry_ts = open_pos.get("entry_ts")
                if entry_ts is None:
                    # Backfill entry_ts if missing (positions loaded from DB won't have it)
                    open_pos["entry_ts"] = time.time()
                elif (time.time() - entry_ts) > self.settings.max_holding_time_s:
                    logger.warning(
                        "[%s] Max holding time exceeded (%.0fs). Force-closing position.",
                        symbol, time.time() - entry_ts
                    )
                    current_price = indicators.get("price", 0.0)
                    forced_exit = {
                        "reason": "max_holding_time",
                        "exit_side": "buy" if open_pos.get("side") == "short" else "sell",
                        "size": open_pos.get("size", 0.0),
                        "trigger_price": current_price,
                        "trade_id": open_pos.get("trade_id"),
                    }
                    self._handle_exit(
                        symbol=symbol,
                        current_price=current_price,
                        triggered=forced_exit,
                    )
            self._last_no_trade_reason = "existing_open_position"
            self._log_no_trade_reason(
                symbol,
                "existing_open_position",
                details=f"trade_id={self._open_positions.get(symbol, {}).get('trade_id')}",
            )
            return

        signal, regime, strategy_name = self.generate_strategy_signal(symbol, candles)
        signal = self._with_default_protection(signal)
        self._log_execution_state(
            symbol=symbol,
            signal=signal,
            reason="signal_generated",
            details=f"regime={regime} strategy={strategy_name}",
        )
        self._save_signal(signal=signal, strategy_name=strategy_name, regime=regime, indicators=indicators)
        logger.info(
            "[%s] Strategy decision: regime=%s strategy=%s action=%s confidence=%.4f",
            symbol,
            regime,
            strategy_name,
            signal.action,
            float(signal.confidence),
        )
        self._structured_log(
            "strategy_decision",
            symbol=symbol,
            regime=regime,
            strategy=strategy_name,
            action=signal.action,
            calculated_score=float(signal.confidence),
            final_confidence=float(signal.confidence),
            price=signal.price,
            indicators={
                "rsi": indicators.get("rsi"),
                "ema20": indicators.get("ema20"),
                "macd": indicators.get("macd"),
                "macd_signal": indicators.get("macd_signal"),
                "macd_histogram": indicators.get("macd_histogram"),
            },
        )
        
        signal, forced_paper_fallback = self._maybe_force_paper_test_entry(
            symbol=symbol,
            signal=signal,
            regime=regime,
            strategy_name=strategy_name,
        )

        if signal.action.lower() == "hold":
            details = (
                f"regime={regime} strategy={strategy_name} confidence={float(signal.confidence):.4f} "
                f"rsi={indicators.get('rsi')} ema20={indicators.get('ema20')} macd={indicators.get('macd')} "
                f"price={indicators.get('price')}"
            )
            logger.info(f"[{symbol}] Signal: HOLD (Confidence: {signal.confidence:.2f})")
            self._last_no_trade_reason = "strategy_signal_hold"
            self._log_no_trade_reason(symbol, "strategy_signal_hold", details=details)
            return
        
        logger.info(f"[{symbol}] DETECTED SIGNAL: {signal.action.upper()} (Confidence: {signal.confidence:.2f})")
        self._structured_log(
            "trade_candidate",
            symbol=symbol,
            regime=regime,
            strategy=strategy_name,
            action=signal.action,
            calculated_score=float(signal.confidence),
            final_confidence=float(signal.confidence),
            decision_reason="paper_execution_fallback" if forced_paper_fallback else "actionable_signal_generated",
        )

        min_confidence = max(0.0, min(1.0, float(self.settings.min_signal_confidence)))
        if float(signal.confidence) < min_confidence:
            self._log_execution_state(
                symbol=symbol,
                signal=signal,
                reason="signal_confidence_below_threshold",
                details=(
                    f"signal_confidence={float(signal.confidence):.4f} "
                    f"threshold={min_confidence:.4f} regime={regime} strategy={strategy_name}"
                ),
            )
            self._last_no_trade_reason = "signal_confidence_below_threshold"
            self._log_no_trade_reason(symbol, "signal_confidence_below_threshold")
            return

        if self.settings.mode == "live" and self._position_mismatch_detected(symbol):
            self._last_no_trade_reason = "position_mismatch"
            self._log_no_trade_reason(symbol, "position_mismatch")
            return

        ok, size = self._validate_risk(signal, indicators)
        if not ok:
            reason = self._last_no_trade_reason or "unknown_risk_rejection"
            self._log_no_trade_reason(symbol, "risk_validation_failed", details=reason)
            return

        self._structured_log(
            "execution_requested",
            symbol=symbol,
            action=signal.action,
            confidence=float(signal.confidence),
            size=float(size),
            strategy=strategy_name,
            regime=regime,
            mode=self.settings.mode,
        )
        try:
            await asyncio.to_thread(self.execute_order, signal, size, strategy_name, regime)
        except DeltaAPIError as exc:
            logger.warning("Order execution failed for %s: %s", symbol, exc)
        except Exception:
            logger.exception("Unexpected order execution failure for %s", symbol)

    async def _risk_monitor(self) -> None:
        _kill_switch_date: Optional[str] = None
        while not self._stop_requested:
            today = datetime.now().strftime("%Y-%m-%d")

            # Reset kill switch at the start of a new calendar day
            if self._kill_switch_triggered and _kill_switch_date and _kill_switch_date != today:
                logger.warning(
                    "Daily kill switch reset for new trading day: previous_date=%s new_date=%s",
                    _kill_switch_date, today,
                )
                self._kill_switch_triggered = False
                self.advanced_risk.reset_daily_pnl()
                _kill_switch_date = None

            if self.safety.check_daily_loss_kill_switch(self.account_equity, self.start_of_day_equity):
                if not self._kill_switch_triggered:
                    logger.critical(
                        "Daily kill switch ACTIVATED — no new trades until %s. Bot continues running.",
                        today,
                    )
                self._kill_switch_triggered = True
                _kill_switch_date = today
            if self.advanced_risk.daily_kill_switch_triggered(self.start_of_day_equity):
                if not self._kill_switch_triggered:
                    logger.critical(
                        "Advanced risk kill switch ACTIVATED — no new trades until %s. Bot continues running.",
                        today,
                    )
                self._kill_switch_triggered = True
                _kill_switch_date = today
            await asyncio.sleep(3)

    async def run_async(self, max_cycles: Optional[int] = None, sleep_interval_s: Optional[int] = None) -> None:
        # sleep_interval_s CLI override takes priority; fall back to settings
        effective_interval = int(sleep_interval_s) if sleep_interval_s is not None else self.settings.trade_frequency_s
        if not self.settings.disable_metrics_server:
            self.metrics.start_server(port=self.settings.metrics_port, addr=self.settings.metrics_addr)
        if self.market_data_service is not None:
            self.market_data_service.start()

        monitor_task = asyncio.create_task(self._risk_monitor())
        cycle = 0
        try:
            while (
                (max_cycles is None or cycle < max_cycles)
                and not self._stop_requested
            ):
                if self._shutdown_requested_via_file():
                    self._stop_requested = True
                    logger.warning("Detected external shutdown signal file. Stopping bot loop gracefully.")
                    break
                started = time.perf_counter()
                logger.info(f"Cycle {cycle + 1} started: analyzing {len(self.settings.trade_symbols)} symbols...")
                if self.settings.mode == "live":
                    await asyncio.to_thread(self._refresh_live_equity)
                    if not await asyncio.to_thread(
                        self.reconcile_positions_with_exchange,
                        reason=f"cycle_{cycle + 1}",
                    ):
                        self.halt_trading(f"cycle_position_reconciliation_failed:{cycle + 1}")
                        break
                tasks = [self.process_symbol(symbol) for symbol in self.settings.trade_symbols]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for symbol, result in zip(self.settings.trade_symbols, results):
                    if isinstance(result, Exception):
                        logger.error(f"process_symbol failed for {symbol}: {result}")
                self._save_performance_metrics()
                cycle += 1
                elapsed = time.perf_counter() - started
                delay = max(0.0, float(effective_interval) - elapsed)
                if delay > 0:
                    await asyncio.sleep(delay)
        finally:
            monitor_task.cancel()
            if self.market_data_service is not None:
                self.market_data_service.stop()
            self._clear_shutdown_signal()

    def run(self, max_cycles: Optional[int] = None, sleep_interval_s: Optional[int] = None) -> None:
        asyncio.run(self.run_async(max_cycles=max_cycles, sleep_interval_s=sleep_interval_s))


def main() -> None:
    parser = argparse.ArgumentParser(description="Professional Delta Exchange India trading bot")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument(
        "--strategy",
        choices=["candle_portfolio", "portfolio", "momentum", "rsi_scalping", "ema_crossover", "trend_following", "mean_reversion"],
        default="portfolio",
    )
    parser.add_argument("--cycles", type=int, default=None, help="Optional number of loop cycles")
    parser.add_argument("--sleep-interval", type=int, default=None, help="Loop sleep interval in seconds (default: trade_frequency_s from settings)")
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbols to trade (example: BTCUSD,ETHUSD)",
    )
    parser.add_argument("--metrics-port", type=int, default=8000)
    parser.add_argument("--metrics-addr", default="0.0.0.0")
    parser.add_argument("--disable-metrics-server", action="store_true")
    parser.add_argument(
        "--log-dir",
        default=None,
        help="Directory to write log files (default: 'logs/'). Pass '' to disable file logging.",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Minimum log level (default: INFO from settings).",
    )
    args = parser.parse_args()

    kwargs = {
        "mode": args.mode,
        "metrics_port": args.metrics_port,
        "metrics_addr": args.metrics_addr,
        "disable_metrics_server": args.disable_metrics_server,
    }
    kwargs["strategy_name"] = args.strategy
    if args.symbols:
        kwargs["trade_symbols"] = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if args.log_dir is not None:
        kwargs["log_dir"] = args.log_dir
    if args.log_level is not None:
        kwargs["log_level"] = args.log_level
    settings = Settings(**kwargs)

    # Wire file logging — creates logs/bot_YYYYMMDD_HHMMSS.log automatically
    log_dir_value = settings.log_dir if settings.log_dir else None
    session_log = configure_logging(
        level=settings.log_level,
        structured=True,
        log_dir=log_dir_value,
    )
    if session_log:
        logger.info("Session log: %s", session_log)

    bot = ProfessionalTradingBot(settings=settings)
    bot.run(max_cycles=args.cycles, sleep_interval_s=args.sleep_interval)


if __name__ == "__main__":
    main()
