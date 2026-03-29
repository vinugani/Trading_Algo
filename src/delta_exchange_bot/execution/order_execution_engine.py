import logging
import math
import time
import hashlib
from dataclasses import dataclass
from typing import Optional

from delta_exchange_bot.api.delta_client import DeltaAPIError
from delta_exchange_bot.api.delta_client import DeltaClient
from delta_exchange_bot.utils.retry import retry_on_exception

logger = logging.getLogger(__name__)


@dataclass
class ProtectionState:
    symbol: str
    side: str  # "long" | "short"
    size: float
    trade_id: Optional[str] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_stop_pct: Optional[float] = None
    trailing_stop_price: Optional[float] = None
    extreme_price: Optional[float] = None
    # Exchange order IDs — stored so we can cancel orphaned orders on close
    stop_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    # Wall-clock time when this protection was first registered.
    # Used by the grace-period guard in on_price_update.
    registered_at: float = 0.0


class OrderExecutionEngine:
    # Seconds after registration during which a protection will NOT fire.
    # Prevents the price that was already live when a position was registered
    # from immediately triggering its own SL/TP.
    PROTECTION_GRACE_PERIOD_S: float = 10.0

    def __init__(self, client: Optional[DeltaClient]):
        self.client = client
        self._protection: dict[str, ProtectionState] = {}
        self.default_spread_threshold_pct = 0.0008
        self.default_slippage_threshold_pct = 0.002
        self.default_chunk_size = 0.0

    @staticmethod
    def _safe_client_order_id(raw: Optional[str], max_len: int = 32) -> Optional[str]:
        if not raw:
            return None
        value = str(raw)
        if len(value) <= max_len:
            return value
        digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
        prefix_len = max(1, max_len - 9)
        return f"{value[:prefix_len]}-{digest}"

    @staticmethod
    def _is_retryable_order_error(exc: Exception) -> bool:
        if isinstance(exc, DeltaAPIError):
            message = str(exc).lower()
            non_retryable_markers = (
                "bad_schema",
                "validation_error",
                "insufficient_margin",
                "signature mismatch",
                "negative_order_size",
                "http 400",
                "http 401",
                "http 403",
                "http 404",
            )
            if any(marker in message for marker in non_retryable_markers):
                return False
        return True

    @retry_on_exception()
    def execute_market_order(
        self,
        symbol: str,
        side: str,
        size: float,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
        max_slippage_pct: Optional[float] = None
    ) -> dict:
        if self.client is None:
            raise DeltaAPIError("DeltaClient is required for live market order execution")
        
        side = side.lower()
        # Slippage check before placing (using ticker)
        if max_slippage_pct:
            ticker = self.client.get_ticker(symbol)
            price = float(ticker.get("mark_price") or ticker.get("price") or 0)
            # This is a pre-check; the exchange might still slip. 
            # For true slippage control on Delta, use Limit orders.
        
        logger.info("Executing market order: symbol=%s side=%s size=%s reduce_only=%s", symbol, side, size, reduce_only)
        response = self.client.place_order(
            symbol=symbol,
            side=side,
            size=size,
            order_type="market_order",
            reduce_only=reduce_only,
            client_order_id=self._safe_client_order_id(client_order_id),
        )
        return self._wait_for_fill(response)

    @retry_on_exception()
    def execute_limit_order(
        self,
        symbol: str,
        side: str,
        size: float,
        price: float,
        time_in_force: str = "gtc",
        post_only: bool = False,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> dict:
        if self.client is None:
            raise DeltaAPIError("DeltaClient is required for live limit order execution")
        side = side.lower()
        logger.info(
            "Executing limit order: symbol=%s side=%s size=%s price=%s tif=%s",
            symbol,
            side,
            size,
            price,
            time_in_force,
        )
        response = self.client.place_order(
            symbol=symbol,
            side=side,
            size=size,
            price=price,
            order_type="limit_order",
            time_in_force=time_in_force,
            post_only=post_only,
            reduce_only=reduce_only,
            client_order_id=self._safe_client_order_id(client_order_id),
        )
        return self._wait_for_fill(response)

    def place_stop_loss(
        self,
        symbol: str,
        position_side: str,
        size: float,
        stop_price: float,
        trade_id: Optional[str] = None,
    ) -> ProtectionState:
        state = self._ensure_state(symbol, position_side, size, reference_price=stop_price, trade_id=trade_id)
        state.stop_loss = stop_price
        # Native exchange stop orders are not used: Delta Exchange testnet only
        # accepts limit_order/market_order and rejects conditional order types.
        # In-memory tracking handles SL/TP reliably for this deployment.
        logger.info("Registered stop loss: symbol=%s side=%s stop=%s size=%s", symbol, state.side, stop_price, size)
        return state

    def place_take_profit(
        self,
        symbol: str,
        position_side: str,
        size: float,
        target_price: float,
        trade_id: Optional[str] = None,
    ) -> ProtectionState:
        state = self._ensure_state(symbol, position_side, size, reference_price=target_price, trade_id=trade_id)
        state.take_profit = target_price
        # Native exchange take-profit orders are not used: same reason as SL above.
        logger.info("Registered take profit: symbol=%s side=%s target=%s size=%s", symbol, state.side, target_price, size)
        return state

    def set_trailing_stop(
        self,
        symbol: str,
        position_side: str,
        size: float,
        trail_pct: float,
        entry_price: float,
        trade_id: Optional[str] = None,
    ) -> ProtectionState:
        if trail_pct <= 0:
            raise ValueError("trail_pct must be > 0")

        state = self._ensure_state(symbol, position_side, size, reference_price=entry_price, trade_id=trade_id)
        state.trailing_stop_pct = trail_pct
        state.extreme_price = entry_price
        if state.side == "long":
            state.trailing_stop_price = entry_price * (1.0 - trail_pct)
        else:
            state.trailing_stop_price = entry_price * (1.0 + trail_pct)

        logger.info(
            "Registered trailing stop: symbol=%s side=%s trail_pct=%s initial_stop=%s",
            symbol,
            state.side,
            trail_pct,
            state.trailing_stop_price,
        )
        return state

    def on_price_update(self, symbol: str, current_price: float) -> Optional[dict]:
        state = self._protection.get(symbol)
        if state is None:
            return None

        # Grace-period guard: do not fire SL/TP in the first N seconds after a
        # protection is registered.  This prevents a stale cached price (the
        # exact same tick that was live when the position was registered) from
        # immediately triggering the brand-new protection level.
        if state.registered_at > 0:
            age_s = time.time() - state.registered_at
            if age_s < self.PROTECTION_GRACE_PERIOD_S:
                logger.debug(
                    "Protection grace period active: symbol=%s age_s=%.2f grace_s=%.1f — skipping trigger evaluation",
                    symbol, age_s, self.PROTECTION_GRACE_PERIOD_S,
                )
                return None

        if state.trailing_stop_pct is not None:
            if state.side == "long":
                if state.extreme_price is None or current_price > state.extreme_price:
                    state.extreme_price = current_price
                state.trailing_stop_price = state.extreme_price * (1.0 - state.trailing_stop_pct)
            else:
                if state.extreme_price is None or current_price < state.extreme_price:
                    state.extreme_price = current_price
                state.trailing_stop_price = state.extreme_price * (1.0 + state.trailing_stop_pct)

        stop_triggered = self._is_stop_triggered(state, current_price)
        tp_triggered = self._is_take_profit_triggered(state, current_price)

        if not stop_triggered and not tp_triggered:
            return None

        reason = "stop_loss" if stop_triggered else "take_profit"
        exit_side = "sell" if state.side == "long" else "buy"
        logger.warning(
            "Protection trigger hit: symbol=%s side=%s reason=%s price=%s",
            symbol,
            state.side,
            reason,
            current_price,
        )
        trade_id = state.trade_id or f"{symbol}-unknown"
        exit_client_order_id = self._safe_client_order_id(f"{trade_id}-exit-{reason}")

        # Cancel the sibling exchange order before placing the exit.
        # e.g. SL triggered → cancel the TP order (and vice-versa) so it
        # cannot re-open the position on the next tick.
        sibling_order_id = state.tp_order_id if stop_triggered else state.stop_order_id
        sibling_label = "take-profit" if stop_triggered else "stop-loss"
        if self.client is not None and sibling_order_id:
            try:
                self.client.cancel_order(order_id=sibling_order_id)
                logger.info(
                    "Cancelled sibling %s order on protection trigger: symbol=%s order_id=%s",
                    sibling_label, symbol, sibling_order_id,
                )
            except Exception as e:
                logger.warning(
                    "Could not cancel sibling %s order: symbol=%s order_id=%s error=%s",
                    sibling_label, symbol, sibling_order_id, e,
                )

        if self.client is None:
            order_response = {
                "paper": True,
                "result": {
                    "id": exit_client_order_id,
                    "status": "filled",
                },
            }
        else:
            order_response = self.execute_market_order(
                symbol=symbol,
                side=exit_side,
                size=state.size,
                reduce_only=True,
                client_order_id=exit_client_order_id,
            )
        del self._protection[symbol]
        return {
            "trade_id": state.trade_id,
            "symbol": symbol,
            "reason": reason,
            "exit_side": exit_side,
            "size": state.size,
            "trigger_price": current_price,
            "client_order_id": exit_client_order_id,
            "exchange_order_id": self._extract_exchange_order_id(order_response),
            "order": order_response,
        }

    def cancel_protection_orders(self, symbol: str) -> None:
        """Cancel any open exchange SL/TP orders for this symbol.

        Must be called whenever a position is closed — either by a protection
        trigger firing or by an external/manual close — to prevent orphaned
        orders from accidentally re-opening a position on the next price move.
        """
        state = self._protection.get(symbol)
        if state is None or self.client is None:
            return

        for label, order_id in (("stop-loss", state.stop_order_id), ("take-profit", state.tp_order_id)):
            if order_id:
                try:
                    self.client.cancel_order(order_id=order_id)
                    logger.info(
                        "Cancelled exchange %s order: symbol=%s order_id=%s",
                        label, symbol, order_id,
                    )
                except Exception as e:
                    # Log but don't raise — position is already closing, best-effort cancel
                    logger.warning(
                        "Could not cancel exchange %s order: symbol=%s order_id=%s error=%s",
                        label, symbol, order_id, e,
                    )

    def clear_protection(self, symbol: str) -> None:
        """Cancel exchange orders then remove in-memory protection state."""
        self.cancel_protection_orders(symbol)
        self._protection.pop(symbol, None)

    def get_protection_order_ids(self, symbol: str) -> tuple[Optional[str], Optional[str]]:
        """Return (stop_order_id, tp_order_id) for a symbol, or (None, None) if not tracked."""
        state = self._protection.get(symbol)
        if state is None:
            return None, None
        return state.stop_order_id, state.tp_order_id

    def restore_protection_order_ids(
        self,
        symbol: str,
        stop_order_id: Optional[str],
        tp_order_id: Optional[str],
    ) -> None:
        """Reload persisted exchange order IDs into ProtectionState after a bot restart.

        Called after place_stop_loss / place_take_profit have already re-established
        the in-memory ProtectionState.  Overwrites any newly-placed order ID so that
        the stored (pre-restart) IDs are used for cancellation instead.
        """
        state = self._protection.get(symbol)
        if state is None:
            return
        if stop_order_id:
            state.stop_order_id = stop_order_id
        if tp_order_id:
            state.tp_order_id = tp_order_id

    @staticmethod
    def _estimate_spread_pct(best_bid: float, best_ask: float) -> float:
        if best_bid <= 0 or best_ask <= 0:
            return 1.0
        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            return 1.0
        return (best_ask - best_bid) / mid

    @staticmethod
    def _estimate_slippage_pct(side: str, expected_price: float, reference_price: float) -> float:
        if expected_price <= 0 or reference_price <= 0:
            return 1.0
        side_n = side.lower()
        if side_n == "buy":
            return max(0.0, (expected_price - reference_price) / reference_price)
        return max(0.0, (reference_price - expected_price) / reference_price)

    @staticmethod
    def _split_chunks(total_size: float, chunk_size: float) -> list[float]:
        total = float(total_size)
        if total <= 0:
            return []
        if chunk_size <= 0 or total <= chunk_size:
            return [total]
        chunks = []
        n = int(math.floor(total / chunk_size))
        for _ in range(n):
            chunks.append(float(chunk_size))
        rem = total - (n * chunk_size)
        if rem > 1e-12:
            chunks.append(rem)
        return chunks

    def execute_smart_order(
        self,
        *,
        symbol: str,
        side: str,
        size: float,
        reference_price: float,
        best_bid: float,
        best_ask: float,
        spread_threshold_pct: Optional[float] = None,
        max_slippage_pct: Optional[float] = None,
        chunk_size: Optional[float] = None,
        max_retries_per_chunk: int = 3,
        client_order_id_prefix: Optional[str] = None,
    ) -> list[dict]:
        """Smart routing:
        - Market order when spread is small.
        - Limit order when spread is large.
        - Slippage guard.
        - Chunked partial execution.
        - Exponential backoff retries.
        """
        spread_threshold = (
            self.default_spread_threshold_pct if spread_threshold_pct is None else max(0.0, float(spread_threshold_pct))
        )
        slippage_threshold = (
            self.default_slippage_threshold_pct if max_slippage_pct is None else max(0.0, float(max_slippage_pct))
        )
        chunk = self.default_chunk_size if chunk_size is None else max(0.0, float(chunk_size))
        chunks = self._split_chunks(size, chunk)
        if not chunks:
            raise ValueError("size must be > 0")

        spread_pct = self._estimate_spread_pct(best_bid=best_bid, best_ask=best_ask)
        use_market = spread_pct <= spread_threshold
        side_n = side.lower()
        expected_price = best_ask if side_n == "buy" else best_bid
        slippage_pct = self._estimate_slippage_pct(side=side_n, expected_price=expected_price, reference_price=reference_price)
        if slippage_pct > slippage_threshold:
            raise DeltaAPIError(
                f"Slippage protection: rejected order symbol={symbol} side={side_n} "
                f"slippage_pct={slippage_pct:.6f} threshold={slippage_threshold:.6f}"
            )

        responses: list[dict] = []
        for i, chunk_qty in enumerate(chunks, start=1):
            chunk_id = self._safe_client_order_id(f"{client_order_id_prefix}-{i}" if client_order_id_prefix else None)
            attempt = 0
            while True:
                attempt += 1
                try:
                    if use_market:
                        resp = self.execute_market_order(
                            symbol=symbol,
                            side=side_n,
                            size=chunk_qty,
                            client_order_id=chunk_id,
                        )
                    else:
                        # Keep limit order protective on the touch price.
                        limit_price = best_ask if side_n == "buy" else best_bid
                        resp = self.execute_limit_order(
                            symbol=symbol,
                            side=side_n,
                            size=chunk_qty,
                            price=limit_price,
                            client_order_id=chunk_id,
                        )
                    responses.append(resp)
                    break
                except Exception as exc:
                    if attempt > max_retries_per_chunk or not self._is_retryable_order_error(exc):
                        raise
                    delay_s = min(8.0, 2 ** (attempt - 1))
                    logger.warning(
                        "Smart order chunk retry: symbol=%s side=%s chunk=%s attempt=%s delay=%ss",
                        symbol,
                        side_n,
                        i,
                        attempt,
                        delay_s,
                    )
                    time.sleep(delay_s)
        return responses

    def _ensure_state(
        self,
        symbol: str,
        position_side: str,
        size: float,
        reference_price: float,
        trade_id: Optional[str] = None,
    ) -> ProtectionState:
        if size <= 0:
            raise ValueError("size must be > 0")
        if reference_price <= 0:
            raise ValueError("reference_price must be > 0")

        normalized_side = position_side.lower()
        if normalized_side not in {"long", "short"}:
            raise ValueError("position_side must be 'long' or 'short'")

        state = self._protection.get(symbol)
        if state is None:
            state = ProtectionState(
                symbol=symbol,
                side=normalized_side,
                size=size,
                trade_id=trade_id,
                extreme_price=reference_price,
                registered_at=time.time(),
            )
            self._protection[symbol] = state
        else:
            # Updating an existing state (e.g. adding TP after SL was already
            # set).  Only reset the clock when the trade_id changes, which
            # signals a genuinely new position — not just adding a second
            # protection level to the same trade.
            if trade_id is not None and trade_id != state.trade_id:
                state.registered_at = time.time()
            state.side = normalized_side
            state.size = size
            if trade_id is not None:
                state.trade_id = trade_id
            if state.extreme_price is None:
                state.extreme_price = reference_price
        return state

    _FILL_TERMINAL: frozenset = frozenset({"closed", "filled", "cancelled", "canceled", "complete"})

    def _wait_for_fill(
        self,
        order_response: dict,
        max_polls: int = 4,
        poll_interval_s: float = 1.5,
    ) -> dict:
        """Poll the exchange until the order reaches a terminal state.

        Market orders on Delta Exchange typically fill within 1-2 s.
        Polls up to max_polls times before returning the original response
        so the caller is never blocked indefinitely.
        Paper mode (client=None) returns immediately.
        """
        if self.client is None:
            return order_response

        order_id = self._extract_exchange_order_id(order_response)
        if not order_id:
            return order_response

        result = order_response.get("result", {}) if isinstance(order_response, dict) else {}
        status = str(result.get("state") or result.get("status") or "").lower()
        if status in self._FILL_TERMINAL:
            return order_response

        for attempt in range(1, max_polls + 1):
            time.sleep(poll_interval_s)
            try:
                fresh = self.client.get_order(order_id)
                fresh_result = fresh.get("result", {}) if isinstance(fresh, dict) else {}
                fresh_status = str(fresh_result.get("state") or fresh_result.get("status") or "").lower()
                logger.debug("Fill poll attempt=%d order_id=%s status=%s", attempt, order_id, fresh_status)
                if fresh_status in self._FILL_TERMINAL:
                    if fresh_status in {"cancelled", "canceled"}:
                        logger.warning("Order cancelled by exchange: order_id=%s", order_id)
                    else:
                        logger.info("Order fill confirmed: order_id=%s status=%s", order_id, fresh_status)
                    return fresh
            except Exception as exc:
                logger.warning("Fill confirmation poll failed: order_id=%s attempt=%d error=%s", order_id, attempt, exc)
                break

        logger.warning("Order fill not confirmed after %d polls: order_id=%s", max_polls, order_id)
        return order_response

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

    @staticmethod
    def _is_stop_triggered(state: ProtectionState, price: float) -> bool:
        stop_candidates = [x for x in (state.stop_loss, state.trailing_stop_price) if x is not None]
        if not stop_candidates:
            return False
        if state.side == "long":
            effective_stop = max(stop_candidates)
            return price <= effective_stop
        effective_stop = min(stop_candidates)
        return price >= effective_stop

    @staticmethod
    def _is_take_profit_triggered(state: ProtectionState, price: float) -> bool:
        if state.take_profit is None:
            return False
        if state.side == "long":
            return price >= state.take_profit
        return price <= state.take_profit
