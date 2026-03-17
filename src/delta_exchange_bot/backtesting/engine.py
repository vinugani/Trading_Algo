from dataclasses import dataclass
from typing import Optional

import pandas as pd

from delta_exchange_bot.strategy.base import Signal
from delta_exchange_bot.strategy.base import Strategy


@dataclass
class _OpenTrade:
    trade_id: str
    symbol: str
    side: str  # "long" or "short"
    entry_time: pd.Timestamp
    entry_price: float
    size: float
    stop_loss: Optional[float]
    take_profit: Optional[float]
    trailing_stop_pct: Optional[float]
    trailing_stop_price: Optional[float]
    extreme_price: float


@dataclass
class BacktestResult:
    metrics: dict[str, float]
    trades: pd.DataFrame
    equity_curve: pd.DataFrame


class BacktestEngine:
    def __init__(
        self,
        strategy: Strategy,
        *,
        initial_equity: float = 10000.0,
        position_size: float = 1.0,
        fee_rate: float = 0.0,
    ):
        self.strategy = strategy
        self.initial_equity = float(initial_equity)
        self.position_size = float(position_size)
        self.fee_rate = float(fee_rate)

    def run(self, candles: pd.DataFrame, symbol: Optional[str] = None) -> BacktestResult:
        df = self._normalize_candles(candles, symbol=symbol)
        resolved_symbol = str(df["symbol"].iloc[0])
        close_prices = df["close"].astype(float).tolist()

        realized_pnl = 0.0
        open_trade: Optional[_OpenTrade] = None
        trades: list[dict] = []
        equity_rows: list[dict] = []
        trade_num = 0

        for i, row in df.iterrows():
            timestamp = row["timestamp"]
            open_price = float(row["open"])
            high = float(row["high"])
            low = float(row["low"])
            close = float(row["close"])
            closed_this_bar = False

            market_data = {resolved_symbol: {"prices": close_prices[: i + 1]}}
            signals = self.strategy.generate(market_data)
            signal = signals[0] if signals else Signal(symbol=resolved_symbol, action="hold", confidence=0.0, price=close)

            if open_trade is not None:
                protection_exit = self._check_protection_exit(open_trade, high=high, low=low)
                if protection_exit is not None:
                    exit_price, exit_reason = protection_exit
                    realized_pnl += self._close_trade(
                        trades=trades,
                        trade=open_trade,
                        exit_time=timestamp,
                        exit_price=exit_price,
                        reason=exit_reason,
                    )
                    open_trade = None
                    closed_this_bar = True

            if open_trade is not None:
                if open_trade.side == "long" and signal.action == "sell":
                    realized_pnl += self._close_trade(
                        trades=trades,
                        trade=open_trade,
                        exit_time=timestamp,
                        exit_price=close,
                        reason="signal_exit",
                    )
                    open_trade = None
                    closed_this_bar = True
                elif open_trade.side == "short" and signal.action == "buy":
                    realized_pnl += self._close_trade(
                        trades=trades,
                        trade=open_trade,
                        exit_time=timestamp,
                        exit_price=close,
                        reason="signal_exit",
                    )
                    open_trade = None
                    closed_this_bar = True

            if open_trade is None and not closed_this_bar:
                action = signal.action.lower()
                if action in {"buy", "sell"}:
                    trade_num += 1
                    side = "long" if action == "buy" else "short"
                    trail_pct = signal.trailing_stop_pct if signal.trailing_stop_pct and signal.trailing_stop_pct > 0 else None
                    if trail_pct is not None:
                        trail_price = close * (1.0 - trail_pct) if side == "long" else close * (1.0 + trail_pct)
                    else:
                        trail_price = None

                    open_trade = _OpenTrade(
                        trade_id=f"{resolved_symbol}-BT-{trade_num}",
                        symbol=resolved_symbol,
                        side=side,
                        entry_time=timestamp,
                        entry_price=close,
                        size=self.position_size,
                        stop_loss=signal.stop_loss,
                        take_profit=signal.take_profit,
                        trailing_stop_pct=trail_pct,
                        trailing_stop_price=trail_price,
                        extreme_price=close,
                    )

            unrealized = self._unrealized_pnl(open_trade, mark_price=close) if open_trade is not None else 0.0
            equity_rows.append({"timestamp": timestamp, "equity": self.initial_equity + realized_pnl + unrealized})

        if open_trade is not None:
            last_row = df.iloc[-1]
            realized_pnl += self._close_trade(
                trades=trades,
                trade=open_trade,
                exit_time=last_row["timestamp"],
                exit_price=float(last_row["close"]),
                reason="end_of_data",
            )
            equity_rows.append(
                {
                    "timestamp": last_row["timestamp"],
                    "equity": self.initial_equity + realized_pnl,
                }
            )

        trades_df = pd.DataFrame(trades)
        equity_df = pd.DataFrame(equity_rows).drop_duplicates(subset=["timestamp"], keep="last")
        metrics = self._compute_metrics(trades_df, equity_df)
        return BacktestResult(metrics=metrics, trades=trades_df, equity_curve=equity_df)

    @staticmethod
    def _normalize_candles(candles: pd.DataFrame, symbol: Optional[str]) -> pd.DataFrame:
        required = {"timestamp", "open", "high", "low", "close"}
        missing = required - set(candles.columns)
        if missing:
            raise ValueError(f"Missing candle columns: {sorted(missing)}")

        df = candles.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp", "open", "high", "low", "close"]).sort_values("timestamp").reset_index(drop=True)
        if df.empty:
            raise ValueError("No valid candles to backtest")

        if "symbol" not in df.columns:
            if symbol is None:
                symbol = "SYMBOL"
            df["symbol"] = symbol

        unique_symbols = df["symbol"].astype(str).unique().tolist()
        if len(unique_symbols) != 1:
            raise ValueError("BacktestEngine currently supports one symbol per run")
        if symbol is not None and str(unique_symbols[0]) != str(symbol):
            raise ValueError(f"Candle symbol mismatch: expected {symbol}, got {unique_symbols[0]}")
        return df

    @staticmethod
    def _unrealized_pnl(trade: _OpenTrade, mark_price: float) -> float:
        if trade.side == "long":
            return (mark_price - trade.entry_price) * trade.size
        return (trade.entry_price - mark_price) * trade.size

    def _check_protection_exit(self, trade: _OpenTrade, *, high: float, low: float) -> Optional[tuple[float, str]]:
        if trade.trailing_stop_pct is not None:
            if trade.side == "long":
                trade.extreme_price = max(trade.extreme_price, high)
                trade.trailing_stop_price = trade.extreme_price * (1.0 - trade.trailing_stop_pct)
            else:
                trade.extreme_price = min(trade.extreme_price, low)
                trade.trailing_stop_price = trade.extreme_price * (1.0 + trade.trailing_stop_pct)

        stop_levels = [x for x in (trade.stop_loss, trade.trailing_stop_price) if x is not None]
        stop_level = None
        if stop_levels:
            stop_level = max(stop_levels) if trade.side == "long" else min(stop_levels)

        # Conservative assumption: if both stop and take-profit are touched in the same candle,
        # stop-loss takes priority.
        if trade.side == "long":
            if stop_level is not None and low <= stop_level:
                return stop_level, "stop_loss"
            if trade.take_profit is not None and high >= trade.take_profit:
                return trade.take_profit, "take_profit"
            return None

        if stop_level is not None and high >= stop_level:
            return stop_level, "stop_loss"
        if trade.take_profit is not None and low <= trade.take_profit:
            return trade.take_profit, "take_profit"
        return None

    def _close_trade(
        self,
        *,
        trades: list[dict],
        trade: _OpenTrade,
        exit_time: pd.Timestamp,
        exit_price: float,
        reason: str,
    ) -> float:
        if trade.side == "long":
            gross = (exit_price - trade.entry_price) * trade.size
        else:
            gross = (trade.entry_price - exit_price) * trade.size

        entry_fee = trade.entry_price * trade.size * self.fee_rate
        exit_fee = exit_price * trade.size * self.fee_rate
        net = gross - entry_fee - exit_fee
        notional = trade.entry_price * trade.size

        trades.append(
            {
                "trade_id": trade.trade_id,
                "symbol": trade.symbol,
                "side": trade.side,
                "entry_time": trade.entry_time,
                "exit_time": exit_time,
                "entry_price": trade.entry_price,
                "exit_price": exit_price,
                "size": trade.size,
                "gross_pnl": gross,
                "net_pnl": net,
                "return_pct": (net / notional * 100.0) if notional > 0 else 0.0,
                "exit_reason": reason,
            }
        )
        return net

    @staticmethod
    def _compute_metrics(trades_df: pd.DataFrame, equity_df: pd.DataFrame) -> dict[str, float]:
        if trades_df.empty:
            total_pnl = 0.0
            win_rate = 0.0
            profit_factor = 0.0
        else:
            total_pnl = float(trades_df["net_pnl"].sum())
            wins = int((trades_df["net_pnl"] > 0).sum())
            total = int(len(trades_df))
            win_rate = (wins / total * 100.0) if total > 0 else 0.0

            gross_profit = float(trades_df.loc[trades_df["net_pnl"] > 0, "net_pnl"].sum())
            gross_loss_abs = float(-trades_df.loc[trades_df["net_pnl"] < 0, "net_pnl"].sum())
            if gross_loss_abs == 0:
                profit_factor = float("inf") if gross_profit > 0 else 0.0
            else:
                profit_factor = gross_profit / gross_loss_abs

        if equity_df.empty:
            max_drawdown = 0.0
        else:
            eq = equity_df["equity"].astype(float)
            running_max = eq.cummax()
            drawdown = (running_max - eq) / running_max.replace(0, pd.NA)
            max_drawdown = float(drawdown.fillna(0.0).max() * 100.0)

        return {
            "total_pnl": total_pnl,
            "win_rate": win_rate,
            "max_drawdown": max_drawdown,
            "profit_factor": profit_factor,
            "total_trades": float(len(trades_df)),
        }
