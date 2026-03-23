import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Any

from sqlalchemy import create_engine, select, update, delete
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError

from delta_exchange_bot.persistence.models import Base, Trade, Position, Order, Signal, ExecutionLog, PerformanceMetric, OrderStatus, PositionSide, TradeStatus

logger = logging.getLogger(__name__)

class DatabaseManager:
    """PostgreSQL Database Manager using SQLAlchemy."""
    
    def __init__(self, dsn: str):
        if dsn.startswith("sqlite"):
            self.engine = create_engine(dsn)
        else:
            self.engine = create_engine(
                dsn,
                pool_pre_ping=True,
                pool_size=10,
                max_overflow=20
            )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self._create_tables()

    def _create_tables(self):
        """Create tables if they don't exist."""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables verified/created successfully.")
        except Exception as e:
            logger.error(f"Error creating database tables: {e}")
            raise

    def get_session(self) -> Session:
        return self.SessionLocal()

    # --- Signal Operations ---

    def save_signal(self, signal_data: Optional[dict] = None, **kwargs) -> None:
        payload = dict(signal_data or {})
        payload.update(kwargs)
        with self.get_session() as session:
            try:
                signal = Signal(
                    signal_id=payload["signal_id"],
                    strategy_name=payload["strategy_name"],
                    symbol=payload["symbol"],
                    action=payload["action"],
                    confidence=payload["confidence"],
                    price=payload["price"],
                    stop_loss=payload.get("stop_loss"),
                    take_profit=payload.get("take_profit"),
                    regime=payload.get("regime"),
                    metadata_json=payload.get("metadata", {})
                )
                session.add(signal)
                session.commit()
            except IntegrityError:
                session.rollback()
                logger.warning(f"Signal {payload['signal_id']} already exists.")
            except Exception as e:
                session.rollback()
                logger.error(f"Error saving signal: {e}")

    # --- Position Operations ---

    def get_active_position(self, symbol: str) -> Optional[dict]:
        with self.get_session() as session:
            pos = session.query(Position).filter(Position.symbol == symbol).first()
            if not pos:
                return None
            return {
                "symbol": pos.symbol,
                "trade_id": pos.trade_id,
                "side": pos.side.value,
                "size": pos.size,
                "avg_entry_price": pos.avg_entry_price,
                "stop_loss": pos.stop_loss,
                "take_profit": pos.take_profit,
                "updated_at": pos.updated_at
            }

    def update_position(self, pos_data: dict) -> None:
        """Upsert current position state."""
        with self.get_session() as session:
            try:
                pos = session.query(Position).filter(Position.symbol == pos_data["symbol"]).first()
                if not pos:
                    pos = Position(
                        symbol=pos_data["symbol"],
                        trade_id=pos_data["trade_id"],
                        side=PositionSide(pos_data["side"].lower()),
                        size=pos_data["size"],
                        avg_entry_price=pos_data["avg_entry_price"],
                        stop_loss=pos_data.get("stop_loss"),
                        take_profit=pos_data.get("take_profit")
                    )
                    session.add(pos)
                else:
                    pos.trade_id = pos_data["trade_id"]
                    pos.side = PositionSide(pos_data["side"].lower())
                    pos.size = pos_data["size"]
                    pos.avg_entry_price = pos_data["avg_entry_price"]
                    pos.stop_loss = pos_data.get("stop_loss")
                    pos.take_profit = pos_data.get("take_profit")
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"Error updating position {pos_data['symbol']}: {e}")

    def close_position(self, symbol: str) -> None:
        with self.get_session() as session:
            try:
                session.query(Position).filter(Position.symbol == symbol).delete()
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"Error closing position {symbol}: {e}")

    # --- Trade Lifecycle Operations ---

    def create_trade(self, trade_data: dict) -> None:
        with self.get_session() as session:
            try:
                trade = Trade(
                    trade_id=trade_data["trade_id"],
                    symbol=trade_data["symbol"],
                    strategy_name=trade_data.get("strategy_name"),
                    side=PositionSide(trade_data["side"].lower()),
                    size=trade_data["size"],
                    entry_price=trade_data["entry_price"],
                    status=TradeStatus.OPEN
                )
                session.add(trade)
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"Error creating trade {trade_data['trade_id']}: {e}")

    def upsert_trade_record(self, **kwargs) -> None:
        """Compatibility helper for code paths that expect a trade upsert API."""
        trade_id = kwargs.get("trade_id")
        if not trade_id:
            logger.error("upsert_trade_record called without trade_id")
            return

        with self.get_session() as session:
            try:
                trade = session.query(Trade).filter(Trade.trade_id == trade_id).first()
                side_value = str(kwargs.get("side", "long")).lower()
                if trade is None:
                    trade = Trade(
                        trade_id=trade_id,
                        symbol=kwargs.get("symbol"),
                        strategy_name=kwargs.get("strategy_name"),
                        side=PositionSide(side_value),
                        size=kwargs.get("size", 0.0),
                        entry_price=kwargs.get("entry_price"),
                        status=TradeStatus.OPEN,
                        metadata_json=kwargs.get("metadata", {}),
                    )
                    session.add(trade)
                else:
                    trade.symbol = kwargs.get("symbol", trade.symbol)
                    trade.strategy_name = kwargs.get("strategy_name", trade.strategy_name)
                    trade.side = PositionSide(side_value)
                    trade.size = kwargs.get("size", trade.size)
                    trade.entry_price = kwargs.get("entry_price", trade.entry_price)
                    trade.status = TradeStatus.OPEN
                    trade.metadata_json = kwargs.get("metadata", trade.metadata_json)
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"Error upserting trade {trade_id}: {e}")

    def close_trade(self, trade_id: str, exit_price: float) -> None:
        with self.get_session() as session:
            try:
                trade = session.query(Trade).filter(Trade.trade_id == trade_id).first()
                if trade:
                    trade.exit_price = exit_price
                    trade.exit_time = datetime.utcnow()
                    trade.status = TradeStatus.CLOSED
                    
                    # Calculate PnL
                    if trade.side == PositionSide.LONG:
                        trade.pnl_raw = (exit_price - trade.entry_price) * trade.size
                    else:
                        trade.pnl_raw = (trade.entry_price - exit_price) * trade.size
                    
                    if trade.entry_price and trade.size:
                        trade.pnl_pct = (trade.pnl_raw / (trade.entry_price * trade.size)) * 100
                    
                    session.commit()
                    logger.info(f"Trade {trade_id} closed at {exit_price}. PnL: {trade.pnl_raw}")
            except Exception as e:
                session.rollback()
                logger.error(f"Error closing trade {trade_id}: {e}")

    # --- Order Tracking ---

    def save_order(self, order_data: dict) -> None:
        with self.get_session() as session:
            try:
                order = Order(
                    client_order_id=order_data["client_order_id"],
                    order_id=order_data.get("order_id"),
                    trade_id=order_data.get("trade_id"),
                    symbol=order_data["symbol"],
                    side=order_data["side"],
                    order_type=order_data["order_type"],
                    size=order_data["size"],
                    price=order_data.get("price"),
                    status=OrderStatus(order_data.get("status", "pending").lower()),
                    metadata_json=order_data.get("metadata", {})
                )
                session.add(order)
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"Error saving order {order_data['client_order_id']}: {e}")

    def update_order_status(self, client_order_id: str, status: str, order_id: str = None, filled_size: float = None, avg_price: float = None) -> None:
        with self.get_session() as session:
            try:
                order = session.query(Order).filter(Order.client_order_id == client_order_id).first()
                if order:
                    order.status = OrderStatus(status.lower())
                    if order_id: order.order_id = order_id
                    if filled_size is not None: order.filled_size = filled_size
                    if avg_price is not None: order.avg_fill_price = avg_price
                    session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"Error updating order {client_order_id}: {e}")

    # --- Execution Logs ---

    def log_execution(self, exec_data: dict) -> None:
        with self.get_session() as session:
            try:
                log = ExecutionLog(
                    execution_id=exec_data["execution_id"],
                    trade_id=exec_data.get("trade_id"),
                    order_id=exec_data.get("order_id"),
                    symbol=exec_data["symbol"],
                    event_type=exec_data["event_type"],
                    side=exec_data.get("side"),
                    size=exec_data.get("size"),
                    price=exec_data.get("price"),
                    status=exec_data.get("status"),
                    reason=exec_data.get("reason"),
                    metadata_json=exec_data.get("metadata", {})
                )
                session.add(log)
                session.commit()
            except IntegrityError:
                session.rollback()
            except Exception as e:
                session.rollback()
                logger.error(f"Error logging execution: {e}")

    # --- Dashboard / Query Methods ---

    def get_all_active_positions(self) -> List[dict]:
        with self.get_session() as session:
            positions = session.query(Position).all()
            return [
                {
                    "symbol": p.symbol,
                    "trade_id": p.trade_id,
                    "side": p.side.value,
                    "size": p.size,
                    "avg_entry_price": p.avg_entry_price,
                    "updated_at": p.updated_at.isoformat() if p.updated_at else None
                }
                for p in positions
            ]

    def get_signals_history(self, limit: int = 50) -> List[dict]:
        with self.get_session() as session:
            signals = session.query(Signal).order_by(Signal.created_at.desc()).limit(limit).all()
            return [
                {
                    "signal_id": s.signal_id,
                    "strategy_name": s.strategy_name,
                    "symbol": s.symbol,
                    "action": s.action,
                    "confidence": s.confidence,
                    "price": s.price,
                    "created_at": s.created_at.isoformat() if s.created_at else None
                }
                for s in signals
            ]

    def get_execution_history(self, limit: int = 50) -> List[dict]:
        with self.get_session() as session:
            logs = session.query(ExecutionLog).order_by(ExecutionLog.created_at.desc()).limit(limit).all()
            return [
                {
                    "execution_id": l.execution_id,
                    "symbol": l.symbol,
                    "event_type": l.event_type,
                    "side": l.side,
                    "size": l.size,
                    "price": l.price,
                    "status": l.status,
                    "created_at": l.created_at.isoformat() if l.created_at else None
                }
                for l in logs
            ]

    def get_trade_records(self, limit: int = 50) -> List[dict]:
        with self.get_session() as session:
            trades = session.query(Trade).order_by(Trade.entry_time.desc()).limit(limit).all()
            return [
                {
                    "trade_id": t.trade_id,
                    "symbol": t.symbol,
                    "side": t.side.value if t.side else None,
                    "status": t.status.value if t.status else None,
                    "entry_price": t.entry_price,
                    "exit_price": t.exit_price,
                    "pnl_raw": t.pnl_raw,
                    "pnl_pct": t.pnl_pct
                }
                for t in trades
            ]

    def save_performance_metrics(self, **kwargs):
        """Saves a snapshot of bot performance."""
        with self.get_session() as session:
            try:
                metric = PerformanceMetric(
                    mode=kwargs.get("mode"),
                    total_trades=kwargs.get("total_trades"),
                    win_rate=kwargs.get("win_rate"),
                    profit_factor=kwargs.get("profit_factor"),
                    max_drawdown=kwargs.get("max_drawdown"),
                    realized_pnl=kwargs.get("realized_pnl"),
                    unrealized_pnl=kwargs.get("unrealized_pnl"),
                    metadata_json=kwargs.get("metadata", {})
                )
                session.add(metric)
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"Error saving performance metrics: {e}")

    # --- Legacy Aliases for Professional Bot Compatibility ---

    def upsert_open_position_state(self, **kwargs):
        """Alias for update_position."""
        # Map old argument names if necessary
        data = {
            "symbol": kwargs.get("symbol"),
            "trade_id": kwargs.get("trade_id"),
            "side": kwargs.get("side"),
            "size": kwargs.get("size"),
            "avg_entry_price": kwargs.get("entry_price"), # legacy used entry_price
            "stop_loss": kwargs.get("stop_loss"),
            "take_profit": kwargs.get("take_profit")
        }
        self.update_position(data)

    def close_trade_record(self, trade_id: str, exit_price: float):
        """Alias for close_trade."""
        self.close_trade(trade_id, exit_price)

    def save_execution(self, **kwargs):
        """Alias for log_execution."""
        self.log_execution({
            "execution_id": kwargs.get("execution_id"),
            "trade_id": kwargs.get("trade_id"),
            "symbol": kwargs.get("symbol"),
            "event_type": kwargs.get("event_type"),
            "side": kwargs.get("side"),
            "size": kwargs.get("size"),
            "price": kwargs.get("price"),
            "status": kwargs.get("status"),
            "reason": kwargs.get("reason"),
            "metadata": kwargs.get("metadata")
        })
