from datetime import datetime, timezone
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Enum, ForeignKey, Index, Text
from sqlalchemy.orm import DeclarativeBase, relationship


def _utcnow():
    return datetime.now(timezone.utc)

class Base(DeclarativeBase):
    pass

class OrderStatus(PyEnum):
    PENDING = "pending"
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"

class PositionSide(PyEnum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"

class TradeStatus(PyEnum):
    OPEN = "open"
    CLOSED = "closed"

class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True)
    signal_id = Column(String(64), unique=True, nullable=False, index=True)
    strategy_name = Column(String(64), nullable=False)
    symbol = Column(String(32), nullable=False, index=True)
    action = Column(String(16), nullable=False)  # buy, sell, hold
    confidence = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    stop_loss = Column(Float)
    take_profit = Column(Float)
    regime = Column(String(32))
    metadata_json = Column(JSON)
    created_at = Column(DateTime, default=_utcnow, index=True)

class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True)
    order_id = Column(String(64), unique=True, index=True) # Exchange order ID
    client_order_id = Column(String(64), unique=True, index=True)
    trade_id = Column(String(64), ForeignKey("trades.trade_id"), index=True)
    symbol = Column(String(32), nullable=False, index=True)
    side = Column(String(16), nullable=False)
    order_type = Column(String(32), nullable=False)
    size = Column(Float, nullable=False)
    price = Column(Float)
    status = Column(Enum(OrderStatus), nullable=False, default=OrderStatus.PENDING)
    filled_size = Column(Float, default=0.0)
    avg_fill_price = Column(Float)
    error_message = Column(Text)
    metadata_json = Column(JSON)
    created_at = Column(DateTime, default=_utcnow, index=True)
    updated_at = Column(DateTime, default=_utcnow, onupdate=datetime.utcnow)

    trade = relationship("Trade", back_populates="orders")

class Trade(Base):
    """Represents a logical trade (entry to exit)."""
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True)
    trade_id = Column(String(64), unique=True, nullable=False, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    strategy_name = Column(String(64))
    side = Column(Enum(PositionSide), nullable=False)
    size = Column(Float, nullable=False)
    entry_price = Column(Float)
    exit_price = Column(Float)
    entry_time = Column(DateTime, default=_utcnow)
    exit_time = Column(DateTime)
    pnl_raw = Column(Float)
    pnl_pct = Column(Float)
    status = Column(Enum(TradeStatus), default=TradeStatus.OPEN, index=True)
    metadata_json = Column(JSON)

    orders = relationship("Order", back_populates="trade")

class Position(Base):
    """Current open position state - Single Source of Truth."""
    __tablename__ = "positions"

    symbol = Column(String(32), primary_key=True)
    trade_id = Column(String(64), ForeignKey("trades.trade_id"), nullable=False)
    side = Column(Enum(PositionSide), nullable=False)
    size = Column(Float, nullable=False)
    avg_entry_price = Column(Float, nullable=False)
    stop_loss = Column(Float)
    take_profit = Column(Float)
    liquidation_price = Column(Float)
    margin = Column(Float)
    # Exchange order IDs for native SL/TP orders — persisted so they survive
    # a bot restart and can be cancelled when the position closes.
    stop_order_id = Column(String(64), nullable=True)
    tp_order_id = Column(String(64), nullable=True)
    updated_at = Column(DateTime, default=_utcnow, onupdate=datetime.utcnow)

class ExecutionLog(Base):
    __tablename__ = "execution_logs"

    id = Column(Integer, primary_key=True)
    execution_id = Column(String(64), unique=True, nullable=False, index=True)
    trade_id = Column(String(64), index=True)
    order_id = Column(String(64), index=True)
    symbol = Column(String(32), nullable=False)
    event_type = Column(String(64), nullable=False) # e.g., 'fill', 'cancel', 'reject'
    side = Column(String(16))
    size = Column(Float)
    price = Column(Float)
    status = Column(String(32))
    reason = Column(Text)
    metadata_json = Column(JSON)
    created_at = Column(DateTime, default=_utcnow, index=True)

class PerformanceMetric(Base):
    __tablename__ = "performance_metrics"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=_utcnow, index=True)
    mode = Column(String(16), index=True)
    total_trades = Column(Integer)
    win_rate = Column(Float)
    profit_factor = Column(Float)
    max_drawdown = Column(Float)
    realized_pnl = Column(Float)
    unrealized_pnl = Column(Float)
    metadata_json = Column(JSON)


class BotState(Base):
    """Persistent key-value store for bot runtime state that must survive restarts.

    Example keys: "start_of_day_equity" — baseline equity for the daily kill switch.
    """
    __tablename__ = "bot_state"

    key = Column(String(64), primary_key=True)
    value_float = Column(Float, nullable=True)
    date_str = Column(String(16), nullable=True)  # "YYYY-MM-DD" — for day-scoped values
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)
