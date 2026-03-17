import sqlite3
import json
from pathlib import Path
from typing import Dict
from typing import Optional


class StateDB:
    def __init__(self, path: str = "state.db"):
        self._path = Path(path)
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._create_tables()

    def _create_tables(self):
        with self._conn:
            self._conn.execute("""CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY, symbol TEXT, side TEXT, size REAL, price REAL, ts DATETIME DEFAULT CURRENT_TIMESTAMP)""")
            self._conn.execute("""CREATE TABLE IF NOT EXISTS positions (symbol TEXT PRIMARY KEY, size REAL, avg_price REAL)""")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY,
                    trade_id TEXT,
                    order_id TEXT,
                    client_order_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    size REAL NOT NULL,
                    price REAL,
                    status TEXT NOT NULL,
                    metadata_json TEXT,
                    ts DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY,
                    signal_id TEXT UNIQUE,
                    strategy_name TEXT,
                    regime TEXT,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    price REAL NOT NULL,
                    stop_loss REAL,
                    take_profit REAL,
                    trailing_stop_pct REAL,
                    metadata_json TEXT,
                    ts DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id INTEGER PRIMARY KEY,
                    mode TEXT NOT NULL,
                    total_trades INTEGER NOT NULL,
                    win_rate REAL NOT NULL,
                    profit_factor REAL NOT NULL,
                    max_drawdown REAL NOT NULL,
                    realized_pnl REAL NOT NULL,
                    unrealized_pnl REAL NOT NULL,
                    metadata_json TEXT,
                    ts DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_logs (
                    id INTEGER PRIMARY KEY,
                    trade_id TEXT NOT NULL,
                    execution_id TEXT NOT NULL UNIQUE,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    size REAL NOT NULL,
                    price REAL,
                    event_type TEXT NOT NULL,
                    order_type TEXT,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT,
                    client_order_id TEXT UNIQUE,
                    exchange_order_id TEXT,
                    metadata_json TEXT,
                    ts DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_execution_logs_trade_id ON execution_logs(trade_id)"
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS open_position_state (
                    symbol TEXT PRIMARY KEY,
                    trade_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    size REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    stop_loss REAL,
                    take_profit REAL,
                    trailing_stop_pct REAL,
                    mode TEXT NOT NULL,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_trade_id ON orders(trade_id)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_ts ON signals(symbol, ts)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_perf_mode_ts ON performance_metrics(mode, ts)")

    def save_trade(self, symbol: str, side: str, size: float, price: float):
        with self._conn:
            self._conn.execute("INSERT INTO trades (symbol, side, size, price) VALUES (?, ?, ?, ?)", (symbol, side, size, price))

    def save_order_record(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        size: float,
        status: str,
        trade_id: Optional[str] = None,
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None,
        price: Optional[float] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        metadata_json = json.dumps(metadata or {}, separators=(",", ":"), sort_keys=True)
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO orders (
                    trade_id, order_id, client_order_id, symbol, side, order_type, size, price, status, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (trade_id, order_id, client_order_id, symbol, side, order_type, size, price, status, metadata_json),
            )

    def save_signal(
        self,
        *,
        signal_id: str,
        strategy_name: str,
        symbol: str,
        action: str,
        confidence: float,
        price: float,
        regime: Optional[str] = None,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        metadata_json = json.dumps(metadata or {}, separators=(",", ":"), sort_keys=True)
        with self._conn:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO signals (
                    signal_id, strategy_name, regime, symbol, action, confidence, price,
                    stop_loss, take_profit, trailing_stop_pct, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    strategy_name,
                    regime,
                    symbol,
                    action,
                    confidence,
                    price,
                    stop_loss,
                    take_profit,
                    trailing_stop_pct,
                    metadata_json,
                ),
            )

    def save_performance_metrics(
        self,
        *,
        mode: str,
        total_trades: int,
        win_rate: float,
        profit_factor: float,
        max_drawdown: float,
        realized_pnl: float,
        unrealized_pnl: float,
        metadata: Optional[dict] = None,
    ) -> None:
        metadata_json = json.dumps(metadata or {}, separators=(",", ":"), sort_keys=True)
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO performance_metrics (
                    mode, total_trades, win_rate, profit_factor, max_drawdown, realized_pnl, unrealized_pnl, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (mode, total_trades, win_rate, profit_factor, max_drawdown, realized_pnl, unrealized_pnl, metadata_json),
            )

    def save_execution(
        self,
        *,
        trade_id: str,
        execution_id: str,
        symbol: str,
        side: str,
        size: float,
        price: Optional[float],
        event_type: str,
        mode: str,
        status: str,
        order_type: Optional[str] = None,
        reason: Optional[str] = None,
        client_order_id: Optional[str] = None,
        exchange_order_id: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> bool:
        """Persist execution log row. Returns True when inserted, False if duplicate."""
        metadata_json = json.dumps(metadata or {}, separators=(",", ":"), sort_keys=True)
        with self._conn:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO execution_logs (
                    trade_id, execution_id, symbol, side, size, price, event_type,
                    order_type, mode, status, reason, client_order_id, exchange_order_id, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade_id,
                    execution_id,
                    symbol,
                    side,
                    size,
                    price,
                    event_type,
                    order_type,
                    mode,
                    status,
                    reason,
                    client_order_id,
                    exchange_order_id,
                    metadata_json,
                ),
            )
        return cursor.rowcount == 1

    def get_executions_by_trade_id(self, trade_id: str) -> list[dict]:
        cursor = self._conn.execute(
            """
            SELECT trade_id, execution_id, symbol, side, size, price, event_type, order_type,
                   mode, status, reason, client_order_id, exchange_order_id, metadata_json, ts
            FROM execution_logs
            WHERE trade_id = ?
            ORDER BY id ASC
            """,
            (trade_id,),
        )
        out = []
        for row in cursor.fetchall():
            out.append(
                {
                    "trade_id": row[0],
                    "execution_id": row[1],
                    "symbol": row[2],
                    "side": row[3],
                    "size": row[4],
                    "price": row[5],
                    "event_type": row[6],
                    "order_type": row[7],
                    "mode": row[8],
                    "status": row[9],
                    "reason": row[10],
                    "client_order_id": row[11],
                    "exchange_order_id": row[12],
                    "metadata": json.loads(row[13] or "{}"),
                    "ts": row[14],
                }
            )
        return out

    def upsert_open_position_state(
        self,
        *,
        symbol: str,
        trade_id: str,
        side: str,
        size: float,
        entry_price: float,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        mode: str,
    ) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO open_position_state (
                    symbol, trade_id, side, size, entry_price, stop_loss, take_profit, trailing_stop_pct, mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    trade_id=excluded.trade_id,
                    side=excluded.side,
                    size=excluded.size,
                    entry_price=excluded.entry_price,
                    stop_loss=excluded.stop_loss,
                    take_profit=excluded.take_profit,
                    trailing_stop_pct=excluded.trailing_stop_pct,
                    mode=excluded.mode,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (symbol, trade_id, side, size, entry_price, stop_loss, take_profit, trailing_stop_pct, mode),
            )

    def remove_open_position_state(self, symbol: str) -> None:
        with self._conn:
            self._conn.execute("DELETE FROM open_position_state WHERE symbol = ?", (symbol,))

    def load_open_position_state(self, mode: Optional[str] = None) -> dict[str, dict]:
        if mode is None:
            cursor = self._conn.execute(
                """
                SELECT symbol, trade_id, side, size, entry_price, stop_loss, take_profit, trailing_stop_pct, mode
                FROM open_position_state
                """
            )
        else:
            cursor = self._conn.execute(
                """
                SELECT symbol, trade_id, side, size, entry_price, stop_loss, take_profit, trailing_stop_pct, mode
                FROM open_position_state
                WHERE mode = ?
                """,
                (mode,),
            )

        out: dict[str, dict] = {}
        for row in cursor.fetchall():
            out[row[0]] = {
                "trade_id": row[1],
                "side": row[2],
                "size": row[3],
                "entry_price": row[4],
                "stop_loss": row[5],
                "take_profit": row[6],
                "trailing_stop_pct": row[7],
                "mode": row[8],
            }
        return out

    def get_positions(self) -> Dict[str, Dict[str, float]]:
        cursor = self._conn.execute("SELECT symbol, size, avg_price FROM positions")
        return {row[0]: {"size": row[1], "avg_price": row[2]} for row in cursor.fetchall()}
