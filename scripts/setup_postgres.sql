-- Delta Exchange Trading Bot Schema (PostgreSQL)

-- ENUMS
CREATE TYPE order_status AS ENUM (
    'pending', 'new', 'partially_filled', 'filled', 'cancelled', 'rejected', 'expired'
);

CREATE TYPE position_side AS ENUM (
    'long', 'short', 'flat'
);

CREATE TYPE trade_status AS ENUM (
    'open', 'closed'
);

-- SIGNALS table
CREATE TABLE signals (
    id SERIAL PRIMARY KEY,
    signal_id VARCHAR(64) UNIQUE NOT NULL,
    strategy_name VARCHAR(64) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    action VARCHAR(16) NOT NULL,
    confidence FLOAT NOT NULL,
    price FLOAT NOT NULL,
    stop_loss FLOAT,
    take_profit FLOAT,
    regime VARCHAR(32),
    metadata_json JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_signals_symbol ON signals(symbol);
CREATE INDEX idx_signals_created_at ON signals(created_at);

-- TRADES table (Logical Trades)
CREATE TABLE trades (
    id SERIAL PRIMARY KEY,
    trade_id VARCHAR(64) UNIQUE NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    strategy_name VARCHAR(64),
    side position_side NOT NULL,
    size FLOAT NOT NULL,
    entry_price FLOAT,
    exit_price FLOAT,
    entry_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    exit_time TIMESTAMP WITH TIME ZONE,
    pnl_raw FLOAT,
    pnl_pct FLOAT,
    status trade_status DEFAULT 'open',
    metadata_json JSONB
);
CREATE INDEX idx_trades_symbol ON trades(symbol);
CREATE INDEX idx_trades_status ON trades(status);

-- ORDERS table (Exchange Orders)
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(64) UNIQUE,
    client_order_id VARCHAR(64) UNIQUE,
    trade_id VARCHAR(64) REFERENCES trades(trade_id),
    symbol VARCHAR(32) NOT NULL,
    side VARCHAR(16) NOT NULL,
    order_type VARCHAR(32) NOT NULL,
    size FLOAT NOT NULL,
    price FLOAT,
    status order_status NOT NULL DEFAULT 'pending',
    filled_size FLOAT DEFAULT 0.0,
    avg_fill_price FLOAT,
    error_message TEXT,
    metadata_json JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_orders_trade_id ON orders(trade_id);
CREATE INDEX idx_orders_symbol ON orders(symbol);

-- POSITIONS table (Single Source of Truth)
CREATE TABLE positions (
    symbol VARCHAR(32) PRIMARY KEY,
    trade_id VARCHAR(64) REFERENCES trades(trade_id) NOT NULL,
    side position_side NOT NULL,
    size FLOAT NOT NULL,
    avg_entry_price FLOAT NOT NULL,
    stop_loss FLOAT,
    take_profit FLOAT,
    liquidation_price FLOAT,
    margin FLOAT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- EXECUTION_LOGS table
CREATE TABLE execution_logs (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(64) UNIQUE NOT NULL,
    trade_id VARCHAR(64),
    order_id VARCHAR(64),
    symbol VARCHAR(32) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    side VARCHAR(16),
    size FLOAT,
    price FLOAT,
    status VARCHAR(32),
    reason TEXT,
    metadata_json JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_exec_logs_trade_id ON execution_logs(trade_id);
CREATE INDEX idx_exec_logs_symbol ON execution_logs(symbol);
