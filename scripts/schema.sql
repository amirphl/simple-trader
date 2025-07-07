-- Candles table (1m and higher, from exchange or constructed)
CREATE TABLE IF NOT EXISTS candles (
    symbol      VARCHAR(32) NOT NULL,
    timeframe   VARCHAR(8)  NOT NULL,
    timestamp   TIMESTAMP   NOT NULL,
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      DOUBLE PRECISION NOT NULL,
    source      VARCHAR(32) NOT NULL,
    PRIMARY KEY (symbol, timeframe, timestamp, source),
    UNIQUE (symbol, timeframe, timestamp, source)
);

-- TimescaleDB hypertable
SELECT create_hypertable('candles', 'timestamp', if_not_exists => TRUE);

-- Orderbooks (L2 snapshots)
CREATE TABLE IF NOT EXISTS orderbooks (
    symbol      VARCHAR(32) NOT NULL,
    timestamp   TIMESTAMP   NOT NULL,
    bids        JSONB       NOT NULL,
    asks        JSONB       NOT NULL,
    PRIMARY KEY (symbol, timestamp)
);
SELECT create_hypertable('orderbooks', 'timestamp', if_not_exists => TRUE);

-- Ticks (trades)
CREATE TABLE IF NOT EXISTS ticks (
    symbol      VARCHAR(32) NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    quantity    DOUBLE PRECISION NOT NULL,
    side        VARCHAR(8) NOT NULL,
    timestamp   TIMESTAMP   NOT NULL,
    PRIMARY KEY (symbol, timestamp, price, quantity, side)
);
SELECT create_hypertable('ticks', 'timestamp', if_not_exists => TRUE);

-- Orders (submitted by bot)
CREATE TABLE IF NOT EXISTS orders (
    order_id    VARCHAR(64) PRIMARY KEY,
    symbol      VARCHAR(32) NOT NULL,
    side        VARCHAR(8) NOT NULL,
    type        VARCHAR(16) NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    quantity    DOUBLE PRECISION NOT NULL,
    status      VARCHAR(16) NOT NULL,
    filled_qty  DOUBLE PRECISION NOT NULL,
    avg_price   DOUBLE PRECISION NOT NULL,
    created_at  TIMESTAMP   NOT NULL,
    updated_at  TIMESTAMP   NOT NULL
);

-- Events (journal)
CREATE TABLE IF NOT EXISTS events (
    time        TIMESTAMP   NOT NULL,
    type        VARCHAR(32) NOT NULL,
    description VARCHAR(128) NOT NULL,
    data        JSONB       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(type, time);

-- State (for safe restart)
CREATE TABLE IF NOT EXISTS state (
    key         VARCHAR(64) PRIMARY KEY,
    value       JSONB       NOT NULL
); 