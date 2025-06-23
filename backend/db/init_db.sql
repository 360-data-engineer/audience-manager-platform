-- Raw Data Tables
CREATE TABLE IF NOT EXISTS upi_transactions_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT NOT NULL UNIQUE,
    user_id TEXT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    merchant_name TEXT,
    category TEXT,
    city_tier INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS credit_card_payments_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT NOT NULL UNIQUE,
    user_id TEXT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    merchant_name TEXT,
    category TEXT,
    city_tier INTEGER,
    is_international BOOLEAN DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Aggregate Tables
CREATE TABLE IF NOT EXISTS upi_transactions_agg (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    total_transactions INTEGER DEFAULT 0,
    total_amount DECIMAL(15, 2) DEFAULT 0,
    last_transaction_date TIMESTAMP,
    favorite_category TEXT,
    city_tier INTEGER,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, period_start, period_end)
);

-- Segment Catalog
CREATE TABLE IF NOT EXISTS segment_catalog (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_name TEXT NOT NULL UNIQUE,
    description TEXT,
    sql_query TEXT NOT NULL,
    target_table TEXT NOT NULL,
    is_active BOOLEAN DEFAULT 1,
    refresh_frequency TEXT DEFAULT 'DAILY',
    last_refreshed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Rules Table
CREATE TABLE IF NOT EXISTS rule_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_name TEXT NOT NULL UNIQUE,
    description TEXT,
    conditions JSON NOT NULL,
    segment_id INTEGER,
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (segment_id) REFERENCES segment_catalog(id)
);
