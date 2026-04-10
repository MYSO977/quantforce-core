"""
QuantForce DB — PostgreSQL 连接 + 五表建表
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor

def _build_dsn() -> str:
    # 优先读 .quant_env 里的分项变量
    host     = os.getenv('PG_HOST', '192.168.0.18')
    port     = os.getenv('PG_PORT', '5432')
    dbname   = os.getenv('PG_DB',   'quantforce')
    user     = os.getenv('PG_USER', 'heng')
    password = os.getenv('PG_PASS', '')
    return f'host={host} port={port} dbname={dbname} user={user} password={password}'

def get_conn():
    return psycopg2.connect(_build_dsn(), cursor_factory=RealDictCursor)

SCHEMA = """
-- L0 白名单：Russell3000 动态股票池
CREATE TABLE IF NOT EXISTS universe_whitelist (
    symbol              TEXT PRIMARY KEY,
    dollar_volume_rank  INT,
    sector              TEXT,
    market_cap          REAL,
    avg_dollar_volume   REAL,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- L1 异动预筛
CREATE TABLE IF NOT EXISTS market_events (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    rvol        REAL,
    vwap_dev    REAL,
    atr         REAL,
    event_type  TEXT,
    price       REAL,
    volume      REAL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_market_events_symbol ON market_events(symbol);
CREATE INDEX IF NOT EXISTS idx_market_events_ts     ON market_events(ts DESC);

-- L2 策略信号
CREATE TABLE IF NOT EXISTS signals_raw (
    id          BIGSERIAL PRIMARY KEY,
    symbol      TEXT NOT NULL,
    signal_type TEXT,
    strategy_id TEXT,
    confidence  REAL,
    is_primary  BOOL DEFAULT TRUE,
    reason      TEXT,
    event_hash  TEXT UNIQUE,
    status      TEXT DEFAULT 'pending',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- L3 融合信号
CREATE TABLE IF NOT EXISTS signals_final (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    confidence      REAL,
    reason          TEXT,
    resonance_score REAL,
    news_boost      REAL DEFAULT 0,
    status          TEXT DEFAULT 'pending',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- L4 执行记录
CREATE TABLE IF NOT EXISTS executions (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    action      TEXT,
    qty         INT,
    price       REAL,
    order_type  TEXT,
    ib_order_id INT,
    phi3_note   TEXT,
    signal_id   BIGINT REFERENCES signals_final(id),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 账户快照
CREATE TABLE IF NOT EXISTS account_state (
    id                BIGSERIAL PRIMARY KEY,
    net_liquidation   REAL,
    available_funds   REAL,
    positions         JSONB,
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);
"""

def init_db():
    """建表（幂等）"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
        conn.commit()
    print("[DB] 所有表初始化完成")

if __name__ == '__main__':
    init_db()
