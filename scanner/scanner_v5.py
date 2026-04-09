#!/usr/bin/env python3
"""
QuantForce_Labs — scanner_v5.py
职责：遍历白名单 → 拉新闻(Finnhub) → 插入 llm_tasks(news_clean)
v5改进：SQLite→PostgreSQL + 429处理 + 短连接 + 更好日志
Pipeline: .143 qwen → .11 phi3 → .18 groq → dispatcher:5556
"""
import os, json, hashlib, psycopg2, time, logging, requests
from datetime import datetime, timedelta

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "d6o9obhr01qu09cild8gd6o9obhr01qu09cild90")
PG_CONF         = dict(host="192.168.0.18", port=5432, dbname="quantforce",
                       user="heng", password="Wh210712!", connect_timeout=10)
WHITELIST_PATH  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "whitelist.json")
NEWS_HOURS      = 48
SCAN_INTERVAL   = 1800

log_dir = os.path.expanduser("~/logs")
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SCANNER_v5] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(log_dir, "scanner_v5.log"), encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

def get_news_finnhub(ticker, hours=NEWS_HOURS, retries=3):
    date_from = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d")
    date_to   = datetime.now().strftime("%Y-%m-%d")
    for attempt in range(retries):
        try:
            r = requests.get(
                "https://finnhub.io/api/v1/company-news",
                params={"symbol": ticker, "from": date_from,
                        "to": date_to, "token": FINNHUB_API_KEY},
                timeout=10
            )
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 15))
                log.warning(f"{ticker} rate limit 429, 等待 {wait}s")
                time.sleep(wait + 2)
                continue
            r.raise_for_status()
            return r.json()[:8]
        except Exception as e:
            log.warning(f"{ticker} finnhub attempt {attempt+1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return []

def enqueue_news_clean(ticker, news, sector, moat):
    if not news:
        return False
    headlines = "\n".join([
        f"- {n.get('headline','')} ({str(n.get('datetime',''))[:10]})"
        for n in news[:6]
    ])
    input_text = f"ticker:{ticker}\nsector:{sector}\nmoat:{moat}\nnews:\n{headlines}"
    input_hash = hashlib.sha256(input_text.encode()).hexdigest()
    conn = None
    try:
        conn = psycopg2.connect(**PG_CONF)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM llm_tasks
                WHERE input_hash=%s AND task_type='news_clean'
                  AND created_at::date = CURRENT_DATE
                LIMIT 1
            """, (input_hash,))
            if cur.fetchone():
                log.info(f"  {ticker}: already queued today, skip")
                return False
            cur.execute("""
                INSERT INTO llm_tasks (task_type, input_hash, input_text, status, created_at)
                VALUES ('news_clean', %s, %s, 'pending', NOW())
            """, (input_hash, input_text))
        conn.commit()
        log.info(f"  {ticker}: queued {len(news)} news items")
        return True
    except Exception as e:
        log.error(f"  {ticker}: insert failed: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def run_scan():
    if not FINNHUB_API_KEY:
        log.error("FINNHUB_API_KEY 未设置")
        return
    try:
        with open(WHITELIST_PATH, encoding="utf-8") as f:
            wl = json.load(f)
        tickers = wl.get("tickers", [])
    except Exception as e:
        log.error(f"加载 whitelist.json 失败: {e}")
        return
    queued = 0
    t0 = datetime.now()
    log.info(f"Scan start {t0.strftime('%Y-%m-%d %H:%M')}  total={len(tickers)}")
    for item in tickers:
        ticker = item["symbol"]
        sector = item.get("sector", "")
        moat   = item.get("moat", "")
        log.info(f"-- {ticker} --")
        news = get_news_finnhub(ticker)
        if not news:
            log.info(f"  {ticker}: no news, skip")
            time.sleep(0.35)
            continue
        ok = enqueue_news_clean(ticker, news, sector, moat)
        if ok:
            queued += 1
        time.sleep(0.35)
    duration = (datetime.now() - t0).total_seconds()
    log.info(f"Scan complete  queued={queued}/{len(tickers)}  duration={duration:.1f}s")

if __name__ == "__main__":
    log.info(f"Scanner v5 启动  interval={SCAN_INTERVAL//60}min")
    while True:
        run_scan()
        log.info(f"Next scan in {SCAN_INTERVAL//60} minutes...")
        time.sleep(SCAN_INTERVAL)
