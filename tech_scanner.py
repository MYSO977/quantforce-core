#!/usr/bin/env python3
"""
tech_scanner.py — 技术指标扫描器
三台机器通用，通过 CONFIG 区分部署参数
"""
import time, logging, json, threading, random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import yfinance as yf

# ─── CONFIG（部署时按节点修改）───────────────────────────────
NODE_NAME    = "center"          # center / executor / compute
MAX_WORKERS  = 10                # .18=10  .11=15  .143=5
SIGNAL_URL   = "http://192.168.0.18:5800/signal"
SCAN_INTERVAL = 300              # 秒，5分钟
COOLDOWN_MIN  = 60               # 同一票冷却分钟
PRICE_MIN     = 5.0
PRICE_MAX     = 800.0
MIN_AVG_VOL   = 300_000
# 股票池来源: "sp500" / "nasdaq100" / "russell1000" / 列表文件路径
TICKER_SOURCE = "/home/heng/tickers_vision.txt"          # .18用sp500, .11用russell1000, .143用nasdaq100
# ─────────────────────────────────────────────────────────────

ET = ZoneInfo("America/New_York")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"/tmp/tech_scanner_{NODE_NAME}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# 冷却记录 {ticker: last_signal_time}
_cooldown: dict[str, datetime] = {}
_cooldown_lock = threading.Lock()


# ── 股票池获取 ────────────────────────────────────────────────
def get_tickers(source: str) -> list[str]:
    try:
        if source == "sp500":
            df = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
            tickers = df["Symbol"].tolist()
        elif source == "nasdaq100":
            df = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4]
            tickers = df["Ticker"].tolist()
        elif source == "russell1000":
            df = pd.read_html("https://en.wikipedia.org/wiki/Russell_1000_Index")[2]
            tickers = df["Ticker"].tolist()
        else:
            with open(source) as f:
                tickers = [l.strip() for l in f if l.strip()]
        tickers = [t.replace(".", "-") for t in tickers]
        log.info(f"股票池加载完成: {len(tickers)} 只 ({source})")
        return tickers
    except Exception as e:
        log.error(f"股票池加载失败: {e}")
        return []


# ── 交易时间检查 ──────────────────────────────────────────────
def is_market_open() -> bool:
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return False
    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return market_open <= now <= market_close


# ── 单票数据拉取 + 指标计算 ───────────────────────────────────
def analyze_ticker(ticker: str) -> dict | None:
    try:
        time.sleep(random.uniform(0.1, 0.4))  # 防限流
        tk = yf.Ticker(ticker)

        # 拉今日1分钟K线（用于RVOL、VWAP、现价、开盘价）
        df1 = tk.history(period="1d", interval="1m")
        if df1.empty or len(df1) < 10:
            return None

        # 拉近30根5分钟K线（用于EMA9、MACD）
        df5 = tk.history(period="5d", interval="5m")
        if df5.empty or len(df5) < 30:
            return None

        # ── 基本价格过滤 ──
        current_price = float(df1["Close"].iloc[-1])
        open_price    = float(df1["Open"].iloc[0])
        if not (PRICE_MIN <= current_price <= PRICE_MAX):
            return None

        # ── 成交量过滤 ──
        avg_vol = float(df1["Volume"].mean()) * 390  # 估算日均成交量
        if avg_vol < MIN_AVG_VOL:
            return None

        # ── 条件1: RVOL ──
        now_et    = datetime.now(ET)
        elapsed   = (now_et.hour * 60 + now_et.minute) - (9 * 60 + 30)
        elapsed   = max(elapsed, 1)
        cum_vol   = float(df1["Volume"].sum())
        # 用过去5日同时段均量估算
        df5d = tk.history(period="5d", interval="1d")
        if len(df5d) >= 2:
            avg_daily_vol = float(df5d["Volume"].iloc[:-1].mean())
            avg_now_vol   = avg_daily_vol * (elapsed / 390)
        else:
            avg_now_vol = cum_vol  # 无历史则保守处理
        rvol = cum_vol / avg_now_vol if avg_now_vol > 0 else 0
        cond_rvol = rvol >= 2.0

        # ── 条件2: VWAP ──
        df1["tp"]  = (df1["High"] + df1["Low"] + df1["Close"]) / 3
        df1["tpv"] = df1["tp"] * df1["Volume"]
        vwap = float(df1["tpv"].cumsum().iloc[-1] / df1["Volume"].cumsum().iloc[-1])
        cond_vwap = current_price > vwap

        # ── 条件3: EMA9斜率 ──
        closes5 = df5["Close"]
        ema9 = closes5.ewm(span=9, adjust=False).mean()
        cond_ema9 = float(ema9.iloc[-1]) > float(ema9.iloc[-2])

        # ── 条件4: MACD > 0 ──
        ema12 = closes5.ewm(span=12, adjust=False).mean()
        ema26 = closes5.ewm(span=26, adjust=False).mean()
        macd_line = float((ema12 - ema26).iloc[-1])
        cond_macd = macd_line > 0

        # ── 条件5: 现价 > 开盘价 ──
        cond_open = current_price > open_price

        all_pass = all([cond_rvol, cond_vwap, cond_ema9, cond_macd, cond_open])

        if not all_pass:
            return None

        return {
            "ticker":  ticker,
            "price":   round(current_price, 2),
            "open":    round(open_price, 2),
            "rvol":    round(rvol, 2),
            "vwap":    round(vwap, 2),
            "macd":    round(macd_line, 4),
            "ema9_up": cond_ema9,
            "source":  f"tech_scanner_{NODE_NAME}",
            "score":   7.5,
            "ts":      datetime.now(ET).isoformat()
        }
    except Exception as e:
        log.debug(f"{ticker} 分析失败: {e}")
        return None


# ── 冷却检查 ──────────────────────────────────────────────────
def check_cooldown(ticker: str) -> bool:
    with _cooldown_lock:
        last = _cooldown.get(ticker)
        if last and (datetime.now(ET) - last) < timedelta(minutes=COOLDOWN_MIN):
            return False
        _cooldown[ticker] = datetime.now(ET)
        return True


# ── 发信号 ────────────────────────────────────────────────────
def push_signal(sig: dict):
    try:
        r = requests.post(SIGNAL_URL, json=sig, timeout=5)
        log.info(f"✅ 信号发送: {sig['ticker']} RVOL={sig['rvol']} 价格={sig['price']} → {r.status_code}")
    except Exception as e:
        log.error(f"信号发送失败 {sig['ticker']}: {e}")


# ── 单轮扫描 ──────────────────────────────────────────────────
def run_scan(tickers: list[str]):
    if not is_market_open():
        log.info("非交易时间，跳过")
        return

    log.info(f"开始扫描 {len(tickers)} 只股票，{MAX_WORKERS} 线程并发...")
    t0 = time.time()
    signals = []

    # 分组
    groups = [tickers[i:i+50] for i in range(0, len(tickers), 50)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(analyze_ticker, tk): tk for group in groups for tk in group}
        for fut in as_completed(futures):
            result = fut.result()
            if result:
                ticker = result["ticker"]
                if check_cooldown(ticker):
                    signals.append(result)
                    push_signal(result)

    elapsed = round(time.time() - t0, 1)
    log.info(f"扫描完成，耗时 {elapsed}s，触发信号 {len(signals)} 个")


# ── 主循环 ────────────────────────────────────────────────────
def main():
    log.info(f"=== tech_scanner 启动 [{NODE_NAME}] ===")
    tickers = get_tickers(TICKER_SOURCE)
    if not tickers:
        log.error("股票池为空，退出")
        return

    while True:
        try:
            run_scan(tickers)
            # 每周一盘前重新加载股票池
            if datetime.now(ET).weekday() == 0 and datetime.now(ET).hour < 9:
                tickers = get_tickers(TICKER_SOURCE)
        except Exception as e:
            log.error(f"扫描异常: {e}")
        log.info(f"等待 {SCAN_INTERVAL}s 后下一轮...")
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
