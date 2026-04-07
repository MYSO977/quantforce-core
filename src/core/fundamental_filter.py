#!/usr/bin/env python3
"""
QuantForce_Labs — fundamental_filter.py
基本面筛查模块
位置: phi3 提取之后 → groq_decision 之前
数据: yfinance.info 预计算指标
缓存: ~/llm_data/fundamentals.json, TTL 7天
"""
import os, json, time, logging, threading
from pathlib import Path
from typing import Dict, Any
import yfinance as yf

# ── 配置 ─────────────────────────────────────
CACHE_FILE = Path(os.getenv("LLM_DATA", os.path.expanduser("~/llm_data"))) / "fundamentals.json"
CACHE_TTL  = int(os.getenv("FUND_CACHE_TTL", 7 * 24 * 3600))  # 默认7天

THRESHOLDS = {
    "rev_growth_min":    0.10,   # 营收增长 > 10% YoY
    "gross_margin_min":  0.30,   # 毛利率 > 30%
    "eps_growth_min":    0.0,    # EPS 加速 > 0
    "debt_equity_max":   200.0,  # debtToEquity 硬红线（百分比形式）
    "inst_pct_bonus":    0.50,   # 机构持仓 > 50% 加分
}

PENALTY = {
    "rev_growth":   0.6,
    "gross_margin": 0.4,
    "eps_growth":   0.5,
}

BASE_SCORE = 7.5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FUND] %(message)s")
log = logging.getLogger(__name__)

# ── 缓存 ─────────────────────────────────────
_cache: Dict = {}
_cache_lock  = threading.Lock()
_cache_dirty = False

def _load_cache():
    global _cache
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        if CACHE_FILE.exists():
            with open(CACHE_FILE) as f:
                _cache = json.load(f)
            log.info("cache loaded: " + str(len(_cache)) + " entries")
    except Exception as e:
        log.warning("cache load failed: " + str(e))
        _cache = {}

def _save_cache():
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(_cache, f, indent=2)
    except Exception as e:
        log.warning("cache save failed: " + str(e))

def _cache_valid(entry: Dict) -> bool:
    return bool(entry) and (time.time() - entry.get("ts", 0)) < CACHE_TTL

# ── 核心筛查 ─────────────────────────────────
def fundamental_filter(ticker: str, base_score: float = BASE_SCORE) -> Dict[str, Any]:
    """
    返回:
    {
        ticker, passed, position_scale, score, flags,
        details: {revenue_growth, gross_margin, eps_growth, debt_equity, inst_pct}
    }
    position_scale: 1.0 全仓 / 0.5 半仓 / 0.0 拦截
    """
    ticker = ticker.upper()

    # 初始化缓存
    if not _cache:
        _load_cache()

    # 读缓存
    with _cache_lock:
        cached = _cache.get(ticker, {})
        if _cache_valid(cached):
            log.debug("cache hit: " + ticker)
            return cached["result"]

    # 拉取数据
    log.info("fetching: " + ticker)
    try:
        info = yf.Ticker(ticker).info
        if not info or info.get("regularMarketPrice") is None:
            raise ValueError("empty info returned")
    except Exception as e:
        log.warning("yfinance failed for " + ticker + ": " + str(e))
        # API 失败 → 软降级，不硬拦
        result = _build_result(ticker, False, 0.5, base_score - 0.5,
                               ["api_error: " + str(e)[:60]], {})
        _write_cache(ticker, result)
        return result

    # 提取指标
    rev_growth   = float(info.get("revenueGrowth")          or 0)
    gross_margin = float(info.get("grossMargins")           or 0)
    eps_growth   = float(info.get("earningsGrowth")         or 0)
    debt_equity  = float(info.get("debtToEquity")           or 0)  # 百分比
    inst_pct     = float(info.get("heldPercentInstitutions")or 0)

    details = {
        "revenue_growth":  round(rev_growth   * 100, 1),
        "gross_margin":    round(gross_margin  * 100, 1),
        "eps_growth":      round(eps_growth    * 100, 1),
        "debt_equity":     round(debt_equity,         1),
        "inst_pct":        round(inst_pct      * 100, 1),
    }

    # 规则引擎
    flags    = []
    penalty  = 0.0
    hard_fail = False

    # 1. 营收增长
    if rev_growth > THRESHOLDS["rev_growth_min"]:
        flags.append("rev_growth " + str(details["revenue_growth"]) + "% OK")
    else:
        flags.append("rev_growth " + str(details["revenue_growth"]) + "% FAIL")
        penalty += PENALTY["rev_growth"]

    # 2. 毛利率
    if gross_margin > THRESHOLDS["gross_margin_min"]:
        flags.append("gross_margin " + str(details["gross_margin"]) + "% OK")
    else:
        flags.append("gross_margin " + str(details["gross_margin"]) + "% FAIL")
        penalty += PENALTY["gross_margin"]

    # 3. EPS 加速
    if eps_growth > THRESHOLDS["eps_growth_min"]:
        flags.append("eps_growth " + str(details["eps_growth"]) + "% OK")
    else:
        flags.append("eps_growth " + str(details["eps_growth"]) + "% FAIL")
        penalty += PENALTY["eps_growth"]

    # 4. 负债率 硬红线
    if debt_equity >= THRESHOLDS["debt_equity_max"]:
        hard_fail = True
        flags.append("DEBT_HARDLINE " + str(details["debt_equity"]) + "% BLOCKED")
    else:
        flags.append("debt_equity " + str(details["debt_equity"]) + "% OK")

    # 5. 机构持仓 加分
    if inst_pct > THRESHOLDS["inst_pct_bonus"]:
        penalty -= 0.2
        flags.append("inst_pct " + str(details["inst_pct"]) + "% BONUS")
    else:
        flags.append("inst_pct " + str(details["inst_pct"]) + "%")

    # 仓位决定
    score = max(0.0, base_score - penalty)

    if hard_fail:
        passed, scale = False, 0.0
    elif score >= 6.5:
        passed, scale = True, 1.0
    elif score >= 5.0:
        passed, scale = False, 0.5
    else:
        passed, scale = False, 0.0

    result = _build_result(ticker, passed, scale, score, flags, details)
    _write_cache(ticker, result)

    log.info(ticker + " -> scale=" + str(scale) + " score=" + str(round(score,2)) + " " + str(flags[:2]))
    return result

def _build_result(ticker, passed, scale, score, flags, details):
    return {
        "ticker":         ticker,
        "passed":         passed,
        "position_scale": scale,
        "score":          round(score, 2),
        "flags":          flags,
        "details":        details,
    }

def _write_cache(ticker, result):
    with _cache_lock:
        _cache[ticker] = {"ts": time.time(), "result": result}
    threading.Thread(target=_save_cache, daemon=True).start()

def clear_cache(ticker: str = None):
    """手动清缓存（测试/强制刷新）"""
    with _cache_lock:
        if ticker:
            _cache.pop(ticker.upper(), None)
        else:
            _cache.clear()
    _save_cache()

# ── CLI 测试 ─────────────────────────────────
if __name__ == "__main__":
    import sys
    tickers = sys.argv[1:] or ["AAPL", "NVDA", "PLTR", "TSLA", "COST"]
    for t in tickers:
        res = fundamental_filter(t)
        print("\n=== " + t + " ===")
        print("passed=" + str(res["passed"]) +
              "  scale=" + str(res["position_scale"]) +
              "  score=" + str(res["score"]))
        for f in res["flags"]:
            print("  " + f)
        if res["details"]:
            print("  details: " + str(res["details"]))
