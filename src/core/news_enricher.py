import requests
import logging
import time
import os
import hashlib
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
BASE = "https://finnhub.io/api/v1"

# 缓存：同一ticker 24h内只查一次基本面
_cache = {}

def _finnhub_get(endpoint, params):
    params["token"] = FINNHUB_API_KEY
    try:
        r = requests.get(BASE + endpoint, params=params, timeout=8)
        if r.status_code == 200:
            return r.json()
        log.warning(f"Finnhub {endpoint} {r.status_code}")
    except Exception as e:
        log.error(f"Finnhub error: {e}")
    return {}

def _get_profile(ticker):
    return _finnhub_get("/stock/profile2", {"symbol": ticker})

def _get_metric(ticker):
    return _finnhub_get("/stock/metric", {"symbol": ticker, "metric": "all"})

def _get_recommendation(ticker):
    data = _finnhub_get("/stock/recommendation", {"symbol": ticker})
    return data[0] if isinstance(data, list) and data else {}

def is_qualified(ticker):
    """排雷函数：通过返回True，排除返回False"""
    now = datetime.now()
    if ticker in _cache:
        result, ts = _cache[ticker]
        if (now - ts).seconds < 86400:
            return result

    profile = _get_profile(ticker)
    metric  = _get_metric(ticker)
    rec     = _get_recommendation(ticker)

    reasons = []

    # 1. 市值 >= $1B
    mktcap = profile.get("marketCapitalization", 0)
    if mktcap and mktcap < 1000:  # Finnhub 单位是百万USD
        reasons.append(f"mktcap={mktcap}M < $1B")

    # 2. 员工数 >= 500 (Premium字段，缺失则跳过)
    employees = profile.get("employeeTotal", profile.get("employees", None))
    if employees is not None and employees < 500:
        reasons.append(f"employees={employees} < 500")

    # 3. EPS 连续2季为负
    m = metric.get("metric", {})
    eps_list = [
        m.get("epsBasicExclExtraItemsAnnual"),
        m.get("epsBasicExclExtraItemsTTM"),
    ]
    eps_neg = sum(1 for e in eps_list if e is not None and e < 0)
    if eps_neg >= 2:
        reasons.append(f"EPS连续负: {eps_list}")

    # 4. D/E <= 2.5
    de = m.get("totalDebt/totalEquityQuarterly") or m.get("totalDebt/totalEquityAnnual")
    if de is not None and de > 2.5:
        reasons.append(f"D/E={de} > 2.5")

    # 5. 分析师评级 != Strong Sell
    strong_sell = rec.get("strongSell", 0)
    buy_total   = rec.get("buy", 0) + rec.get("strongBuy", 0)
    if strong_sell >= 3 and strong_sell > buy_total:
        reasons.append(f"strongSell={strong_sell}")

    # 6. 经营现金流 > 0
    cf = m.get("operatingCashFlowTTM") or m.get("freeCashFlowTTM")
    if cf is not None and cf <= 0:
        reasons.append(f"cashflow={cf} <= 0")

    passed = len(reasons) == 0
    if not passed:
        log.info(f"RECHECK FAIL {ticker}: {'; '.join(reasons)}")
    else:
        log.info(f"RECHECK PASS {ticker}: mktcap={mktcap}M employees={employees}")

    _cache[ticker] = (passed, now)
    return passed
