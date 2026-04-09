#!/usr/bin/env python3
"""
signal_fusion.py
QuantForce Labs — 信号融合服务 (center .18)
架构：signals_raw → [四层过滤 + 共振判断] → signals_final → ZMQ PUSH → ib_executor

流程：
  1. 轮询 signals_raw WHERE status='pending'
  2. 按 ticker 分组，合并同一 ticker 的 news + tech 信号
  3. 执行 L1~L4 过滤（L4 由 tech 信号的 features 携带）
  4. 共振判断：news + tech 双信号得分更高
  5. 通过 → 写 signals_final + ZMQ PUSH 到 ib_executor(.11)
  6. 标记 signals_raw 为 processed
"""

import os
import sys
import json
from src.risk.risk_gate import RiskGate
risk_gate = RiskGate(os.path.expanduser('~/QuantForce_Labs/src/risk/risk_config.yaml'))
import time
import logging
import psycopg2
import psycopg2.extras
import zmq
from datetime import datetime, timedelta
import pytz

# ─────────────────────────────────────────
#  配置
# ─────────────────────────────────────────

PG_DSN = os.getenv(
    "QUANT_PG_DSN",
    "host=192.168.0.18 port=5432 dbname=quantforce user=heng password=YOUR_PG_PASSWORD"
)

ZMQ_PUSH_ADDR = "tcp://192.168.0.11:5558"   # ib_executor PULL 监听地址

POLL_INTERVAL   = 5          # 秒，轮询间隔
L1_SCORE_MIN    = 7.5        # L1: Groq news score 阈值
L2_COOLDOWN_MIN = 60         # L2: 同 ticker 冷却分钟数
L3_RVOL_MIN     = 2.0        # L3: 最低 RVOL

ET = pytz.timezone("America/New_York")

# ─────────────────────────────────────────
#  日志
# ─────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FUSION] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("signal_fusion")

# ─────────────────────────────────────────
#  ZMQ 初始化
# ─────────────────────────────────────────

_zmq_ctx    = zmq.Context()
_zmq_socket = _zmq_ctx.socket(zmq.PUSH)
_zmq_socket.connect(ZMQ_PUSH_ADDR)
log.info(f"ZMQ PUSH 已连接 → {ZMQ_PUSH_ADDR}")

# ─────────────────────────────────────────
#  冷却表（内存，重启清空）
# ─────────────────────────────────────────

_cooldown: dict[str, datetime] = {}

def check_l2_cooldown(ticker: str) -> bool:
    """L2: 60分钟冷却。True = 允许通过。"""
    last = _cooldown.get(ticker)
    if last and (datetime.now(ET) - last) < timedelta(minutes=L2_COOLDOWN_MIN):
        return False
    return True

def set_cooldown(ticker: str):
    _cooldown[ticker] = datetime.now(ET)

# ─────────────────────────────────────────
#  数据库操作
# ─────────────────────────────────────────

def get_pending_signals(conn) -> list[dict]:
    """读取所有 pending 信号，按 ticker 聚合。"""
    sql = """
        SELECT id, symbol AS ticker, signal_type, direction, importance,
               features, created_at
        FROM signals_raw
        WHERE status = 'pending'
          AND created_at > NOW() - INTERVAL '2 hours'
        ORDER BY ticker, created_at ASC
        FOR UPDATE SKIP LOCKED;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def mark_processed(conn, ids: list[int], status: str = "processed"):
    """批量标记 signals_raw 状态。"""
    if not ids:
        return
    sql = """
        UPDATE signals_raw
        SET status = %s, updated_at = NOW()
        WHERE id = ANY(%s);
    """
    with conn.cursor() as cur:
        cur.execute(sql, (status, ids))


def write_signals_final(conn, final: dict) -> int | None:
    """写入 signals_final，返回新行 id。"""
    sql = """
        INSERT INTO signals_final
            (ticker, direction, confidence, reason, features, created_at)
        VALUES
            (%s, %s, %s, %s, %s, NOW())
        RETURNING id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            final["ticker"],
            final["direction"],
            final["confidence"],
            final["reason"],
            json.dumps(final["features"]),
        ))
        row = cur.fetchone()
    return row[0] if row else None

# ─────────────────────────────────────────
#  四层过滤逻辑
# ─────────────────────────────────────────

def apply_filters(ticker: str, news_sigs: list, tech_sigs: list) -> dict | None:
    """
    对单个 ticker 执行 L1~L4 过滤。
    返回 final_signal dict 或 None（未通过）。
    """

    # ── L1: news score ≥ 7.5 ────────────────────────────────
    # 只要有一条 news 信号 score ≥ 7.5 即通过
    news_score = 0.0
    news_reason = ""
    if news_sigs:
        best_news = max(news_sigs, key=lambda s: s.get("importance", 0))
        f = best_news.get("features", {})
        news_score = float(f.get("score", best_news.get("importance", 0)) or 0)
        news_reason = f.get("reason", f.get("headline", ""))
        if news_score < L1_SCORE_MIN:
            log.debug(f"[L1❌] {ticker} news_score={news_score:.1f} < {L1_SCORE_MIN}")
            # 没有 news 信号时允许纯 tech 信号继续（L1 仅对 news 类型适用）
            if not tech_sigs:
                return None
    elif not tech_sigs:
        return None

    # ── L2: 60分钟冷却 ──────────────────────────────────────
    if not check_l2_cooldown(ticker):
        log.debug(f"[L2❌] {ticker} 冷却中")
        return None

    # ── L3: RVOL ≥ 2.0（从 tech 信号 features 读取）────────
    rvol = 0.0
    if tech_sigs:
        best_tech = max(tech_sigs, key=lambda s: s.get("importance", 0))
        f = best_tech.get("features", {})
        rvol = float(f.get("rvol", 0) or 0)
        if rvol < L3_RVOL_MIN:
            log.debug(f"[L3❌] {ticker} RVOL={rvol:.2f} < {L3_RVOL_MIN}")
            return None

    # ── L4: 强势多头排列（tech 信号携带 l4_pass）────────────
    l4_pass = False
    l4_features = {}
    if tech_sigs:
        best_tech = max(tech_sigs, key=lambda s: s.get("importance", 0))
        f = best_tech.get("features", {})
        l4_pass = bool(f.get("l4_pass", False))
        l4_features = {
            "price":      f.get("price"),
            "ema9":       f.get("ema9"),
            "vwap":       f.get("vwap"),
            "open_price": f.get("open_price"),
            "macd_line":  f.get("macd_line"),
            "macd_hist":  f.get("macd_hist"),
            "rvol":       f.get("rvol"),
        }
        if not l4_pass:
            log.debug(f"[L4❌] {ticker} 多头排列未成立 price={f.get('price')} ema9={f.get('ema9')} vwap={f.get('vwap')}")
            return None

    # ── 共振评分 ────────────────────────────────────────────
    # news + tech 双信号：confidence 更高
    has_news = bool(news_sigs) and news_score >= L1_SCORE_MIN
    has_tech = bool(tech_sigs) and l4_pass

    if has_news and has_tech:
        confidence = min(round((news_score / 10.0) * 0.6 + 0.4, 3), 1.0)
        reason = f"共振信号 news_score={news_score:.1f} rvol={rvol:.2f} L4✅"
    elif has_tech:
        confidence = round(min(rvol / 10.0 + 0.3, 0.75), 3)
        reason = f"纯技术信号 rvol={rvol:.2f} L4✅"
    elif has_news:
        confidence = round(news_score / 10.0 * 0.5, 3)
        reason = f"纯新闻信号 score={news_score:.1f}"
    else:
        return None

    final = {
        "ticker":     ticker,
        "direction":  "buy",
        "confidence": confidence,
        "reason":     reason,
        "features": {
            **l4_features,
            "news_score":  news_score,
            "news_reason": news_reason,
            "has_news":    has_news,
            "has_tech":    has_tech,
            "fusion_ts":   datetime.now(ET).isoformat(),
        },
    }

    log.info(f"[✅通过] {ticker} confidence={confidence} {reason}")
    return final

# ─────────────────────────────────────────
#  ZMQ 推送
# ─────────────────────────────────────────

def fetch_account_state(conn) -> dict:
    """从 account_state 表读最新一行供 risk_gate 使用"""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT nav, equity_start, day_pnl, total_exposure, cash, buying_power
                FROM account_state
                ORDER BY updated_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if not row:
                log.warning("[RISK] account_state 表为空，risk_gate 将保守拦截")
                return {}
            return dict(row)
    except Exception as e:
        log.error(f"[RISK] fetch_account_state 失败: {e}")
        return {}


def zmq_push(final_signal: dict, final_id: int):
    payload = {**final_signal, "signal_id": final_id}
    try:
        _zmq_socket.send_json(payload, zmq.NOBLOCK)
        log.info(f"📡 ZMQ推送 {final_signal['ticker']} id={final_id}")
    except zmq.Again:
        log.warning(f"ZMQ发送队列满，{final_signal['ticker']} 暂存PG等待executor拉取")

# ─────────────────────────────────────────
#  主循环
# ─────────────────────────────────────────

def fusion_cycle(conn):
    """单次融合周期。"""
    rows = get_pending_signals(conn)
    if not rows:
        return

    # 按 ticker 分组
    by_ticker: dict[str, dict] = {}
    for row in rows:
        t = row["ticker"]
        if t not in by_ticker:
            by_ticker[t] = {"news": [], "tech": [], "ids": []}
        by_ticker[t][row["signal_type"]].append(row)
        by_ticker[t]["ids"].append(row["id"])

    processed_ids = []
    skipped_ids   = []

    for ticker, group in by_ticker.items():
        final = apply_filters(ticker, group["news"], group["tech"])

        if final:
            final_id = write_signals_final(conn, final)
            if final_id:
                account = fetch_account_state(conn)
                risk_result = risk_gate.evaluate(final)
                if not risk_result.approved:
                    log.warning(f"[RISK❌] {final['ticker']} blocked: {risk_result.reason}")
                    skipped_ids.extend(group["ids"])
                    continue
                zmq_push(final, final_id)
                set_cooldown(ticker)
            processed_ids.extend(group["ids"])
        else:
            # 超过2小时的 pending 直接标记 skipped，避免积压
            for row in group["news"] + group["tech"]:
                age = datetime.now(pytz.utc) - row["created_at"].replace(tzinfo=pytz.utc)
                if age > timedelta(hours=2):
                    skipped_ids.append(row["id"])

    if processed_ids:
        mark_processed(conn, processed_ids, "processed")
    if skipped_ids:
        mark_processed(conn, skipped_ids, "skipped")

    conn.commit()

    if processed_ids:
        log.info(f"本轮处理 {len(by_ticker)} 只，通过 {len(processed_ids)} 条信号")


def main():
    log.info("=== signal_fusion 启动 ===")
    log.info(f"L1≥{L1_SCORE_MIN} | L2冷却{L2_COOLDOWN_MIN}min | L3 RVOL≥{L3_RVOL_MIN} | L4多头排列")

    while True:
        try:
            with psycopg2.connect(PG_DSN) as conn:
                while True:
                    try:
                        fusion_cycle(conn)
                    except psycopg2.OperationalError:
                        log.warning("PG连接断开，重新连接...")
                        break
                    except Exception as e:
                        log.error(f"fusion_cycle 异常: {e}", exc_info=True)
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    time.sleep(POLL_INTERVAL)

        except Exception as e:
            log.error(f"PG连接失败: {e}")
            time.sleep(15)


if __name__ == "__main__":
    main()
