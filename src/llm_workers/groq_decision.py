#!/usr/bin/env python3
"""
QuantForce_Labs — groq_decision.py
节点: center (.18)  模型: llama-3.1-8b-instant (Groq)
职责: 拉取 signal_decision 任务，L1 评分 ≥7.5 才触发
      输出写入 signals_raw (signal_type='news')
      [v2] 集成 FundamentalFilter，score<6.0 直接拦截
"""
import psycopg2, json, time, logging, os, socket, uuid, hashlib
import sys
sys.path.insert(0, "/home/heng/QuantForce_Labs/src/risk")
sys.path.insert(0, "/home/heng/QuantForce_Labs/src/core")
from groq import Groq
from fundamental_filter import fundamental_filter as fund_check
from risk_gate import RiskGate

PG_CONF        = dict(host="192.168.0.18", port=5432, dbname="quantforce",
                      user="heng", password="Wh210712!")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY")
GROQ_MODEL     = "llama-3.1-8b-instant"
NODE_IP        = socket.gethostbyname(socket.gethostname())
POLL_SEC       = 5
LOCK_TTL       = 60
GROQ_TIMEOUT   = 10
L1_MIN_SCORE   = 7.5
RISK_CONFIG    = os.path.expanduser("~/QuantForce_Labs/src/risk/risk_config.yaml")
FUND_MIN_SCORE = 6.0
NEWS_EXPIRE_MIN = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GROQ_DECISION] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.expanduser("~/logs/groq_decision.log"))
    ]
)
log = logging.getLogger(__name__)
log.propagate = False
_risk_gate = RiskGate(RISK_CONFIG)

DECISION_PROMPT = """你是量化交易信号决策助手。
基于提供的金融事件 JSON，给出交易信号。
严格只输出 JSON，不要任何解释：
{
  "action": "BUY|SELL|HOLD",
  "ticker": "股票代码",
  "size": 0.05,
  "confidence": 0.0,
  "l1_score": 0.0,
  "reasoning": "简短理由（英文）",
  "risk_level": "high|medium|low",
  "source": "groq",
  "low_confidence": false
}
size 范围 0.01-0.10，confidence 范围 0.0-1.0，l1_score 范围 0-10。"""

def get_conn():
    return psycopg2.connect(**PG_CONF)

def claim_task(conn):
    cur = conn.cursor()
    cur.execute("""
        UPDATE llm_tasks SET status='pending', locked_by=NULL, locked_at=NULL
        WHERE task_type='signal_decision' AND status='pending'
          AND locked_by IS NOT NULL
          AND EXTRACT(EPOCH FROM (NOW() - locked_at)) > %s
    """, (LOCK_TTL,))
    cur.execute("""
        SELECT id, input_text, score FROM llm_tasks
        WHERE task_type='signal_decision'
          AND status='pending'
          AND locked_by IS NULL
          AND (score IS NULL OR score >= %s)
        ORDER BY score DESC, created_at ASC LIMIT 1
    """, (L1_MIN_SCORE,))
    row = cur.fetchone()
    if not row:
        conn.commit()
        return None, None, None
    task_id, input_text, score = row
    cur.execute("""
        UPDATE llm_tasks SET locked_by=%s, locked_at=NOW()
        WHERE id=%s AND locked_by IS NULL
    """, (NODE_IP, task_id))
    conn.commit()
    return task_id, input_text, score

def call_groq(client, text):
    t0 = time.time()
    resp = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": DECISION_PROMPT},
            {"role": "user",   "content": text[:3000]}
        ],
        max_tokens=512,
        temperature=0.1,
        timeout=GROQ_TIMEOUT
    )
    content = resp.choices[0].message.content.strip()
    latency = int((time.time() - t0) * 1000)
    if content.startswith("```"):
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]
    return json.loads(content), latency

def mark_fallback(conn, task_id, reason):
    cur = conn.cursor()
    cur.execute("""
        UPDATE llm_tasks
        SET status='fallback', node='groq',
            output_json=%s, locked_by=NULL, locked_at=NULL
        WHERE id=%s
    """, (json.dumps({"fallback_reason": reason}), task_id))
    conn.commit()
    log.warning(f"[{task_id}] → fallback  reason={reason}")

def mark_blocked(conn, task_id, reason):
    cur = conn.cursor()
    cur.execute("""
        UPDATE llm_tasks
        SET status='blocked', node='groq',
            output_json=%s, locked_by=NULL, locked_at=NULL
        WHERE id=%s
    """, (json.dumps({"blocked_reason": reason}), task_id))
    conn.commit()
    log.info(f"[{task_id}] → blocked  reason={reason}")

def extract_ticker(input_text):
    try:
        data = json.loads(input_text)
        return data.get("ticker") or data.get("symbol") or data.get("stock")
    except Exception:
        return None

def write_signal(conn, result, score, input_text):
    """写入 signals_raw"""
    action    = result.get("action", "HOLD").upper()
    ticker    = result.get("ticker", "")
    if not ticker or action == "HOLD":
        return
    direction  = "buy" if action == "BUY" else "sell"
    l1_score   = float(result.get("l1_score", score or 0))
    confidence = float(result.get("confidence", 0.5))
    importance = min(int(l1_score * 10), 100)
    fused_score = round(importance * confidence, 2)
    event_hash  = hashlib.sha256(
        (ticker + action + str(int(time.time() // 1800))).encode()
    ).hexdigest()
    signal_id   = str(uuid.uuid4())
    features    = {
        "l1_score":   l1_score,
        "size":       result.get("size", 0.05),
        "risk_level": result.get("risk_level", "medium"),
        "reasoning":  result.get("reasoning", ""),
        "source":     "groq",
        "input_text": input_text[:500],
    }
    cur = conn.cursor()
    # 防重：同一 event_hash 已存在则跳过
    cur.execute("SELECT id FROM signals_raw WHERE event_hash=%s", (event_hash,))
    if cur.fetchone():
        log.info(f"  → 信号已存在 event_hash={event_hash[:8]}，跳过")
        return
    cur.execute("""
        INSERT INTO signals_raw
            (signal_id, symbol, signal_type, direction, importance, confidence,
             score, source, pipeline, event_hash, features, expire_at, status)
        VALUES (%s,%s,'news',%s,%s,%s,%s,'groq_decision','news_v1',%s,%s,
                NOW() + INTERVAL '%s minutes','pending')
    """, (signal_id, ticker, direction, importance, confidence, fused_score,
          event_hash, json.dumps(features), NEWS_EXPIRE_MIN))
    conn.commit()
    log.info(f"  → signals_raw 写入: {ticker} {direction} importance={importance} confidence={confidence} score={fused_score}")

def process(conn, client, task_id, input_text, score):
    try:
        ticker = extract_ticker(input_text)
        if ticker:
            passed, fund_score, scale, reasons = _fund_filter.check(ticker)
            log.info(f"[{task_id}] FUND {ticker} score={fund_score:.1f} scale={scale} passed={passed}")
            if not passed or fund_score < FUND_MIN_SCORE:
                mark_blocked(conn, task_id,
                             f"fundamental score={fund_score:.1f} < {FUND_MIN_SCORE}")
                return
        else:
            scale = 1.0
            log.warning(f"[{task_id}] 无法提取 ticker，跳过基本面过滤")

        result, latency = call_groq(client, input_text)
        result["source"] = "groq"
        result["low_confidence"] = result.get("confidence", 1.0) < 0.6
        if scale < 1.0:
            original_size = result.get("size", 0.05)
            result["size"] = round(original_size * scale, 4)

        cur = conn.cursor()
        cur.execute("""
            UPDATE llm_tasks
            SET status='done', node=%s, model=%s, output_json=%s,
                score=%s, latency_ms=%s, locked_by=NULL, locked_at=NULL
            WHERE id=%s
        """, ("groq", GROQ_MODEL,
              json.dumps(result, ensure_ascii=False),
              result.get("l1_score", score), latency, task_id))
        conn.commit()
        log.info(f"[{task_id}] done  latency={latency}ms  action={result.get('action')}  "
                 f"confidence={result.get('confidence')}")

        # 写入 signals_raw
        write_signal(conn, result, score, input_text)

    except Exception as e:
        err = str(e)
        log.error(f"[{task_id}] Groq 调用失败: {err}")
        mark_fallback(conn, task_id, err)

def main():
    if not GROQ_API_KEY:
        log.error("GROQ_API_KEY 未设置，请检查 ~/.quant_env")
        return
    client = Groq(api_key=GROQ_API_KEY)
    log.info(f"Groq Decision 启动  node={NODE_IP}  model={GROQ_MODEL}  "
             f"L1_min={L1_MIN_SCORE}  FUND_min={FUND_MIN_SCORE}  db=PostgreSQL")
    while True:
        try:
            conn = get_conn()
            task_id, input_text, score = claim_task(conn)
            if task_id:
                log.info(f"[{task_id}] 认领任务  score={score}")
                process(conn, client, task_id, input_text or "", score or 0)
            conn.close()
        except Exception as e:
            log.error(f"主循环异常: {e}")
        time.sleep(POLL_SEC)

if __name__ == "__main__":
    os.makedirs(os.path.expanduser("~/logs"), exist_ok=True)
    main()
