#!/usr/bin/env python3
"""
QuantForce_Labs — qwen_cleaner.py
节点: compute (.143)  模型: qwen2.5:0.5b
职责: 拉取 news_clean 任务，做关键词打标/去噪/格式清洗
      完成后自动插入下游 event_extract 任务（流水线自驱动）
"""
import psycopg2, json, time, hashlib, requests, logging, os, socket

PG_CONF  = dict(host="192.168.0.18", port=5432, dbname="quantforce",
                user="heng", password="Wh210712!")
OLLAMA   = "http://localhost:11434"
MODEL    = "qwen2.5:0.5b"
NODE_IP  = socket.gethostbyname(socket.gethostname())
POLL_SEC = 5
LOCK_TTL = 120

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [QWEN_CLEANER] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.expanduser("~/logs/qwen_cleaner.log"))
    ]
)
log = logging.getLogger(__name__)

SYSTEM_PROMPT = """你是金融新闻清洗助手。
对输入的原始新闻文本执行：
1. 去除广告、模板、无关段落
2. 提取关键词标签（公司名、代码、事件类型）
3. 判断是否与美股市场相关（relevant: true/false）
4. 输出标准格式

严格只输出 JSON，不要任何解释：
{
  "relevant": true,
  "clean_text": "清洗后的正文",
  "tags": ["AAPL", "earnings", "beat"],
  "event_type": "earnings|macro|sector|other",
  "sentiment": "positive|negative|neutral"
}"""

def get_conn():
    return psycopg2.connect(**PG_CONF)

def claim_task(conn):
    cur = conn.cursor()
    cur.execute("""
        UPDATE llm_tasks SET status='pending', locked_by=NULL, locked_at=NULL
        WHERE task_type='news_clean' AND status='pending'
          AND locked_by IS NOT NULL
          AND EXTRACT(EPOCH FROM (NOW() - locked_at)) > %s
    """, (LOCK_TTL,))
    cur.execute("""
        SELECT id, input_text FROM llm_tasks
        WHERE task_type='news_clean' AND status='pending' AND locked_by IS NULL
        ORDER BY created_at ASC LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        conn.commit()
        return None, None
    task_id, input_text = row
    cur.execute("""
        UPDATE llm_tasks SET locked_by=%s, locked_at=NOW()
        WHERE id=%s AND locked_by IS NULL
    """, (NODE_IP, task_id))
    conn.commit()
    return task_id, input_text

def call_qwen(text):
    t0 = time.time()
    resp = requests.post(f"{OLLAMA}/api/chat", json={
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": text[:2000]}
        ],
        "stream": False,
        "options": {"temperature": 0.1, "num_predict": 512}
    }, timeout=30)
    resp.raise_for_status()
    content = resp.json()["message"]["content"].strip()
    latency = int((time.time() - t0) * 1000)
    if content.startswith("```"):
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]
    return json.loads(content), latency

def enqueue_event_extract(conn, task_id, clean_text, tags):
    h = hashlib.sha256(clean_text.encode()).hexdigest()
    cur = conn.cursor()
    cur.execute("SELECT id FROM llm_tasks WHERE input_hash=%s AND task_type='event_extract'", (h,))
    if cur.fetchone():
        return
    cur.execute("""
        INSERT INTO llm_tasks (task_type, input_hash, input_text, status)
        VALUES ('event_extract', %s, %s, 'pending')
    """, (h, clean_text))
    conn.commit()
    log.info(f"  → 投递 event_extract 任务 (hash={h[:8]})")

def process(conn, task_id, input_text):
    try:
        result, latency = call_qwen(input_text)
        cur = conn.cursor()
        cur.execute("""
            UPDATE llm_tasks
            SET status='done', node=%s, model=%s, output_json=%s,
                latency_ms=%s, locked_by=NULL, locked_at=NULL
            WHERE id=%s
        """, (NODE_IP, MODEL, json.dumps(result, ensure_ascii=False),
              latency, task_id))
        conn.commit()
        log.info(f"[{task_id}] done  latency={latency}ms  relevant={result.get('relevant')}  tags={result.get('tags')}")
        if result.get("relevant"):
            enqueue_event_extract(conn, task_id, result.get("clean_text", ""), result.get("tags", []))
    except Exception as e:
        log.error(f"[{task_id}] failed: {e}")
        cur = conn.cursor()
        cur.execute("""
            UPDATE llm_tasks
            SET status='failed', output_json=%s, locked_by=NULL, locked_at=NULL
            WHERE id=%s
        """, (json.dumps({"error": str(e)}), task_id))
        conn.commit()

def main():
    log.info(f"Qwen Cleaner 启动  node={NODE_IP}  model={MODEL}  db=PostgreSQL")
    try:
        r = requests.get(f"{OLLAMA}/api/tags", timeout=5)
        models = [m["name"] for m in r.json().get("models", [])]
        if not any(MODEL in m for m in models):
            log.warning(f"模型 {MODEL} 未找到，请先 ollama pull {MODEL}")
    except Exception as e:
        log.error(f"Ollama 连接失败: {e}")

    while True:
        try:
            conn = get_conn()
            task_id, input_text = claim_task(conn)
            if task_id:
                log.info(f"[{task_id}] 认领任务，文本长度={len(input_text or '')}")
                process(conn, task_id, input_text or "")
            conn.close()
        except Exception as e:
            log.error(f"主循环异常: {e}")
        time.sleep(POLL_SEC)

if __name__ == "__main__":
    os.makedirs(os.path.expanduser("~/logs"), exist_ok=True)
    main()
