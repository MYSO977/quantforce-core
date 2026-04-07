# QuantForce-Labs

Distributed 6-node quantitative trading system

## Node Architecture

| Node | IP | Role | Services |
|------|-----|------|---------|
| Acer (main) | 192.168.0.18 | Orchestration | dispatcher, quant_api, groq_decision |
| Dell | 192.168.0.11 | Execution | ib_executor_v2, phi3_extractor |
| Lenovo | 192.168.0.143 | Compute | qwen_cleaner |
| Vision | 192.168.0.15 | News | news_scanner |
| Sentry | 192.168.0.101 | Sentry | monitoring |
| Courier | 192.168.0.102 | Notify | email_listener, notify_worker |

## Signal Pipeline

news_scanner(.15) -> llm_tasks DB -> qwen(.143) -> phi3(.11) -> groq(.18) -> dispatcher:5556 -> executor(.11)

## Strategy v3.3

- L1: Groq news score >= 7.5
- L2: 60min cooldown
- L3: RVOL >= 2.0
- L4: price > VWAP and MACD > 0

## Health Check

bash ~/qf_check.sh
