#!/bin/bash
# QuantForce Labs - 全节点自检自恢复脚本
TELEGRAM_TOKEN="7019506529:AAHGd21YXchNiqaJrMYAH7qNE3O7TmNRRB8"
TELEGRAM_CHAT_ID="6318635327"
LOG_FILE="/var/log/qf_healthcheck.log"
MAX_RESTART_ATTEMPTS=3
ALERT_COOLDOWN=300
COOLDOWN_DIR="/tmp/qf_alert_cooldown"

mkdir -p "$COOLDOWN_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

send_telegram() {
    local msg="$1"
    curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage" \
        -d chat_id="${TELEGRAM_CHAT_ID}" \
        -d text="${msg}" \
        -d parse_mode="HTML" \
        --max-time 10 > /dev/null 2>&1
}

can_alert() {
    local key="$1"
    local cooldown_file="$COOLDOWN_DIR/$key"
    if [ -f "$cooldown_file" ]; then
        local last=$(cat "$cooldown_file")
        local now=$(date +%s)
        if (( now - last < ALERT_COOLDOWN )); then
            return 1
        fi
    fi
    date +%s > "$cooldown_file"
    return 0
}

send_alert() {
    local node="$1" service="$2" status="$3"
    local key="${node}_${service}"; key="${key//[^a-zA-Z0-9_]/_}"
    if can_alert "$key"; then
        send_telegram "🚨 <b>QuantForce Alert</b>
节点: <code>${node}</code>
服务: <code>${service}</code>
状态: ${status}
时间: $(date '+%Y-%m-%d %H:%M:%S CDT')"
        log "ALERT: $node/$service - $status"
    fi
}

send_recovery() {
    local node="$1" service="$2"
    send_telegram "✅ <b>QuantForce Recovered</b>
节点: <code>${node}</code>
服务: <code>${service}</code>
状态: 已自动恢复
时间: $(date '+%Y-%m-%d %H:%M:%S CDT')"
    log "RECOVERY: $node/$service restarted successfully"
}

check_local_service() {
    local service="$1"
    if ! systemctl is-active --quiet "$service"; then
        log "FAIL: $service on .18, attempting restart..."
        send_alert ".18" "$service" "❌ 服务宕机，尝试重启"
        local attempt=0
        while (( attempt < MAX_RESTART_ATTEMPTS )); do
            (( attempt++ ))
            systemctl restart "$service"; sleep 3
            if systemctl is-active --quiet "$service"; then
                send_recovery ".18" "$service"; return 0
            fi
        done
        send_alert ".18_FAILED" "$service" "💀 重启失败(${MAX_RESTART_ATTEMPTS}次)，需人工介入"
    else
        log "OK: $service on .18"
    fi
}

check_remote_service() {
    local ip="$1" service="$2"
    local node=".${ip##*.}"
    local status
    status=$(ssh -o ConnectTimeout=5 -o BatchMode=yes heng@"$ip" \
        "systemctl is-active $service" 2>/dev/null)
    if [ "$status" != "active" ]; then
        log "FAIL: $service on $node, attempting restart..."
        send_alert "$node" "$service" "❌ 服务宕机，尝试重启"
        local attempt=0
        while (( attempt < MAX_RESTART_ATTEMPTS )); do
            (( attempt++ ))
            ssh -o ConnectTimeout=5 -o BatchMode=yes heng@"$ip" \
                "sudo systemctl restart $service" 2>/dev/null
            sleep 3
            status=$(ssh -o ConnectTimeout=5 -o BatchMode=yes heng@"$ip" \
                "systemctl is-active $service" 2>/dev/null)
            if [ "$status" = "active" ]; then
                send_recovery "$node" "$service"; return 0
            fi
        done
        send_alert "${node}_FAILED" "$service" "💀 重启失败(${MAX_RESTART_ATTEMPTS}次)，需人工介入"
    else
        log "OK: $service on $node"
    fi
}

check_node_reachable() {
    local ip="$1" node=".${ip##*.}"
    if ! ping -c 1 -W 3 "$ip" > /dev/null 2>&1; then
        log "UNREACHABLE: $node ($ip)"
        send_alert "$node" "NODE" "🔴 节点离线"
        return 1
    fi
    return 0
}

log "===== QuantForce Healthcheck Start ====="

check_local_service "signal_fusion"

check_node_reachable "192.168.0.11" && check_remote_service "192.168.0.11" "ib_executor"

if check_node_reachable "192.168.0.15"; then
    check_remote_service "192.168.0.15" "tech_scanner"
    check_remote_service "192.168.0.15" "news_scanner_v4"
fi

if check_node_reachable "192.168.0.101"; then
    check_remote_service "192.168.0.101" "sentry-watchdog"
fi

if check_node_reachable "192.168.0.102"; then
    check_remote_service "192.168.0.102" "email_listener"
    check_remote_service "192.168.0.102" "notify-worker"
    check_remote_service "192.168.0.102" "quant-courier"
fi

log "===== QuantForce Healthcheck Done ====="
