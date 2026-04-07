#!/bin/bash
# ============================================
# QuantForce_Labs 新系统环境检测脚本
# 用法: bash qf_check.sh
# ============================================

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PASS=0
FAIL=0

ok()   { echo -e "  ${GREEN}✓${NC} $1"; ((PASS++)); }
fail() { echo -e "  ${RED}✗${NC} $1"; ((FAIL++)); }
warn() { echo -e "  ${YELLOW}!${NC} $1"; }
section() { echo -e "\n${CYAN}▶ $1${NC}"; }

echo "============================================"
echo "  QuantForce_Labs 环境检测"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"

section "系统基础"
OS=$(lsb_release -d 2>/dev/null | cut -f2)
[[ "$OS" == *"Ubuntu"* ]] && ok "OS: $OS" || fail "OS异常: $OS"
IP=$(hostname -I | awk '{print $1}')
[[ "$IP" == "192.168.0.18" ]] && ok "IP: $IP (主节点)" || warn "IP: $IP (预期 192.168.0.18)"
timedatectl | grep -q "synchronized: yes" && ok "时间同步 NTP" || warn "NTP未同步"

section "Python 环境"
PY=$(python3 --version 2>/dev/null)
[[ -n "$PY" ]] && ok "$PY" || fail "python3 未安装"
pip3 --version &>/dev/null && ok "pip3 就绪" || fail "pip3 未安装"
for pkg in zmq flask requests pandas groq; do
    python3 -c "import $pkg" 2>/dev/null && ok "  pkg: $pkg" || fail "  pkg缺失: $pkg"
done

section "ZMQ 信号链端口"
for port in 5555 5556 5557; do
    ss -tlnp 2>/dev/null | grep -q ":$port" && ok "port $port 监听中" || warn "port $port 无监听"
done

section "六节点网络"
declare -A NODES=(["executor"]="192.168.0.11" ["compute"]="192.168.0.143" ["vision"]="192.168.0.15" ["sentry"]="192.168.0.101" ["courier"]="192.168.0.102")
for name in "${!NODES[@]}"; do
    ping -c1 -W1 "${NODES[$name]}" &>/dev/null && ok "$name (${NODES[$name]}) 可达" || fail "$name (${NODES[$name]}) 不可达"
done

section "Systemd 服务"
for svc in grafana-server quant-dispatcher quant-viz quant-signal; do
    systemctl is-active --quiet "$svc" 2>/dev/null && ok "$svc running" || warn "$svc 未运行"
done

section "环境配置"
[[ -f "$HOME/.quant_env" ]] && ok "~/.quant_env 存在" || fail "~/.quant_env 缺失"
if [[ -f "$HOME/.quant_env" ]]; then
    source "$HOME/.quant_env" 2>/dev/null
    [[ -n "$GROQ_API_KEY" ]] && ok "GROQ_API_KEY 已配置" || fail "GROQ_API_KEY 未设置"
fi

section "Grafana"
[[ -f "/var/lib/grafana/dashboards/ipad.json" ]] && ok "ipad.json 存在" || warn "ipad.json 缺失"
curl -s http://localhost:3000/api/health 2>/dev/null | grep -q "ok" && ok "Grafana API 正常" || warn "Grafana 未响应"

section "Ollama / LLM"
command -v ollama &>/dev/null && ok "ollama 已安装" || warn "ollama 未安装"
if command -v ollama &>/dev/null; then
    ollama list 2>/dev/null | grep -q "phi3:mini"     && ok "模型: phi3:mini"     || warn "模型缺失: phi3:mini"
    ollama list 2>/dev/null | grep -q "qwen2.5:0.5b" && ok "模型: qwen2.5:0.5b" || warn "模型缺失: qwen2.5:0.5b"
fi

section "QuantForce_Labs 项目"
QF="$HOME/QuantForce_Labs"
[[ -d "$QF" ]] && ok "项目目录存在" || warn "项目目录不存在"
for d in src strategies data configs logs; do
    [[ -d "$QF/$d" ]] && ok "  $d/" || warn "  $d/ 缺失"
done

echo ""
echo "============================================"
echo -e "  结果: ${GREEN}${PASS} 通过${NC}  ${RED}${FAIL} 失败${NC}"
echo "============================================"
[[ $FAIL -eq 0 ]] && echo -e "  ${GREEN}环境就绪！${NC}" || echo -e "  ${RED}请修复以上失败项。${NC}"
echo ""
