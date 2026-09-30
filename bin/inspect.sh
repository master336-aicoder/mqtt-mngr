#!/usr/bin/env bash
# ── 巡检脚本 ──────────────────────────────────────────────
# 用法: bin/inspect.sh
# 输出: 进程、端口、磁盘、JVM、数据库、日志错误、API 可用性
set -euo pipefail
cd "$(dirname "$0")/.."

APP_NAME="mngr-server"
PID_FILE="data/${APP_NAME}.pid"
PORT="${MNGR_PORT:-8090}"
DATA_DIR="${MNGR_DATA_DIR:-./data}"

echo "══════════════════════════════════════════════════════"
echo "  mqtt-mngr 巡检报告  $(date '+%Y-%m-%d %H:%M:%S')"
echo "══════════════════════════════════════════════════════"
echo ""

# ── 1. 进程状态 ──
echo "── 1. 进程状态 ──"
if [ -f "$PID_FILE" ]; then
  PID=$(cat "$PID_FILE")
  if kill -0 "$PID" 2>/dev/null; then
    echo "  状态: 运行中 (PID=${PID})"
    # 进程运行时长
    if command -v ps &>/dev/null; then
      ELAPSED=$(ps -p "$PID" -o etime= 2>/dev/null | tr -d ' ' || echo "N/A")
      echo "  运行时长: ${ELAPSED}"
    fi
  else
    echo "  状态: 已死 (PID=${PID} 不存在)"
  fi
else
  echo "  状态: 未启动 (无 PID 文件)"
fi
echo ""

# ── 2. 端口监听 ──
echo "── 2. 端口监听 ──"
if command -v netstat &>/dev/null; then
  LISTEN=$(netstat -ano 2>/dev/null | grep ":${PORT} " | grep -i "LISTEN" || true)
  if [ -n "$LISTEN" ]; then
    echo "  :${PORT} 已监听"
    echo "  ${LISTEN}" | head -3 | sed 's/^/    /'
  else
    echo "  :${PORT} 未监听"
  fi
elif command -v ss &>/dev/null; then
  LISTEN=$(ss -tlnp 2>/dev/null | grep ":${PORT} " || true)
  if [ -n "$LISTEN" ]; then
    echo "  :${PORT} 已监听"
    echo "  ${LISTEN}" | head -3 | sed 's/^/    /'
  else
    echo "  :${PORT} 未监听"
  fi
else
  echo "  (netstat/ss 不可用，跳过)"
fi
echo ""

# ── 3. 健康检查 ──
echo "── 3. 健康检查 ──"
HTTP_CODE=$(curl -so /dev/null -w '%{http_code}' "http://localhost:${PORT}/actuator/health" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
  BODY=$(curl -sf "http://localhost:${PORT}/actuator/health" 2>/dev/null)
  echo "  HTTP ${HTTP_CODE}: ${BODY}"
else
  echo "  HTTP ${HTTP_CODE}: 服务不可达"
fi
echo ""

# ── 4. 磁盘空间 ──
echo "── 4. 磁盘空间 ──"
if [ -d "$DATA_DIR" ]; then
  echo "  数据目录: ${DATA_DIR}"
  # 数据库文件大小
  DB_FILE="${DATA_DIR}/db/mngr.mv.db"
  if [ -f "$DB_FILE" ]; then
    DB_SIZE=$(du -h "$DB_FILE" | cut -f1)
    echo "  数据库文件: ${DB_SIZE}"
  fi
  DATA_SIZE=$(du -sh "$DATA_DIR" 2>/dev/null | cut -f1)
  echo "  数据目录总计: ${DATA_SIZE}"
  # 磁盘可用
  if command -v df &>/dev/null; then
    AVAIL=$(df -h "$DATA_DIR" 2>/dev/null | tail -1 | awk '{print $4}')
    echo "  磁盘可用: ${AVAIL}"
  fi
else
  echo "  数据目录不存在: ${DATA_DIR}"
fi
echo ""

# ── 5. 日志检查 ──
echo "── 5. 最近日志错误 ──"
LOG_FILE="logs/stdout.log"
if [ -f "$LOG_FILE" ]; then
  ERRORS=$(grep -c "ERROR" "$LOG_FILE" 2>/dev/null || echo "0")
  echo "  日志文件: ${LOG_FILE}"
  echo "  ERROR 总数: ${ERRORS}"
  echo "  最近 5 条 ERROR:"
  grep "ERROR" "$LOG_FILE" 2>/dev/null | tail -5 | sed 's/^/    /' || echo "    (无)"
else
  echo "  日志文件不存在: ${LOG_FILE}"
fi
echo ""

# ── 6. Prometheus 指标摘要 ──
echo "── 6. 关键指标 ──"
METRICS=$(curl -sf "http://localhost:${PORT}/actuator/prometheus" 2>/dev/null || true)
if [ -n "$METRICS" ]; then
  # JVM 内存
  JVM_MEM=$(echo "$METRICS" | grep "^jvm_memory_used_bytes{" | head -1 || true)
  if [ -n "$JVM_MEM" ]; then
    echo "  JVM 内存: ${JVM_MEM}"
  fi
  # HTTP 请求数
  HTTP_REQ=$(echo "$METRICS" | grep "^http_server_requests_seconds_count{" | head -3 || true)
  if [ -n "$HTTP_REQ" ]; then
    echo "  HTTP 请求:"
    echo "$HTTP_REQ" | sed 's/^/    /'
  fi
  # 订阅数
  SUB_COUNT=$(echo "$METRICS" | grep "^mngr_subscription" | head -3 || true)
  if [ -n "$SUB_COUNT" ]; then
    echo "  订阅指标:"
    echo "$SUB_COUNT" | sed 's/^/    /'
  fi
else
  echo "  (无法获取 Prometheus 指标)"
fi
echo ""

# ── 7. 数据库表统计 ──
echo "── 7. 业务数据概览 ──"
# 通过 API 获取
OVERVIEW=$(curl -sf "http://localhost:${PORT}/api/v1/public/overview" 2>/dev/null || true)
if [ -n "$OVERVIEW" ]; then
  echo "  ${OVERVIEW}" | head -1 | sed 's/^/    /'
else
  echo "  (API 不可达)"
fi
echo ""

echo "══════════════════════════════════════════════════════"
echo "  巡检完成"
echo "══════════════════════════════════════════════════════"
