#!/usr/bin/env bash
# ── 健康检查脚本 ──────────────────────────────────────────
# 用法: bin/health.sh
# 返回: 0=健康 1=异常 2=未运行
set -euo pipefail
cd "$(dirname "$0")/.."

APP_NAME="mngr-server"
PID_FILE="data/${APP_NAME}.pid"
PORT="${MNGR_PORT:-8090}"

# ── 进程检查 ──
if [ ! -f "$PID_FILE" ]; then
  echo "[DOWN] PID 文件不存在"
  exit 2
fi

PID=$(cat "$PID_FILE")
if ! kill -0 "$PID" 2>/dev/null; then
  echo "[DOWN] 进程 ${PID} 不存在"
  rm -f "$PID_FILE"
  exit 2
fi

# ── 健康检查 ──
HTTP_CODE=$(curl -so /dev/null -w '%{http_code}' "http://localhost:${PORT}/actuator/health" 2>/dev/null || echo "000")

if [ "$HTTP_CODE" = "200" ]; then
  BODY=$(curl -sf "http://localhost:${PORT}/actuator/health" 2>/dev/null)
  echo "[UP] PID=${PID}  port=${PORT}  status=${BODY}"
  exit 0
else
  echo "[WARN] PID=${PID}  port=${PORT}  http=${HTTP_CODE} (进程在但服务未就绪)"
  exit 1
fi
