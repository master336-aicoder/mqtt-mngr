#!/usr/bin/env bash
# ── 停止脚本 ──────────────────────────────────────────────
# 用法: bin/stop.sh [--force]
#
# 优先 graceful shutdown (SIGTERM + 等待)，--force 则 SIGKILL
set -euo pipefail
cd "$(dirname "$0")/.."

APP_NAME="mngr-server"
PID_FILE="data/${APP_NAME}.pid"
FORCE=false
TIMEOUT=30

for arg in "$@"; do
  case "$arg" in
    --force) FORCE=true ;;
    *) echo "未知参数: $arg"; exit 1 ;;
  esac
done

if [ ! -f "$PID_FILE" ]; then
  echo "[INFO] 未发现运行中的服务 (PID 文件不存在)"
  exit 0
fi

PID=$(cat "$PID_FILE")

if ! kill -0 "$PID" 2>/dev/null; then
  echo "[INFO] 进程 ${PID} 已不存在，清理 PID 文件"
  rm -f "$PID_FILE"
  exit 0
fi

echo ">> 停止服务 (PID=${PID}) ..."

if [ "$FORCE" = true ]; then
  kill -9 "$PID" 2>/dev/null || true
  rm -f "$PID_FILE"
  echo "[INFO] 已强制停止"
  exit 0
fi

# ── Graceful: 先尝试 actuator shutdown ──
PORT="${MNGR_PORT:-8090}"
curl -sf -X POST "http://localhost:${PORT}/actuator/shutdown" >/dev/null 2>&1 || true

# ── 等待退出 ──
for i in $(seq 1 $TIMEOUT); do
  if ! kill -0 "$PID" 2>/dev/null; then
    rm -f "$PID_FILE"
    echo "[INFO] 服务已停止"
    exit 0
  fi
  sleep 1
done

# ── 超时: SIGTERM ──
echo "[WARN] 超时 ${TIMEOUT}s，发送 SIGTERM ..."
kill "$PID" 2>/dev/null || true
sleep 3

if kill -0 "$PID" 2>/dev/null; then
  echo "[WARN] 仍未退出，发送 SIGKILL ..."
  kill -9 "$PID" 2>/dev/null || true
fi

rm -f "$PID_FILE"
echo "[INFO] 服务已停止"
