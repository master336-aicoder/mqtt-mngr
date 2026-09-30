#!/usr/bin/env bash
# ── 启动脚本 ──────────────────────────────────────────────
# 用法: bin/start.sh [--foreground] [--profile=file]
#
# 环境变量:
#   MNGR_PORT          服务端口 (默认 8090)
#   MNGR_DATA_DIR      数据目录 (默认 ./data)
#   MNGR_MASTER_KEY    凭据加密主密钥
#   MNGR_AUTH_TOKEN    静态 API Token (非空启用)
#   MNGR_JVM_OPTS      额外 JVM 参数
set -euo pipefail
cd "$(dirname "$0")/.."

APP_NAME="mngr-server"
JAR="target/${APP_NAME}.jar"
PID_FILE="data/${APP_NAME}.pid"
LOG_DIR="logs"
FOREGROUND=false
PROFILE=""

for arg in "$@"; do
  case "$arg" in
    --foreground) FOREGROUND=true ;;
    --profile=*)  PROFILE="${arg#--profile=}" ;;
    *) echo "未知参数: $arg"; exit 1 ;;
  esac
done

# ── 预检 ──
if [ ! -f "$JAR" ]; then
  echo "[ERROR] 未找到 ${JAR}，请先执行 bin/build.sh"
  exit 1
fi

if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "[WARN] 服务已在运行 (PID=${OLD_PID})，如需重启请使用 bin/restart.sh"
    exit 0
  else
    rm -f "$PID_FILE"
  fi
fi

mkdir -p "$LOG_DIR" "$(dirname "$PID_FILE")"

# ── 构建 JVM 参数 ──
PORT="${MNGR_PORT:-8090}"
DATA_DIR="${MNGR_DATA_DIR:-./data}"

JVM_OPTS="-Xms256m -Xmx512m"
JVM_OPTS="${JVM_OPTS} -XX:+UseG1GC"
JVM_OPTS="${JVM_OPTS} -XX:+HeapDumpOnOutOfMemoryError"
JVM_OPTS="${JVM_OPTS} -XX:HeapDumpPath=${LOG_DIR}/heapdump.hprof"
JVM_OPTS="${JVM_OPTS} -Dfile.encoding=UTF-8"
if [ -n "${MNGR_JVM_OPTS:-}" ]; then
  JVM_OPTS="${JVM_OPTS} ${MNGR_JVM_OPTS}"
fi

# ── 构建 Spring 参数 ──
SPRING_OPTS="--server.port=${PORT}"
SPRING_OPTS="${SPRING_OPTS} --mngr.data-dir=${DATA_DIR}"
if [ -n "${MNGR_MASTER_KEY:-}" ]; then
  SPRING_OPTS="${SPRING_OPTS} --mngr.master-key=${MNGR_MASTER_KEY}"
fi
if [ -n "${MNGR_AUTH_TOKEN:-}" ]; then
  SPRING_OPTS="${SPRING_OPTS} --mngr.auth.token=${MNGR_AUTH_TOKEN}"
fi
if [ -n "$PROFILE" ]; then
  SPRING_OPTS="${SPRING_OPTS} --spring.profiles.active=${PROFILE}"
fi

# ── 启动 ──
echo "════════════════════════════════════════"
echo "  启动 ${APP_NAME}"
echo "  端口: ${PORT}"
echo "  数据: ${DATA_DIR}"
echo "  日志: ${LOG_DIR}/"
echo "════════════════════════════════════════"

if [ "$FOREGROUND" = true ]; then
  exec java ${JVM_OPTS} -jar "$JAR" ${SPRING_OPTS}
else
  nohup java ${JVM_OPTS} -jar "$JAR" ${SPRING_OPTS} \
    > "${LOG_DIR}/stdout.log" 2>&1 &
  PID=$!
  echo "$PID" > "$PID_FILE"
  echo "[INFO] 服务已启动 (PID=${PID})"
  echo "[INFO] 日志: tail -f ${LOG_DIR}/stdout.log"

  # 等待启动确认
  for i in $(seq 1 30); do
    sleep 1
    if ! kill -0 "$PID" 2>/dev/null; then
      echo "[ERROR] 进程已退出，请检查 ${LOG_DIR}/stdout.log"
      rm -f "$PID_FILE"
      exit 1
    fi
    if curl -sf "http://localhost:${PORT}/actuator/health" >/dev/null 2>&1; then
      echo "[INFO] 服务就绪 ✓  http://localhost:${PORT}"
      exit 0
    fi
  done
  echo "[WARN] 30 秒内未通过健康检查，请手动确认"
fi
