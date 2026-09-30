#!/usr/bin/env bash
# ── 编译打包脚本 ──────────────────────────────────────────
# 用法: bin/build.sh [--skip-test] [--clean]
set -euo pipefail
cd "$(dirname "$0")/.."

SKIP_TEST=""
CLEAN=""

for arg in "$@"; do
  case "$arg" in
    --skip-test) SKIP_TEST="-DskipTests" ;;
    --clean)     CLEAN="clean" ;;
    *) echo "未知参数: $arg"; exit 1 ;;
  esac
done

echo "════════════════════════════════════════"
echo "  mqtt-mngr 编译打包"
echo "════════════════════════════════════════"
echo ""

# ── 检查 JDK ──
if ! command -v mvn &>/dev/null; then
  echo "[ERROR] 未找到 mvn，请确认 Maven 已安装并在 PATH 中"
  exit 1
fi

JAVA_VER=$(java -version 2>&1 | head -1 | awk -F'"' '{print $2}' | cut -d. -f1)
echo "[INFO] Java 版本: ${JAVA_VER}"
echo "[INFO] Maven: $(mvn --version 2>&1 | head -1)"
echo ""

# ── 编译 ──
if [ -n "$CLEAN" ]; then
  echo ">> mvn clean package ${SKIP_TEST}"
  mvn clean package ${SKIP_TEST} -q
else
  echo ">> mvn package ${SKIP_TEST}"
  mvn package ${SKIP_TEST} -q
fi

# ── 产物 ──
JAR="target/mngr-server.jar"
if [ -f "$JAR" ]; then
  SIZE=$(du -h "$JAR" | cut -f1)
  echo ""
  echo "════════════════════════════════════════"
  echo "  BUILD SUCCESS"
  echo "  产物: ${JAR} (${SIZE})"
  echo "════════════════════════════════════════"
else
  echo "[ERROR] 打包失败，未找到 ${JAR}"
  exit 1
fi
