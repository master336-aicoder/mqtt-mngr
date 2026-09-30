#!/usr/bin/env bash
# ── 重启脚本 ──────────────────────────────────────────────
# 用法: bin/restart.sh [--profile=file]
set -euo pipefail
cd "$(dirname "$0")/.."

echo ">> 停止旧服务 ..."
bash "$(dirname "$0")/stop.sh" "$@"

echo ""
echo ">> 启动新服务 ..."
bash "$(dirname "$0")/start.sh" "$@"
