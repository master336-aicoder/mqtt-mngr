@echo off
REM ── 停止脚本 (Windows) ──────────────────────────────────
REM 用法: bin\stop.bat [--force]
setlocal enabledelayedexpansion
cd /d "%~dp0.."

set APP_NAME=mngr-server
set PID_FILE=data\%APP_NAME%.pid
set PORT=%MNGR_PORT%
if "%PORT%"=="" set PORT=8090

if not exist "%PID_FILE%" (
  echo [INFO] 未发现运行中的服务
  exit /b 0
)

set /p PID=<"%PID_FILE%"

taskkill /F /PID %PID% >nul 2>&1
if errorlevel 1 (
  echo [INFO] 进程 %PID% 已不存在
) else (
  echo [INFO] 服务已停止 (PID=%PID%)
)

del /f "%PID_FILE%" >nul 2>&1
