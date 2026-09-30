@echo off
REM ── 健康检查脚本 (Windows) ──────────────────────────────
setlocal enabledelayedexpansion
cd /d "%~dp0.."

set APP_NAME=mngr-server
set PID_FILE=data\%APP_NAME%.pid
set PORT=%MNGR_PORT%
if "%PORT%"=="" set PORT=8090

if not exist "%PID_FILE%" (
  echo [DOWN] PID 文件不存在
  exit /b 2
)

set /p PID=<"%PID_FILE%"

curl -sf "http://localhost:%PORT%/actuator/health" >nul 2>&1
if errorlevel 1 (
  echo [WARN] PID=%PID%  端口=%PORT%  服务未就绪
  exit /b 1
)

for /f %%i in ('curl -sf "http://localhost:%PORT%/actuator/health"') do set BODY=%%i
echo [UP] PID=%PID%  port=%PORT%  status=%BODY%
exit /b 0
