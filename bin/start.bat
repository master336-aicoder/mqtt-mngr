@echo off
REM ── 启动脚本 (Windows) ──────────────────────────────────
REM 用法: bin\start.bat
setlocal enabledelayedexpansion
cd /d "%~dp0.."

set APP_NAME=mngr-server
set JAR=target\%APP_NAME%.jar
set PID_FILE=data\%APP_NAME%.pid
set LOG_DIR=logs
set PORT=%MNGR_PORT%
if "%PORT%"=="" set PORT=8090
set DATA_DIR=%MNGR_DATA_DIR%
if "%DATA_DIR%"=="" set DATA_DIR=.\data

if not exist "%JAR%" (
  echo [ERROR] 未找到 %JAR%，请先执行 bin\build.bat
  exit /b 1
)

if exist "%PID_FILE%" (
  set /p OLD_PID=<"%PID_FILE%"
  echo [WARN] 发现 PID 文件 (PID=%OLD_PID%)，请确认服务是否已运行
  echo        如需重启请使用 bin\restart.bat
  exit /b 0
)

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "data" mkdir "data"

set JVM_OPTS=-Xms256m -Xmx512m -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%LOG_DIR%\heapdump.hprof -Dfile.encoding=UTF-8
if defined MNGR_JVM_OPTS set JVM_OPTS=%JVM_OPTS% %MNGR_JVM_OPTS%

set SPRING_OPTS=--server.port=%PORT% --mngr.data-dir=%DATA_DIR%
if defined MNGR_MASTER_KEY set SPRING_OPTS=%SPRING_OPTS% --mngr.master-key=%MNGR_MASTER_KEY%
if defined MNGR_AUTH_TOKEN set SPRING_OPTS=%SPRING_OPTS% --mngr.auth.token=%MNGR_AUTH_TOKEN%

echo ════════════════════════════════════════
echo   启动 %APP_NAME%
echo   端口: %PORT%
echo   数据: %DATA_DIR%
echo ════════════════════════════════════════

start /b java %JVM_OPTS% -jar "%JAR%" %SPRING_OPTS%

timeout /t 3 /nobreak >nul

for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":%PORT% " ^| findstr "LISTENING"') do (
  echo %%a> "%PID_FILE%"
  echo [INFO] 服务已启动 (PID=%%a)
  goto :done
)

echo [WARN] 未能捕获 PID，请手动确认
:done
