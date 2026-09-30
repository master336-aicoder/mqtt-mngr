@echo off
REM ── 编译打包脚本 (Windows) ──────────────────────────────
REM 用法: bin\build.bat [--skip-test] [--clean]
setlocal enabledelayedexpansion
cd /d "%~dp0.."

set SKIP_TEST=
set CLEAN=

:parse_args
if "%~1"=="" goto :done_args
if "%~1"=="--skip-test" set SKIP_TEST=-DskipTests
if "%~1"=="--clean" set CLEAN=clean
shift
goto :parse_args
:done_args

echo ════════════════════════════════════════
echo   mqtt-mngr 编译打包
echo ════════════════════════════════════════
echo.

where mvn >nul 2>&1
if errorlevel 1 (
  echo [ERROR] 未找到 mvn，请确认 Maven 已安装并在 PATH 中
  exit /b 1
)

if "%CLEAN%"=="clean" (
  call mvn clean package %SKIP_TEST% -q
) else (
  call mvn package %SKIP_TEST% -q
)

if errorlevel 1 (
  echo [ERROR] 打包失败
  exit /b 1
)

if exist "target\mngr-server.jar" (
  echo.
  echo ════════════════════════════════════════
  echo   BUILD SUCCESS
  echo   产物: target\mngr-server.jar
  echo ════════════════════════════════════════
) else (
  echo [ERROR] 未找到 target\mngr-server.jar
  exit /b 1
)
