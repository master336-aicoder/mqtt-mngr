@echo off
REM ── 重启脚本 (Windows) ──────────────────────────────────
call "%~dp0stop.bat" %*
timeout /t 2 /nobreak >nul
call "%~dp0start.bat" %*
