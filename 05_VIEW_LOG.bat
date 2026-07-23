@echo off
setlocal
cd /d "%~dp0"

set "LOG=%CD%\latest_farms\logs\configured_rebalancer_loop.log"
if not exist "%LOG%" (
    echo [FAIL] The log file does not exist yet.
    echo Run 03_RUN_ONE_CYCLE.bat or 04_RUN_LOOP.bat first.
    pause
    exit /b 1
)

start "Configured Rebalancer Log" powershell -NoProfile -NoExit -Command "Get-Content -LiteralPath '%LOG%' -Wait -Tail 100"
exit /b 0
