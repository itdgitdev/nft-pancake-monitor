@echo off
setlocal
cd /d "%~dp0"

call "%~dp002_CHECK_CONFIG.bat" --no-pause
if errorlevel 1 (
    echo.
    echo Loop start cancelled because the read-only check failed.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo LIVE AUTOMATION LOOP
echo Only one worker may use the same wallet.
echo Keep this computer awake and do not close this window.
echo Config: %CD%\my_rebalance_config.json
echo ==========================================================
set "CONFIRM="
set /p "CONFIRM=Type START to begin the live loop: "
if not "%CONFIRM%"=="START" (
    echo Loop start cancelled.
    pause
    exit /b 1
)

if not exist "latest_farms\logs" mkdir "latest_farms\logs"
set "PYTHON=%CD%\.venv\Scripts\python.exe"
set "CONFIG=%CD%\my_rebalance_config.json"
set "LOG=%CD%\latest_farms\logs\configured_rebalancer_loop.log"
set "PYTHONUNBUFFERED=1"

echo.
echo Enter the last 10 private-key characters when prompted.
echo The characters will not be displayed. This is normal.
echo To stop safely, wait for the current transaction receipt and press Ctrl+C once.
powershell -NoProfile -ExecutionPolicy Bypass -Command "& { & $env:PYTHON -m latest_farms.configured_pool_rebalancer.cli --config $env:CONFIG --execute --loop 2>&1 | Tee-Object -FilePath $env:LOG; exit $LASTEXITCODE }"
set "RESULT=%ERRORLEVEL%"

echo.
echo The loop stopped. Review the log before restarting it.
echo Log: %LOG%
pause
exit /b %RESULT%
