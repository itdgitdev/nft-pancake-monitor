@echo off
setlocal
cd /d "%~dp0"

call "%~dp002_CHECK_CONFIG.bat" --no-pause
if errorlevel 1 (
    echo.
    echo Live run cancelled because the read-only check failed.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo THIS IS A LIVE RUN AND MAY SEND REAL TRANSACTIONS
echo Config: %CD%\my_rebalance_config.json
echo Review the wallet, chain and pool shown in the check above.
echo ==========================================================
set "CONFIRM="
set /p "CONFIRM=Type LIVE to run exactly one live cycle: "
if not "%CONFIRM%"=="LIVE" (
    echo Live run cancelled.
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
powershell -NoProfile -ExecutionPolicy Bypass -Command "& { & $env:PYTHON -m latest_farms.configured_pool_rebalancer.cli --config $env:CONFIG --execute 2>&1 | Tee-Object -FilePath $env:LOG; exit $LASTEXITCODE }"
set "RESULT=%ERRORLEVEL%"

echo.
if "%RESULT%"=="0" (
    echo One live cycle finished. Review every result and transaction before starting the loop.
) else (
    echo The live cycle ended with an error. Do not start the loop.
)
echo Log: %LOG%
pause
exit /b %RESULT%
