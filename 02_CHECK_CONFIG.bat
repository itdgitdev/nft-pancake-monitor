@echo off
setlocal
cd /d "%~dp0"

set "NO_PAUSE=0"
if /I "%~1"=="--no-pause" set "NO_PAUSE=1"

echo ==============================================
echo Configured Pool Rebalancer - Read-only Check
echo ==============================================

if not exist ".venv\Scripts\python.exe" (
    echo [FAIL] Python environment not found. Run 01_SETUP.bat first.
    set "RESULT=1"
    goto :finish
)
echo [PASS] Python environment

".venv\Scripts\python.exe" -m latest_farms.configured_pool_rebalancer.preflight --project-root "%CD%" --config "%CD%\my_rebalance_config.json"
set "RESULT=%ERRORLEVEL%"

:finish
if "%NO_PAUSE%"=="0" pause
exit /b %RESULT%
