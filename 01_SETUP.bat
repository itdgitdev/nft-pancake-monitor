@echo off
setlocal
cd /d "%~dp0"

echo ==============================================
echo Configured Pool Rebalancer - Setup
echo ==============================================

if not exist "requirements.txt" (
    echo [FAIL] requirements.txt was not found. Run this file from the extracted project folder.
    goto :failed
)
if not exist "latest_farms\configured_pool_rebalancer\cli.py" (
    echo [FAIL] The configured rebalancer module was not found. Do not move this BAT file.
    goto :failed
)

py -3.13 --version >nul 2>&1
if errorlevel 1 (
    echo [FAIL] Python 3.13 was not found.
    echo Install Python 3.13 from https://www.python.org/downloads/
    goto :failed
)
echo [PASS] Python 3.13 found

if not exist ".venv\Scripts\python.exe" (
    echo Creating the local Python environment...
    py -3.13 -m venv ".venv"
    if errorlevel 1 goto :failed
) else (
    echo [PASS] Local Python environment already exists
)

echo Updating pip...
".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 goto :failed

echo Installing project libraries. This can take several minutes...
".venv\Scripts\python.exe" -m pip install -r "requirements.txt"
if errorlevel 1 goto :failed

".venv\Scripts\python.exe" -m latest_farms.configured_pool_rebalancer.cli --help >nul
if errorlevel 1 goto :failed

echo.
echo SETUP COMPLETED
echo You can now close this window and continue with the guide.
pause
exit /b 0

:failed
echo.
echo SETUP FAILED. Read the [FAIL] message above before trying again.
pause
exit /b 1
