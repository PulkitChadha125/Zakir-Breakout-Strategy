@echo off
setlocal

cd /d "%~dp0"

if not exist ".venv\" (
    echo [Setup] .venv not found. Creating virtual environment...
    python -m venv .venv
    if errorlevel 1 (
        echo [Error] Failed to create .venv.
        exit /b 1
    )
) else (
    echo [Setup] .venv found.
)

echo [Setup] Installing dependencies from requirements.txt...
call ".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 (
    echo [Error] Failed to upgrade pip.
    exit /b 1
)

call ".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 (
    echo [Error] Failed to install dependencies.
    exit /b 1
)

echo [Run] Starting Delta Breakout Strategy...
call ".venv\Scripts\python.exe" main.py

endlocal
