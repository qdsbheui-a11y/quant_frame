@echo off
setlocal
cd /d "%~dp0"

set "DEFAULT_TEMPLATE=%~dp0my_bt_lab\app\configs\quant_lab_aliyun_ssh.yaml"
set "PY_EXE="

if exist ".venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0.venv\Scripts\python.exe"
) else (
    where py >nul 2>nul
    if not errorlevel 1 (
        echo Creating local virtual environment...
        py -3 -m venv .venv
        if exist ".venv\Scripts\python.exe" (
            set "PY_EXE=%~dp0.venv\Scripts\python.exe"
        )
    )
)

if not defined PY_EXE (
    echo Could not find a ready Python virtual environment.
    echo Please install Python for Windows first, then re-run this launcher.
    pause
    exit /b 1
)

echo Using Python: %PY_EXE%
echo Installing/updating required packages...
"%PY_EXE%" -m pip install --upgrade pip
"%PY_EXE%" -m pip install --upgrade -r requirements.txt
if errorlevel 1 (
    echo.
    echo Dependency installation failed.
    pause
    exit /b 1
)

set "MY_BT_LAB_DEFAULT_TEMPLATE=%DEFAULT_TEMPLATE%"
echo Loading template: %MY_BT_LAB_DEFAULT_TEMPLATE%
set "PGSSLMODE=disable"
echo Forcing PGSSLMODE=%PGSSLMODE% for SSH-tunneled quant_lab connection.
echo Starting quant_lab desktop workbench...
"%PY_EXE%" -m my_bt_lab.app.mt4_desktop

if errorlevel 1 (
    echo.
    echo Startup failed.
    pause
)

endlocal
