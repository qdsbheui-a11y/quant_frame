@echo off
chcp 65001 >nul
setlocal

rem 切换到本 bat 所在目录。这里应当是包含 my_bt_lab 文件夹的项目根目录。
cd /d "%~dp0"

if not exist "my_bt_lab\app\simple_desktop.py" (
    echo [ERROR] 没找到 my_bt_lab\app\simple_desktop.py
    echo 请确认 run_simple_desktop.bat 放在 my_bt_lab 文件夹同级目录。
    echo 当前目录: %CD%
    pause
    exit /b 1
)

set "PYTHONUTF8=1"
set "PYTHONPATH=%CD%;%PYTHONPATH%"

echo ========================================
echo 启动量化回测助手 - DB Tick 普通版
echo 当前目录: %CD%
echo ========================================
echo.

where python >nul 2>nul
if errorlevel 1 (
    echo [ERROR] 系统找不到 python 命令。
    echo 请确认 Python 已安装，并已加入 PATH。
    pause
    exit /b 1
)

python -m my_bt_lab.app.simple_desktop
set "APP_EXIT_CODE=%ERRORLEVEL%"

if not "%APP_EXIT_CODE%"=="0" (
    echo.
    echo [ERROR] 程序异常退出，退出码: %APP_EXIT_CODE%
    echo 请检查上方日志，或在项目根目录手动执行:
    echo python -m my_bt_lab.app.simple_desktop
    pause
    exit /b %APP_EXIT_CODE%
)

endlocal
