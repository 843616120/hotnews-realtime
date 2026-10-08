@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0start-kafka.ps1"
if errorlevel 1 (
    echo.
    echo Startup failed. Check the message above.
    pause
    exit /b 1
)
echo.
pause
