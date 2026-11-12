@echo off
chcp 936 >nul
title XRouter 川流 - 卸载程序
set "DEST=%LOCALAPPDATA%\XRouter"

echo ==========================================
echo    XRouter·川流  卸载程序
echo ==========================================
echo.
if not exist "%DEST%" (
  echo   未检测到安装目录 %DEST%，可能已卸载。
  pause
  exit /b 0
)
echo   将停止服务、移除开机自启与桌面快捷方式，并删除：
echo     %DEST%
echo.
choice /c YN /n /m "  确认卸载？[Y=卸载 / N=取消] "
if errorlevel 2 exit /b 0

echo.
echo   [1/4] 停止服务 ...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python' -and $_.CommandLine -like '*llm_router.py*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }" >nul 2>nul

echo   [2/4] 移除开机自启 ...
reg delete "HKCU\Software\Microsoft\Windows\CurrentVersion\Run" /v XRouter /f >nul 2>nul

echo   [3/4] 移除桌面快捷方式 ...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$p=[Environment]::GetFolderPath('Desktop')+'\XRouter 川流.lnk'; if(Test-Path -LiteralPath $p){Remove-Item -LiteralPath $p -Force}" >nul 2>nul

echo   [4/4] 删除程序目录 ...
rd /s /q "%DEST%" >nul 2>nul
if exist "%DEST%" (
  echo   部分文件被占用未能删除，请重启后手动删除 %DEST%
) else (
  echo   卸载完成。
)
echo.
pause
exit /b 0