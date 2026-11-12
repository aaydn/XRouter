@echo off
chcp 936 >nul
setlocal
title XRouter 川流 - 安装程序
set "DEST=%LOCALAPPDATA%\XRouter"

echo ==========================================
echo    XRouter·川流  安装程序
echo ==========================================
echo   全程以当前用户身份运行，不需要管理员权限
echo.

rem ---------- 误用管理员运行时提醒 ----------
net session >nul 2>nul
if not errorlevel 1 (
  echo   [提醒] 检测到以管理员身份运行。程序会装到该管理员账户目录，
  echo          普通账户看不到。建议用日常账户双击本程序。
  echo.
  timeout /t 4 >nul
)

rem ---------- 已安装并在运行：直接打开 ----------
powershell -NoProfile -Command "try{Invoke-WebRequest 'http://127.0.0.1:3100/v1/models' -TimeoutSec 2 -UseBasicParsing ^| Out-Null; exit 0}catch{exit 1}" >nul 2>nul
if not errorlevel 1 (
  echo   检测到服务已在运行，直接打开界面。
  start "" http://127.0.0.1:3100/logs
  echo.
  pause
  exit /b 0
)

rem ---------- 检测 Python ----------
set "PY="
where python >nul 2>nul && set "PY=python"
if not defined PY (
  where py >nul 2>nul && set "PY=py -3"
)
if not defined PY (
  echo   [错误] 未检测到 Python 3.9 或更高版本。
  echo.
  echo   请先安装 Python（从 python.org 下载），安装时：
  echo     1) 勾选 Add Python to PATH
  echo     2) 选择 "Install for me only"（仅为我安装，无需管理员权限）
  echo   装好后重新双击本程序。
  echo.
  pause
  exit /b 1
)

rem ---------- 释放程序文件 ----------
echo   [1/4] 释放程序文件到 %DEST% ...
if not exist "%DEST%" mkdir "%DEST%" >nul 2>nul
set "TMPZ=%TEMP%\xrouter_payload.zip"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$t=[IO.File]::ReadAllText('%~f0',[Text.Encoding]::GetEncoding(936));$i=$t.LastIndexOf('#PAYLOAD#');$b=$t.Substring($i+9) -replace '[^A-Za-z0-9+/=]','';[IO.File]::WriteAllBytes('%TMPZ%',[Convert]::FromBase64String($b))"
if not exist "%TMPZ%" (
  echo   [错误] 安装包损坏（载荷解码失败），请重新下载。
  pause
  exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -LiteralPath '%TMPZ%' -DestinationPath '%DEST%' -Force"
del "%TMPZ%" >nul 2>nul

rem ---------- 依赖（全部装到用户目录，不需要管理员权限） ----------
echo   [2/4] 检查运行依赖 ...
%PY% -c "import fastapi,uvicorn,httpx" >nul 2>nul
if errorlevel 1 (
  echo         正在安装依赖 fastapi / uvicorn / httpx（用户目录，无需管理员）...
  %PY% -m pip install --quiet --user --disable-pip-version-check --no-warn-script-location fastapi uvicorn httpx
  if errorlevel 1 (
    echo         默认源失败，改用清华镜像重试 ...
    %PY% -m pip install --quiet --user --disable-pip-version-check --no-warn-script-location -i https://pypi.tuna.tsinghua.edu.cn/simple fastapi uvicorn httpx
  )
  %PY% -c "import fastapi,uvicorn,httpx" >nul 2>nul
  if errorlevel 1 (
    echo.
    echo   [错误] 依赖安装失败（可能无法访问网络或 Python 环境受限）。
    echo   可手动执行：%PY% -m pip install --user fastapi uvicorn httpx
    echo.
    pause
    exit /b 1
  )
)

rem ---------- 快捷方式 + 开机自启 ----------
echo   [3/4] 创建桌面快捷方式与开机自启 ...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$d='%DEST%';$w=New-Object -ComObject WScript.Shell;$s=$w.CreateShortcut([Environment]::GetFolderPath('Desktop')+'\XRouter 川流.lnk');$s.TargetPath=$d+'\launcher.bat';$s.WorkingDirectory=$d;$s.WindowStyle=7;$s.Description='XRouter 川流 LLM 分流器';$s.Save();New-ItemProperty -Path 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run' -Name 'XRouter' -Value ('\"'+$d+'\launcher.bat\"') -PropertyType String -Force | Out-Null" >nul 2>nul

rem ---------- 启动 ----------
echo   [4/4] 启动服务 ...
start "" /min "%DEST%\launcher.bat"
powershell -NoProfile -Command "for($i=0;$i -lt 20;$i++){try{Invoke-WebRequest 'http://127.0.0.1:3100/v1/models' -TimeoutSec 2 -UseBasicParsing ^| Out-Null; exit 0}catch{Start-Sleep -Milliseconds 800}}; exit 1" >nul 2>nul
if errorlevel 1 (
  echo         服务启动较慢或失败，请稍后双击桌面「XRouter 川流」。
) else (
  start "" http://127.0.0.1:3100/logs
)

echo.
echo ==========================================
echo   安装完成
echo   日志页：http://127.0.0.1:3100/logs
echo   设置页：http://127.0.0.1:3100/settings
echo   首次使用请在设置页配置上游接口与 API Key
echo   卸载：双击安装包中的「双击卸载.bat」
echo ==========================================
echo.
pause
exit /b 0