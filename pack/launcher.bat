@echo off
rem XRouter launcher: run silently with pythonw, fallback to python
cd /d "%~dp0"
where pythonw >nul 2>nul && (start "" /b pythonw "%~dp0llm_router.py" & exit /b 0)
start "" /b python "%~dp0llm_router.py"