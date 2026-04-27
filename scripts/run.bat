@echo off
REM Launcher for EnLogosGRAG. Installs dependencies on first run, starts
REM the server in this window, and opens the browser once it's listening.
REM
REM Usage: scripts\run.bat
REM        scripts\run.bat --no-open        (skip the browser launch)
REM        set PORT=9000 && scripts\run.bat (override the listen port)

setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "REPO_DIR=%SCRIPT_DIR%.."
set "PORT_TO_OPEN=%PORT%"
if "%PORT_TO_OPEN%"=="" set "PORT_TO_OPEN=8780"

REM Verify Node is on PATH and >= 22 (for built-in node:sqlite).
where node >nul 2>nul
if errorlevel 1 (
    echo ERROR: Node.js is not on PATH. Install Node 22+ from https://nodejs.org/
    pause
    exit /b 1
)
for /f "tokens=*" %%V in ('node --version 2^>nul') do set "NODE_VER=%%V"
echo Node version: %NODE_VER%
REM Strip leading "v" and pull the major version number.
set "NODE_MAJOR=%NODE_VER:v=%"
for /f "tokens=1 delims=." %%M in ("%NODE_MAJOR%") do set "NODE_MAJOR=%%M"
if %NODE_MAJOR% LSS 22 (
    echo ERROR: Node 22+ is required for node:sqlite. Found %NODE_VER%.
    pause
    exit /b 1
)

REM Install dependencies on first run.
if not exist "%REPO_DIR%\node_modules" (
    echo Installing dependencies...
    pushd "%REPO_DIR%"
    call npm install
    if errorlevel 1 (
        echo ERROR: npm install failed.
        popd
        pause
        exit /b 1
    )
    popd
)

REM Open the browser after a short delay so the server has time to bind.
REM Skip if --no-open was passed.
if /i "%~1"=="--no-open" goto SERVE
start "" /b cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:%PORT_TO_OPEN%/"

:SERVE
echo Starting EnLogosGRAG on http://localhost:%PORT_TO_OPEN%/
echo Press Ctrl-C to stop.
pushd "%REPO_DIR%"
call npm start
set "EXIT_CODE=%ERRORLEVEL%"
popd
exit /b %EXIT_CODE%
