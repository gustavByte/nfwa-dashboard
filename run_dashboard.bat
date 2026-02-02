@echo off
setlocal
cd /d "%~dp0"

rem Starter NFWA-dashboard (lokal webside) og åpner nettleser automatisk.
rem Avslutt ved å lukke dette vinduet (Ctrl+C fungerer også).

py -m nfwa web
if %errorlevel% equ 0 goto end

python -m nfwa web
if %errorlevel% equ 0 goto end

rem Fallback: oppdater evt. denne stien dersom Python er installert et annet sted
"%LocalAppData%\Programs\Python\Python312\python.exe" -m nfwa web
if %errorlevel% equ 0 goto end

echo.
echo Kunne ikke starte dashboardet. Kontroller at Python er installert.
echo Tips: prøv i PowerShell:  python -m nfwa web
pause

:end
endlocal

