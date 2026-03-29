param(
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host "Installing build dependencies..."
& $PythonExe -m pip install --upgrade pyinstaller

Write-Host "Building PMJobHunter.exe ..."
& $PythonExe -m PyInstaller `
  --noconfirm `
  --clean `
  --onefile `
  --name PMJobHunter `
  --collect-all playwright `
  --collect-submodules uvicorn `
  --collect-submodules uvicorn.loops `
  --collect-submodules uvicorn.protocols `
  --collect-submodules uvicorn.lifespan `
  --collect-submodules anyio `
  --collect-submodules websockets `
  --collect-submodules watchfiles `
  --add-data "app/templates;app/templates" `
  app/desktop_launcher.py

Write-Host "Build complete. EXE path: $repoRoot\\dist\\PMJobHunter.exe"
