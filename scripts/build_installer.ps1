param(
    [string]$PythonExe = "python",
    [string]$Version = "0.0.0",
    [string]$InnoCompiler = "iscc"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if ($Version.StartsWith("v")) {
    $Version = $Version.Substring(1)
}

$playwrightRuntimeDir = Join-Path $repoRoot "build\ms-playwright"
if (Test-Path $playwrightRuntimeDir) {
    Remove-Item -LiteralPath $playwrightRuntimeDir -Recurse -Force
}
New-Item -Path $playwrightRuntimeDir -ItemType Directory -Force | Out-Null

Write-Host "Building EXE..."
& powershell -ExecutionPolicy Bypass -File ".\scripts\build_exe.ps1" -PythonExe $PythonExe

Write-Host "Downloading Playwright Chromium runtime to installer payload..."
$env:PLAYWRIGHT_BROWSERS_PATH = $playwrightRuntimeDir
& $PythonExe -m playwright install chromium

if (-not (Test-Path ".\dist\PMJobHunter.exe")) {
    throw "Missing EXE at .\dist\PMJobHunter.exe"
}

Write-Host "Building installer with Inno Setup..."
& $InnoCompiler "/DAppVersion=$Version" "/DRepoRoot=$repoRoot" ".\installer\PMJobHunter.iss"

if (-not (Test-Path ".\dist\PMJobHunter-Setup.exe")) {
    throw "Missing installer at .\dist\PMJobHunter-Setup.exe"
}

Write-Host "Installer build complete: $repoRoot\dist\PMJobHunter-Setup.exe"
