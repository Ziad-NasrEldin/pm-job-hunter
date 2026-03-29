param(
    [string]$ProjectPath = (Resolve-Path "$PSScriptRoot\..").Path,
    [string]$PythonExe = "$ProjectPath\.venv\Scripts\python.exe"
)

if (!(Test-Path $PythonExe)) {
    Write-Error "Python executable not found at $PythonExe"
    exit 1
}

$collectAction = New-ScheduledTaskAction -Execute $PythonExe -Argument "-m app.cli collect" -WorkingDirectory $ProjectPath
$digestAction = New-ScheduledTaskAction -Execute $PythonExe -Argument "-m app.cli digest" -WorkingDirectory $ProjectPath

$collectTrigger = New-ScheduledTaskTrigger -Daily -At 9:00AM
$digestTrigger = New-ScheduledTaskTrigger -Daily -At 9:15AM

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U -RunLevel Limited

Register-ScheduledTask -TaskName "PMJobHunter-Collect" -Action $collectAction -Trigger $collectTrigger -Principal $principal -Force | Out-Null
Register-ScheduledTask -TaskName "PMJobHunter-Digest" -Action $digestAction -Trigger $digestTrigger -Principal $principal -Force | Out-Null

Write-Host "Scheduled tasks created: PMJobHunter-Collect and PMJobHunter-Digest"
