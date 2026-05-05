$bat     = "C:\Users\virat.arya\ETG\SoftsDatabase - Documents\Database\Hardmine\Non Fundamental\Weather\Sugar\daily_push.bat"
$action  = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$bat`""
$trigger = New-ScheduledTaskTrigger -Daily -At 07:30AM
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable

Register-ScheduledTask `
    -TaskName "Sugar Weather Daily Update" `
    -Action   $action `
    -Trigger  $trigger `
    -Settings $settings `
    -Force

Write-Host "Task 'Sugar Weather Daily Update' registered — runs daily at 07:30 AM."
