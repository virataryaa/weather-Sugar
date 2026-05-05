@echo off
SET REPO="C:\Users\virat.arya\ETG\SoftsDatabase - Documents\Database\Hardmine\Non Fundamental\Weather\Sugar"
SET LOGFILE=%TEMP%\sugar_weather_update.log
SET TO=virat.arya@etgworld.com

cd /d %REPO%

echo [%date% %time%] Running daily update... > %LOGFILE%
python daily_update.py >> %LOGFILE% 2>&1
SET UPDATE_STATUS=%ERRORLEVEL%

echo. >> %LOGFILE%
echo [%date% %time%] Pushing to GitHub... >> %LOGFILE%
git add data\*.parquet >> %LOGFILE% 2>&1
git commit -m "Daily weather update %date%" >> %LOGFILE% 2>&1
git push origin main >> %LOGFILE% 2>&1
SET PUSH_STATUS=%ERRORLEVEL%

IF %UPDATE_STATUS%==0 (
    IF %PUSH_STATUS%==0 (
        SET SUBJECT=Sugar Weather: Update OK %date%
    ) ELSE (
        SET SUBJECT=Sugar Weather: Git Push FAILED %date%
    )
) ELSE (
    SET SUBJECT=Sugar Weather: Update FAILED -- Needs Intervention %date%
)

powershell -ExecutionPolicy Bypass -Command ^
  "$outlook = New-Object -ComObject Outlook.Application; ^
   $mail = $outlook.CreateItem(0); ^
   $mail.To = '%TO%'; $mail.Subject = '%SUBJECT%'; ^
   $body = Get-Content '%LOGFILE%' -Raw; ^
   $mail.Body = $body; $mail.Send(); ^
   Start-Sleep -Seconds 3; ^
   [System.Runtime.Interopservices.Marshal]::ReleaseComObject($outlook) | Out-Null"
