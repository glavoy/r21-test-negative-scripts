@echo off
   setlocal enabledelayedexpansion
   set LOGFILE=C:\logs\r21_negative_%date:~-4,4%%date:~-10,2%%date:~-7,2%_%time:~0,2%%time:~3,2%%time:~6,2%.log
   set LOGFILE=%LOGFILE: =0%
   
   echo Running R21 Data Processing Pipeline... > "%LOGFILE%"
   python "D:\R21TestNegative\scripts\process_r21_data.py" >> "%LOGFILE%" 2>&1
   
   exit /b %errorlevel%