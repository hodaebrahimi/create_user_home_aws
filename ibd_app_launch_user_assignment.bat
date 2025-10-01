@echo off
setlocal enabledelayedexpansion

REM === DEBUG SECTION ===
echo DEBUG: Batch execution started at %DATE% %TIME% > C:\temp\appstream_debug.log
echo DEBUG: Current user: %USERNAME% >> C:\temp\appstream_debug.log
echo DEBUG: Current directory: %CD% >> C:\temp\appstream_debug.log
echo DEBUG: Testing Python path... >> C:\temp\appstream_debug.log
"C:\MiniConda\miniconda3\python.exe" --version >> C:\temp\appstream_debug.log 2>&1
echo DEBUG: Python test exit code: %errorlevel% >> C:\temp\appstream_debug.log
REM === END DEBUG SECTION ===

echo Starting IBD Medical Imaging Annotation...
echo ============================================

REM Configuration - UPDATE THESE VALUES AS NEEDED
set BUCKET_NAME=hoda2-ibd-sample-cases-us-west-2
set PYTHON_SCRIPT_PATH=C:\Scripts\user_assignment_script.py
set COMPLETION_SYNC_SCRIPT=C:\Scripts\completion_sync.py
set PYTHON_EXE=C:\MiniConda\miniconda3\python.exe

REM Get the current username
set USERNAME=%USERNAME%
echo Current user: %USERNAME%

REM Verify Python executable exists and is accessible
if not exist "%PYTHON_EXE%" (
    echo ERROR: Python executable not found at %PYTHON_EXE%
    echo Please check the Python installation path.
    pause
    exit /b 1
)

REM Test if we can actually run Python
"%PYTHON_EXE%" --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Cannot execute Python at %PYTHON_EXE%
    echo This may be a permissions issue.
    pause
    exit /b 1
)

REM Check if Python script exists
if not exist "%PYTHON_SCRIPT_PATH%" (
    echo ERROR: Python script not found at %PYTHON_SCRIPT_PATH%
    echo Please check the script path and try again.
    pause
    exit /b 1
)

REM Install Python dependencies
echo Installing Python dependencies...
C:\MiniConda\miniconda3\Scripts\pip.exe install -r C:\Scripts\ibd_labeling_local_1-main\requirements.txt >nul 2>&1
if %errorlevel% neq 0 (
    echo WARNING: Some dependencies may not have installed correctly
    echo This may cause issues during execution
) else (
    echo Dependencies installed successfully
)

echo Stopping any background S3 sync processes...
taskkill /f /im python.exe >nul 2>&1
echo Waiting for processes to terminate...
for /L %%i in (1,1,3) do (
    echo.
)

REM Run user assignment script
echo.
echo Running hybrid user assignment system...
echo Bucket: %BUCKET_NAME%
echo.

REM Call the user assignment Python script and capture output
set "AWS_DEFAULT_REGION=us-west-2"

echo Running Python script: %PYTHON_SCRIPT_PATH%
echo Using Python executable: %PYTHON_EXE%
"%PYTHON_EXE%" "%PYTHON_SCRIPT_PATH%" > temp_output.txt 2>&1

REM Check the exit code
if %errorlevel% neq 0 (
    echo.
    echo ERROR: User assignment script failed with exit code %errorlevel%
    echo Output from script:
    type temp_output.txt
    echo.
    echo Press any key to exit...
    pause > nul
    del /f temp_output.txt 2>nul
    exit /b 1
)

REM Script succeeded, show output and extract assigned user
echo Python script output:
type temp_output.txt

REM Extract the assigned user using a simpler method
set ASSIGNED_USER=
for /f "tokens=*" %%i in (temp_output.txt) do (
    set "line=%%i"
    if "!line:~0,14!" == "ASSIGNED_USER=" (
        set "ASSIGNED_USER=!line:~14!"
    )
)

REM Clean up temporary file
del /f temp_output.txt 2>nul

REM Debug output
echo DEBUG: Extracted ASSIGNED_USER as: "%ASSIGNED_USER%"

REM Verify we got a valid user assignment
if not defined ASSIGNED_USER (
    echo ERROR: Could not determine assigned user
    pause
    exit /b 1
)

REM Set the user home directory based on assigned user
set USER_HOME=C:\AppStreamUsers\%ASSIGNED_USER%

REM Create the user home directory if it doesn't exist
if not exist "%USER_HOME%" (
    echo Creating user home directory: %USER_HOME%
    mkdir "%USER_HOME%"
)

REM Set environment variable for the application
set USER_HOME_DIR=%USER_HOME%

echo.
echo ============================================
echo Assigned user: %ASSIGNED_USER%
echo User home directory: %USER_HOME%
echo Environment variable USER_HOME_DIR set to: %USER_HOME_DIR%

REM Create necessary subdirectories
echo.
echo Creating application subdirectories...
if not exist "%USER_HOME%\ibd_root" (
    mkdir "%USER_HOME%\ibd_root"
    echo Created: %USER_HOME%\ibd_root
)

if not exist "%USER_HOME%\annotations" (
    mkdir "%USER_HOME%\annotations"
    echo Created: %USER_HOME%\annotations
)

if not exist "%USER_HOME%\temp" (
    mkdir "%USER_HOME%\temp"
    echo Created: %USER_HOME%\temp
)

REM Change to application directory
echo.
echo Changing to application directory...
cd /d C:\Scripts\ibd_labeling_local_1-main

if %errorlevel% neq 0 (
    echo ERROR: Could not change to application directory C:\Scripts\ibd_labeling_local_1-main
    echo Please verify the path exists.
    pause
    exit /b 1
)

REM Launch the Python application
echo.
echo ============================================
echo Launching IBD Annotator...
echo Python: %PYTHON_EXE%
echo Working directory: %CD%
echo Assigned User: %ASSIGNED_USER%
echo.

REM Step 1: Run data preparation
echo Step 1: Preparing segmentation data...
echo TRACE: about to run prep_seg_data.py

REM Pass the user output directory as environment variable
set "USER_OUTPUT_DIR=%USER_HOME%"

REM Let prep_seg_data.py handle S3 vs mount detection automatically
"%PYTHON_EXE%" prep_seg_data.py --parameter_file prep_seg.yaml --use-local-mount

if %errorlevel% neq 0 (
    echo ERROR: Data preparation failed
    goto ERROR_EXIT_WITH_SYNC
)

echo TRACE: prep finished, continuing to Step 2

REM Step 2: Launch main application
echo.
echo Step 2: Starting manual labeling interface...
echo Python: %PYTHON_EXE%
echo Working directory: %CD%
echo USER_HOME_DIR: %USER_HOME_DIR%
echo ASSIGNED_USER: %ASSIGNED_USER%
echo TRACE: launching main app...
"%PYTHON_EXE%" ibd_manual_labeling_speedup.py 2>&1
set "PYTHON_EXIT_CODE=%ERRORLEVEL%"
echo TRACE: main app exited with %PYTHON_EXIT_CODE%

REM Step 3: Sync completed cases (runs regardless of main app exit code)
echo.
echo Step 3: Syncing completed cases...
echo TRACE: starting completion sync...

REM Check if completion sync script exists
if not exist "%COMPLETION_SYNC_SCRIPT%" (
    echo ERROR: Completion sync script not found at %COMPLETION_SYNC_SCRIPT%
    echo Please ensure the completion_sync.py script is in place
    echo Skipping sync for this session
    goto SKIP_SYNC
)

REM Run the completion sync
echo Running completion sync script...
echo Command: "%PYTHON_EXE%" "%COMPLETION_SYNC_SCRIPT%" "%BUCKET_NAME%" "%ASSIGNED_USER%" "%USER_HOME%"

"%PYTHON_EXE%" "%COMPLETION_SYNC_SCRIPT%" "%BUCKET_NAME%" "%ASSIGNED_USER%" "%USER_HOME%"
set "SYNC_EXIT_CODE=%ERRORLEVEL%"
echo TRACE: completion sync exited with %SYNC_EXIT_CODE%
goto SYNC_DONE

:SKIP_SYNC
echo [!] Completion sync was skipped - script not found
set "SYNC_EXIT_CODE=999"

:SYNC_DONE
REM Report sync results
if "%SYNC_EXIT_CODE%"=="0" (
    echo [+] Completion sync completed successfully
) else (
    echo [!] Completion sync finished with issues (exit code %SYNC_EXIT_CODE%)
)

echo TRACE: Reached exit code check section
echo DEBUG: About to check main app exit code
echo DEBUG: PYTHON_EXIT_CODE is: "%PYTHON_EXIT_CODE%"
echo DEBUG: SYNC_EXIT_CODE is: "%SYNC_EXIT_CODE%"

REM Check for success - both must be 0
if "%PYTHON_EXIT_CODE%"=="0" (
    if "%SYNC_EXIT_CODE%"=="0" (
        echo DEBUG: Both succeeded - going to success
        goto SUCCESS_SECTION
    ) else (
        echo DEBUG: Sync failed - going to error
        goto ERROR_EXIT_FINAL
    )
) else (
    echo DEBUG: Main app failed - going to error  
    goto ERROR_EXIT_FINAL
)

REM This line should never execute
echo ERROR: Fell through exit code check - this should not happen!
goto ERROR_EXIT_FINAL

:SUCCESS_SECTION
echo.
echo ============================================
echo IBD Annotator completed successfully!
echo User: %ASSIGNED_USER%
echo User data available in: %USER_HOME%
echo S3 bucket: %BUCKET_NAME%
echo Main app exit code: %PYTHON_EXIT_CODE%
echo Sync exit code: %SYNC_EXIT_CODE%
echo ============================================

REM Always show completion sync summary before closing
echo.
echo COMPLETION SYNC SUMMARY:
if "%SYNC_EXIT_CODE%"=="0" (
    echo - All completed cases have been synced successfully
    echo - Check sync_tracking.json in user directory for details
) else (
    echo - Some issues occurred during sync (check logs above)
    echo - You may need to run sync manually later
)

goto END

:ERROR_EXIT_WITH_SYNC
echo TRACE: ENTERED ERROR_EXIT_WITH_SYNC at %DATE% %TIME%
echo.
echo ERROR: Data preparation failed, but still attempting completion sync...

REM Still try to sync any existing completed cases
if exist "%COMPLETION_SYNC_SCRIPT%" (
    echo Running emergency completion sync...
    "%PYTHON_EXE%" "%COMPLETION_SYNC_SCRIPT%" "%BUCKET_NAME%" "%ASSIGNED_USER%" "%USER_HOME%" 2>nul
)

goto ERROR_EXIT_FINAL

:ERROR_EXIT_FINAL
echo TRACE: ENTERED ERROR_EXIT_FINAL at %DATE% %TIME%
echo.
echo ============================================
echo An error occurred during execution.
echo.
echo Troubleshooting steps:
echo 1. Check AWS credentials are configured
echo 2. Verify S3 bucket permissions
echo 3. Ensure all required files are present
echo 4. Check Python environment setup
echo 5. Verify user assignment script is working
echo.
echo Assigned user: %ASSIGNED_USER%
echo User home directory: %USER_HOME%
echo S3 bucket: %BUCKET_NAME%
echo Main app exit code: %PYTHON_EXIT_CODE%
echo Sync exit code: %SYNC_EXIT_CODE%
echo ============================================

REM Show what was synced even on error
echo.
echo COMPLETION SYNC STATUS:
if "%SYNC_EXIT_CODE%"=="0" (
    echo - Completed cases were synced successfully
) else (
    echo - Sync had issues or was skipped
)

goto END

:END
echo.
echo Press any key to close...
pause > nul