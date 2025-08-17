@echo off
setlocal enabledelayedexpansion
echo Starting IBD Medical Imaging Annotation...
echo ============================================

REM Configuration - UPDATE THESE VALUES AS NEEDED
set BUCKET_NAME=hoda2-ibd-sample-cases-us-west-2
set SYNC_DIRECTION=both
set PYTHON_SCRIPT_PATH=C:\Scripts\create_user_home.py

REM Get the current username
set USERNAME=%USERNAME%
echo Current user: %USERNAME%

REM Check if Python script exists
if not exist "%PYTHON_SCRIPT_PATH%" (
    echo ERROR: Python script not found at %PYTHON_SCRIPT_PATH%
    echo Please check the script path and try again.
    pause
    exit /b 1
)

REM Create user-specific home directory with S3 synchronization
echo.
echo Creating user workspace with S3 synchronization...
echo Bucket: %BUCKET_NAME%
echo Sync direction: %SYNC_DIRECTION%
echo.

REM Call the Python script with S3 parameters and capture output
echo Running: python "%PYTHON_SCRIPT_PATH%" "%USERNAME%" "%BUCKET_NAME%" "%SYNC_DIRECTION%"
python "%PYTHON_SCRIPT_PATH%" "%USERNAME%" "%BUCKET_NAME%" "%SYNC_DIRECTION%" > temp_output.txt 2>&1

REM Check the exit code
if %errorlevel% neq 0 (
    echo.
    echo ERROR: Python script failed with exit code %errorlevel%
    echo Output from script:
    type temp_output.txt
    echo.
    echo Press any key to continue with local-only setup...
    pause > nul
    
    REM Fallback to local-only setup
    echo.
    echo Attempting local-only setup...
    python "%PYTHON_SCRIPT_PATH%" "%USERNAME%" > temp_local.txt 2>&1
    
    if %errorlevel% neq 0 (
        echo ERROR: Could not create local user directory
        type temp_local.txt
        del /f temp_output.txt temp_local.txt 2>nul
        pause
        exit /b 1
    )
    
    REM Extract USER_HOME from local-only output using a loop
    set USER_HOME=
    for /f "tokens=*" %%i in (temp_local.txt) do (
        echo %%i | find "BATCH_USER_HOME=" >nul
        if !errorlevel! equ 0 (
            for /f "tokens=2 delims==" %%j in ("%%i") do set USER_HOME=%%j
        )
    )
    del /f temp_local.txt 2>nul
    
) else (
    REM Script succeeded, show output and extract USER_HOME
    type temp_output.txt
    
    REM Extract the home directory path using a loop to find BATCH_USER_HOME line
    set USER_HOME=
    for /f "tokens=*" %%i in (temp_output.txt) do (
        echo %%i | find "BATCH_USER_HOME=" >nul
        if !errorlevel! equ 0 (
            for /f "tokens=2 delims==" %%j in ("%%i") do set USER_HOME=%%j
        )
    )
    
    REM If that didn't work, try extracting from "User home path:" line
    if not defined USER_HOME (
        for /f "tokens=*" %%i in (temp_output.txt) do (
            echo %%i | find "User home path:" >nul
            if !errorlevel! equ 0 (
                for /f "tokens=4" %%j in ("%%i") do set USER_HOME=%%j
            )
        )
    )
)

REM Clean up temporary file
del /f temp_output.txt 2>nul

REM Verify we got a valid home directory
if not defined USER_HOME (
    echo ERROR: Could not determine user home directory
    pause
    exit /b 1
)

REM Set environment variable for the application
set USER_HOME_DIR=%USER_HOME%

echo.
echo ============================================
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

REM Set Python environment
set PYTHON_EXE=C:\Users\ImageBuilderAdmin\miniconda3\envs\appstream-deploy\python.exe

REM Verify Python executable exists
if not exist "%PYTHON_EXE%" (
    echo ERROR: Python executable not found at %PYTHON_EXE%
    echo Please check the Python installation path.
    pause
    exit /b 1
)

REM Launch the Python application
echo.
echo ============================================
echo Launching IBD Annotator...
echo Python: %PYTHON_EXE%
echo Working directory: %CD%
echo.

REM Step 1: Run data preparation
echo Step 1: Preparing segmentation data...
"%PYTHON_EXE%" prep_seg_data.py --parameter_file prep_seg.yaml

if %errorlevel% neq 0 (
    echo.
    echo ERROR: Data preparation failed (prep_seg_data.py)
    echo Check the prep_seg.yaml file and data paths.
    echo.
    goto ERROR_EXIT
)

echo Data preparation completed successfully.

REM Step 2: Launch main application
echo.
echo Step 2: Starting manual labeling interface...
"%PYTHON_EXE%" ibd_manual_labeling_speedup.py

if %errorlevel% neq 0 (
    echo.
    echo ERROR: Main application failed (ibd_manual_labeling_speedup.py)
    goto ERROR_EXIT
)

echo.
echo ============================================
echo IBD Annotator completed successfully!
echo User data synchronized with S3 bucket: %BUCKET_NAME%
echo ============================================
goto END

:ERROR_EXIT
echo.
echo ============================================
echo An error occurred during execution.
echo.
echo Troubleshooting steps:
echo 1. Check AWS credentials are configured
echo 2. Verify S3 bucket permissions
echo 3. Ensure all required files are present
echo 4. Check Python environment setup
echo.
echo User home directory: %USER_HOME%
echo S3 bucket: %BUCKET_NAME%
echo ============================================
echo.
echo Press any key to close...
pause > nul
exit /b 1

:END
echo.
echo Press any key to close...
pause > nul