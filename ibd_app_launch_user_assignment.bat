@echo off
setlocal enabledelayedexpansion
echo Starting IBD Medical Imaging Annotation...
echo ============================================

REM Configuration - UPDATE THESE VALUES AS NEEDED
set BUCKET_NAME=hoda2-ibd-sample-cases-us-west-2
set PYTHON_SCRIPT_PATH=C:\Scripts\user_assignment_script.py

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

REM Run user assignment script
echo.
echo Running user assignment system...
echo Bucket: %BUCKET_NAME%
echo.

REM Call the user assignment Python script and capture output
echo Running: python "%PYTHON_SCRIPT_PATH%"
python "%PYTHON_SCRIPT_PATH%" > temp_output.txt 2>&1

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
echo Assigned User: %ASSIGNED_USER%
echo.

REM Step 1: Run data preparation
echo Step 1: Preparing segmentation data...
echo TRACE: about to run prep_seg_data.py
"%PYTHON_EXE%" prep_seg_data.py --parameter_file prep_seg.yaml
echo TRACE: prep finished, continuing to Step 2

REM Step 2: Launch main application
echo.
echo Step 2: Starting manual labeling interface...
echo Python: %PYTHON_EXE%
echo Working directory: %CD%
echo USER_HOME_DIR: %USER_HOME_DIR%
echo ASSIGNED_USER: %ASSIGNED_USER%
echo TRACE: launching main app...
"%PYTHON_EXE%" ibd_manual_labeling_speedup.py
set "PYTHON_EXIT_CODE=%ERRORLEVEL%"
echo TRACE: main app exited with %PYTHON_EXIT_CODE%

if not "%PYTHON_EXIT_CODE%"=="0" (
    echo TRACE: goto ERROR_EXIT from MAIN (saved exit=%PYTHON_EXIT_CODE%)
    goto ERROR_EXIT
)

echo.
echo ============================================
echo IBD Annotator completed successfully!
echo User: %ASSIGNED_USER%
echo User data available in: %USER_HOME%
echo S3 bucket: %BUCKET_NAME%
echo ============================================
goto END

:ERROR_EXIT
echo TRACE: ENTERED ERROR_EXIT at %DATE% %TIME%
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
echo ============================================
echo.
echo Press any key to close...
pause > nul
exit /b 1

:END
echo.
echo Press any key to close...
pause > nul