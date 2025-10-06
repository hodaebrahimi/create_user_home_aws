#!/bin/bash

# Exit on error (but we'll handle errors explicitly where needed)
set -e

# === DEBUG SECTION ===
DEBUG_LOG="$HOME/appstream_debug.log"
echo "DEBUG: Script execution started at $(date)" > "$DEBUG_LOG"
echo "DEBUG: Current user: $USER" >> "$DEBUG_LOG"
echo "DEBUG: Current directory: $(pwd)" >> "$DEBUG_LOG"
echo "DEBUG: Testing Python path..." >> "$DEBUG_LOG"
python3 --version >> "$DEBUG_LOG" 2>&1
echo "DEBUG: Python test exit code: $?" >> "$DEBUG_LOG"
# === END DEBUG SECTION ===

echo "Starting IBD Medical Imaging Annotation..."
echo "============================================"

# Configuration - UPDATE THESE VALUES AS NEEDED
BUCKET_NAME="hoda2-ibd-sample-cases-us-west-2"
PYTHON_SCRIPT_PATH="/opt/AnnotationApplication/create_user_home_aws/user_assignment_script.py"
COMPLETION_SYNC_SCRIPT="/opt/AnnotationApplication/create_user_home_aws/completion_sync.py"
PYTHON_EXE="python3"  # Use system python3
AWS_PROFILE="appstream_machine_role"

# === DETERMINE BASE DIRECTORY ===
# Try /home/appstream first (preferred), fallback to /opt/appstream
if [ -d "/home/appstream" ] && [ -w "/home/appstream" ]; then
    BASE_DIR="/home/appstream"
    echo "Using base directory: $BASE_DIR (writable home directory)"
elif [ -d "/opt/appstream" ] && [ -w "/opt/appstream" ]; then
    BASE_DIR="/opt/appstream"
    echo "Using base directory: $BASE_DIR (fallback location)"
else
    echo "ERROR: Neither /home/appstream nor /opt/appstream is available and writable"
    read -p "Press Enter to exit..."
    exit 1
fi
echo "Base directory: $BASE_DIR"
echo ""
# === END BASE DIRECTORY DETECTION ===

# === AWS PROFILE SETUP SECTION ===
echo "Setting up AWS profile..."
export AWS_PROFILE="$AWS_PROFILE"
export AWS_DEFAULT_REGION="us-west-2"

echo "Using AWS profile: $AWS_PROFILE"
echo "AWS region: $AWS_DEFAULT_REGION"

# Test AWS CLI access with profile
if ! aws s3 ls --profile "$AWS_PROFILE" > /dev/null 2>&1; then
    echo "⚠ WARNING: AWS profile '$AWS_PROFILE' not configured or inaccessible"
    echo "Attempting to continue with default credentials..."
fi

echo ""
# === END AWS PROFILE SETUP SECTION ===

# Get the current username
CURRENT_USER="$USER"
echo "Current user: $CURRENT_USER"

# Verify Python executable exists and is accessible
if ! command -v "$PYTHON_EXE" &> /dev/null; then
    echo "ERROR: Python executable not found: $PYTHON_EXE"
    echo "Please check the Python installation."
    read -p "Press Enter to exit..."
    exit 1
fi

# Test if we can actually run Python
if ! "$PYTHON_EXE" --version &> /dev/null; then
    echo "ERROR: Cannot execute Python: $PYTHON_EXE"
    echo "This may be a permissions issue."
    read -p "Press Enter to exit..."
    exit 1
fi

# Check if Python script exists
if [ ! -f "$PYTHON_SCRIPT_PATH" ]; then
    echo "ERROR: Python script not found at $PYTHON_SCRIPT_PATH"
    echo "Please check the script path and try again."
    read -p "Press Enter to exit..."
    exit 1
fi

# Install Python dependencies
echo "Installing Python dependencies..."
if pip3 install -r /opt/AnnotationApplication/ibd_labeling_local_1-main/requirements.txt &> /dev/null; then
    echo "Dependencies installed successfully"
else
    echo "WARNING: Some dependencies may not have installed correctly"
    echo "This may cause issues during execution"
fi

echo "Stopping any background S3 sync processes..."
pkill -f "python.*s3.*sync" 2>/dev/null || true
echo "Waiting for processes to terminate..."
sleep 3

# Run user assignment script
echo ""
echo "Running hybrid user assignment system..."
echo "Bucket: $BUCKET_NAME"
echo "AWS Profile: $AWS_PROFILE"
echo "Base Directory: $BASE_DIR"
echo ""

# Test S3 access - try with profile first, fall back to default credentials
echo "Testing S3 bucket access..."
TEST_OUTPUT=$(aws s3 ls s3://$BUCKET_NAME/ --profile "$AWS_PROFILE" 2>&1)
TEST_EXIT=$?

if [ $TEST_EXIT -ne 0 ]; then
    # Profile test failed, check if it's a profile issue or S3 issue
    if echo "$TEST_OUTPUT" | grep -q "could not be found\|profile.*not found"; then
        echo "⚠ AWS profile '$AWS_PROFILE' not found, trying default credentials..."
        # Try without profile
        if aws s3 ls s3://$BUCKET_NAME/ > /dev/null 2>&1; then
            echo "✓ S3 bucket access confirmed using default credentials"
            echo "Note: Python scripts will still attempt to use profile '$AWS_PROFILE'"
        else
            echo ""
            echo "ERROR: Cannot access S3 bucket: $BUCKET_NAME"
            echo ""
            echo "This usually means:"
            echo "1. No AWS credentials available (profile or IAM role)"
            echo "2. Insufficient S3 permissions"
            echo "3. Bucket name is incorrect or in wrong region"
            echo ""
            read -p "Press Enter to exit..."
            exit 1
        fi
    else
        echo ""
        echo "ERROR: Cannot access S3 bucket: $BUCKET_NAME"
        echo "Error details: $TEST_OUTPUT"
        echo ""
        read -p "Press Enter to exit..."
        exit 1
    fi
else
    echo "✓ S3 bucket access confirmed with profile: $AWS_PROFILE"
fi
echo ""

echo "Running Python script: $PYTHON_SCRIPT_PATH"
echo "Using Python executable: $PYTHON_EXE"

# Pass BASE_DIR to Python script via environment
export BASE_DIR="$BASE_DIR"

# Run the user assignment script and capture output
TEMP_OUTPUT=$(mktemp)
if ! "$PYTHON_EXE" "$PYTHON_SCRIPT_PATH" > "$TEMP_OUTPUT" 2>&1; then
    echo ""
    echo "ERROR: User assignment script failed with exit code $?"
    echo "Output from script:"
    cat "$TEMP_OUTPUT"
    echo ""
    echo "Press Enter to exit..."
    read
    rm -f "$TEMP_OUTPUT"
    exit 1
fi

# Script succeeded, show output
echo "Python script output:"
cat "$TEMP_OUTPUT"

# Extract the assigned user
ASSIGNED_USER=$(grep "^ASSIGNED_USER=" "$TEMP_OUTPUT" | cut -d'=' -f2)

# Clean up temporary file
rm -f "$TEMP_OUTPUT"

# Debug output
echo "DEBUG: Extracted ASSIGNED_USER as: \"$ASSIGNED_USER\""

# Verify we got a valid user assignment
if [ -z "$ASSIGNED_USER" ]; then
    echo "ERROR: Could not determine assigned user"
    read -p "Press Enter to exit..."
    exit 1
fi

# Set the user home directory based on assigned user and BASE_DIR
USER_HOME="$BASE_DIR/$ASSIGNED_USER"

# Create the user home directory if it doesn't exist
if [ ! -d "$USER_HOME" ]; then
    echo "Creating user home directory: $USER_HOME"
    mkdir -p "$USER_HOME"
fi

# Set environment variable for the application
export USER_HOME_DIR="$USER_HOME"

echo ""
echo "============================================"
echo "Assigned user: $ASSIGNED_USER"
echo "User home directory: $USER_HOME"
echo "Environment variable USER_HOME_DIR set to: $USER_HOME_DIR"

# Create necessary subdirectories
echo ""
echo "Creating application subdirectories..."
for subdir in ibd_root annotations temp; do
    if [ ! -d "$USER_HOME/$subdir" ]; then
        mkdir -p "$USER_HOME/$subdir"
        echo "Created: $USER_HOME/$subdir"
    fi
done

ASSIGNMENT_FILE="$USER_HOME/assignment_info.txt"
if [ ! -f "$ASSIGNMENT_FILE" ]; then
    echo "WARNING: Assignment file not found: $ASSIGNMENT_FILE"
    echo "Creating assignment file..."
    echo "$ASSIGNED_USER" > "$ASSIGNMENT_FILE"
    echo "$CURRENT_USER" >> "$ASSIGNMENT_FILE"
    echo "Created at: $(date)" >> "$ASSIGNMENT_FILE"
fi

# Change to application directory
echo ""
echo "Changing to application directory..."
APP_DIR="/opt/AnnotationApplication/ibd_labeling_local_1-main"

if ! cd "$APP_DIR" 2>/dev/null; then
    echo "ERROR: Could not change to application directory $APP_DIR"
    echo "Please verify the path exists."
    read -p "Press Enter to exit..."
    exit 1
fi

# Launch the Python application
echo ""
echo "============================================"
echo "Launching IBD Annotator..."
echo "Python: $PYTHON_EXE"
echo "Working directory: $(pwd)"
echo "Assigned User: $ASSIGNED_USER"
echo ""

# Function to handle sync
run_completion_sync() {
    echo ""
    echo "Step 3: Syncing completed cases..."
    echo "TRACE: starting completion sync..."
    
    # Check if completion sync script exists
    if [ ! -f "$COMPLETION_SYNC_SCRIPT" ]; then
        echo "ERROR: Completion sync script not found at $COMPLETION_SYNC_SCRIPT"
        echo "Please ensure the completion_sync.py script is in place"
        echo "Skipping sync for this session"
        return 999
    fi
    
    # Run the completion sync
    echo "Running completion sync script..."
    echo "Command: $PYTHON_EXE $COMPLETION_SYNC_SCRIPT $BUCKET_NAME $USER_HOME"
    
    "$PYTHON_EXE" "$COMPLETION_SYNC_SCRIPT" "$BUCKET_NAME" "$USER_HOME"
    local sync_exit=$?
    echo "TRACE: completion sync exited with $sync_exit"
    return $sync_exit
}

# Step 1: Run data preparation
echo "Step 1: Preparing segmentation data..."
echo "TRACE: about to run prep_seg_data.py"

# Pass the user output directory as environment variable
export USER_OUTPUT_DIR="$USER_HOME"

# Let prep_seg_data.py handle S3 vs mount detection automatically
set +e  # Don't exit on error for this section
"$PYTHON_EXE" prep_seg_data.py --parameter_file prep_seg.yaml --use-local-mount
PREP_EXIT=$?
set -e

if [ $PREP_EXIT -ne 0 ]; then
    echo "ERROR: Data preparation failed"
    echo "TRACE: ENTERED ERROR_EXIT_WITH_SYNC at $(date)"
    echo ""
    echo "ERROR: Data preparation failed, but still attempting completion sync..."
    
    # Still try to sync any existing completed cases
    run_completion_sync
    SYNC_EXIT_CODE=$?
    
    # Jump to error exit
    PYTHON_EXIT_CODE=$PREP_EXIT
    ERROR_MODE=1
else
    echo "TRACE: prep finished, continuing to Step 2"
    
    # Step 2: Launch main application
    echo ""
    echo "Step 2: Starting manual labeling interface..."
    echo "Python: $PYTHON_EXE"
    echo "Working directory: $(pwd)"
    echo "USER_HOME_DIR: $USER_HOME_DIR"
    echo "ASSIGNED_USER: $ASSIGNED_USER"
    echo "TRACE: launching main app..."
    
    set +e  # Don't exit on error
    "$PYTHON_EXE" ibd_manual_labeling_speedup.py > /tmp/gradio_output.log 2>&1 &
    GRADIO_PID=$!
    
    # Give Gradio time to start
    echo "Waiting for Gradio interface to start..."
    sleep 5
    
    # Find Chrome executable
    if command -v google-chrome &> /dev/null; then
        CHROME_CMD="google-chrome"
    elif command -v chrome &> /dev/null; then
        CHROME_CMD="chrome"
    elif command -v chromium &> /dev/null; then
        CHROME_CMD="chromium"
    else
        echo "WARNING: Chrome not found"
        CHROME_CMD=""
    fi
    
    # Open browser to Gradio (default port 7860)
    if [ -n "$CHROME_CMD" ]; then
        echo "Opening Chrome at http://127.0.0.1:7860"
        $CHROME_CMD --new-window http://127.0.0.1:7860 &
    else
        echo "Please open http://127.0.0.1:7860 manually in your browser"
    fi
    
    # Wait for Gradio to finish
    wait $GRADIO_PID
    PYTHON_EXIT_CODE=$?
    set -e
    
    echo "TRACE: main app exited with $PYTHON_EXIT_CODE"
    
    # Step 3: Sync completed cases (runs regardless of main app exit code)
    run_completion_sync
    SYNC_EXIT_CODE=$?
    
    ERROR_MODE=0
fi

# Report sync results
if [ "$SYNC_EXIT_CODE" -eq 0 ]; then
    echo "[+] Completion sync completed successfully"
else
    echo "[!] Completion sync finished with issues (exit code $SYNC_EXIT_CODE)"
fi

echo "TRACE: Reached exit code check section"
echo "DEBUG: About to check main app exit code"
echo "DEBUG: PYTHON_EXIT_CODE is: \"$PYTHON_EXIT_CODE\""
echo "DEBUG: SYNC_EXIT_CODE is: \"$SYNC_EXIT_CODE\""

# Check for success - both must be 0
if [ "$PYTHON_EXIT_CODE" -eq 0 ] && [ "$SYNC_EXIT_CODE" -eq 0 ]; then
    echo "DEBUG: Both succeeded - going to success"
    
    # SUCCESS SECTION
    echo ""
    echo "============================================"
    echo "IBD Annotator completed successfully!"
    echo "User: $ASSIGNED_USER"
    echo "User data available in: $USER_HOME"
    echo "Base directory: $BASE_DIR"
    echo "S3 bucket: $BUCKET_NAME"
    echo "AWS Profile: $AWS_PROFILE"
    echo "Main app exit code: $PYTHON_EXIT_CODE"
    echo "Sync exit code: $SYNC_EXIT_CODE"
    echo "============================================"
    
    # Always show completion sync summary before closing
    echo ""
    echo "COMPLETION SYNC SUMMARY:"
    echo "- All completed cases have been synced successfully"
    echo "- Check sync_tracking.json in user directory for details"
    
else
    echo "DEBUG: Something failed - going to error"
    
    # ERROR EXIT FINAL
    echo "TRACE: ENTERED ERROR_EXIT_FINAL at $(date)"
    echo ""
    echo "============================================"
    echo "An error occurred during execution."
    echo ""
    echo "Troubleshooting steps:"
    echo "1. Check AWS profile '$AWS_PROFILE' is configured"
    echo "2. Verify S3 bucket permissions for profile"
    echo "3. Ensure all required files are present"
    echo "4. Check Python environment setup"
    echo "5. Verify user assignment script is working"
    echo ""
    echo "Assigned user: $ASSIGNED_USER"
    echo "User home directory: $USER_HOME"
    echo "Base directory: $BASE_DIR"
    echo "S3 bucket: $BUCKET_NAME"
    echo "AWS Profile: $AWS_PROFILE"
    echo "Main app exit code: $PYTHON_EXIT_CODE"
    echo "Sync exit code: $SYNC_EXIT_CODE"
    echo "============================================"
    
    # Show what was synced even on error
    echo ""
    echo "COMPLETION SYNC STATUS:"
    if [ "$SYNC_EXIT_CODE" -eq 0 ]; then
        echo "- Completed cases were synced successfully"
    else
        echo "- Sync had issues or was skipped"
    fi
fi

echo ""
echo "Press Enter to close..."
read