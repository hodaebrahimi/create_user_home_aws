#!/usr/bin/env python3
"""
IBD Case Completion Sync Script for Linux/AppStream

Uses AWS profile 'appstream_machine_role' for S3 operations

Syncs completed cases to:
1. ~/MyFiles/HomeFolder/ (AppStream persistent storage) - PRIMARY
2. S3 bucket at ibd_root/{user}/ (if available) - SECONDARY

Structure in S3: ibd_root/user1/case_name/

Works with flexible base directories:
  - /home/appstream (preferred)
  - /opt/appstream (fallback)
"""

import os
import sys
import json
import shutil
from pathlib import Path
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import fnmatch

# AWS Profile configuration
AWS_PROFILE = 'appstream_machine_role'

def get_current_username():
    """
    Get the current username from AppStream environment
    Priority order:
    1. APPSTREAM_USER_NAME - actual user pool username (e.g., "hoda", "john")
    2. APPSTREAM_SAML_SUBJECT_NAME_ID - parsed from SAML (email format)
    3. APPSTREAM_USER_ID - unique user ID (fallback)
    4. System username (last resort)
    """
    username = os.environ.get('APPSTREAM_USER_NAME')
    
    if username:
        return username.lower()
    
    saml_subject = os.environ.get('APPSTREAM_SAML_SUBJECT_NAME_ID')
    if saml_subject:
        username = saml_subject.split('@')[0]
        return username.lower()
    
    username = os.environ.get('APPSTREAM_USER_ID')
    if username:
        return username.lower()
    
    username = (os.environ.get('USERNAME') or 
               os.environ.get('USER') or 
               'unknown_user')
    return username.lower()

def detect_base_directory(user_home_dir):
    """
    Detect which base directory we're using based on user_home_dir path
    Returns: Path object for base directory
    """
    user_path = Path(user_home_dir)
    
    # Check if we're under ~/MyFiles/HomeFolder (AppStream persistent storage)
    homefolder_base = Path.home() / "MyFiles" / "HomeFolder"
    if str(user_path).startswith(str(homefolder_base)):
        return homefolder_base
    
    # Check if we're under /opt/appstream (temporary fallback)
    if '/opt/appstream/' in str(user_path):
        return Path('/opt/appstream')
    
    # Default to parent of user_home_dir
    return user_path.parent

def get_assigned_user_from_folder(user_home_dir):
    """
    Get the assigned username from the folder's assignment_info.txt
    Returns: (username, user_folder_name) or (None, None)
    """
    assignment_file = Path(user_home_dir) / "assignment_info.txt"
    
    if not assignment_file.exists():
        print(f"[!] No assignment_info.txt found in {user_home_dir}")
        return None, None
    
    try:
        with open(assignment_file, 'r') as f:
            lines = f.read().strip().split('\n')
            # Format: line 0 = user folder (e.g., "user1")
            #         line 1 = username (e.g., "hoda")
            if len(lines) >= 2:
                user_folder = lines[0].strip()
                username = lines[1].strip().lower()
                print(f"[*] Found assignment: {username} → {user_folder}")
                return username, user_folder
    except Exception as e:
        print(f"[!] Error reading assignment file: {e}")
    
    return None, None


def initialize_s3_client_for_sync(bucket_name):
    """Initialize S3 client using AWS profile for sync operations"""
    try:
        print(f"[*] Initializing S3 client with AWS profile: {AWS_PROFILE}")
        region = os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')
        
        # Create boto3 session with profile
        session = boto3.Session(profile_name=AWS_PROFILE, region_name=region)
        s3_client = session.client('s3')
        
        # Quick connection test
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"[+] S3 connection established for sync")
        return s3_client
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"[!] S3 client initialization failed: {error_code}")
        return None
    except NoCredentialsError:
        print(f"[!] No AWS credentials found for profile: {AWS_PROFILE}")
        return None
    except Exception as e:
        if 'could not be found' in str(e).lower():
            print(f"[!] AWS profile '{AWS_PROFILE}' not found")
        else:
            print(f"[!] S3 connection failed: {e}")
        return None

def find_completed_cases(user_home_dir):
    """Find all cases marked as complete in the user directory"""
    user_path = Path(user_home_dir)
    completed_cases = []
    
    if not user_path.exists():
        print(f"[!] User directory not found: {user_path}")
        return completed_cases
    
    print(f"[*] Scanning for completed cases in: {user_path}")
    
    # Look for case directories with completion flag
    for item in user_path.iterdir():
        if item.is_dir():
            completion_flag = item / "01_labeling_complete.txt"
            if completion_flag.exists():
                completed_cases.append(item)
                print(f"  [+] Found completed case: {item.name}")
    
    print(f"[*] Found {len(completed_cases)} completed cases")
    return completed_cases

def get_sync_tracking_file(user_home_dir):
    """Get path to sync tracking file"""
    return Path(user_home_dir) / "sync_tracking.json"

def load_sync_tracking(sync_tracking_file):
    """Load sync tracking information"""
    if not sync_tracking_file.exists():
        return {}
    
    try:
        with open(sync_tracking_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"[!] Error loading sync tracking: {e}")
        return {}

def save_sync_tracking(sync_tracking_file, tracking_data):
    """Save sync tracking information"""
    try:
        sync_tracking_file.parent.mkdir(parents=True, exist_ok=True)
        with open(sync_tracking_file, 'w') as f:
            json.dump(tracking_data, f, indent=2)
        return True
    except Exception as e:
        print(f"[!] Error saving sync tracking: {e}")
        return False

def get_completion_files_to_sync(case_dir, assigned_user):
    """Get list of completion files to sync for a case"""
    case_name = case_dir.name
    
    # Files to sync when case is complete (excluding large original files)
    completion_patterns = [
        "01_labeling_complete.txt",           # Completion flag
        f"*_organs_*_ibd.nrrd",              # User segmentation files  
        f"{assigned_user}_organs_*_ibd.nrrd", # User-specific segmentations
        "*.yaml",                             # Configuration files
        "screenshot.png",                     # Screenshots
        "*_backup.nrrd",                     # Backup files
        "slicer.yaml"                        # Slicer configuration
    ]
    
    # Patterns to explicitly exclude
    exclude_patterns = [
        "intestine_train_*.nii.gz",          # Original training images
        "organs_*.nii.gz",                   # Original organ files
        "*.tmp",
        "*.temp",
        "02_synced_to_*"                     # Previous sync flags
    ]
    
    # Collect all files to potentially sync
    files_to_sync = []
    
    for pattern in completion_patterns:
        matching_files = list(case_dir.glob(pattern))
        files_to_sync.extend(matching_files)
    
    # Filter out excluded files
    filtered_files = []
    for file_path in files_to_sync:
        should_exclude = False
        for exclude_pattern in exclude_patterns:
            if fnmatch.fnmatch(file_path.name, exclude_pattern):
                should_exclude = True
                print(f"    [~] Excluding: {file_path.name}")
                break
        
        if not should_exclude and file_path.is_file():
            filtered_files.append(file_path)
    
    return filtered_files

def sync_case_to_persistent_storage(persistent_path, assigned_user, case_dir):
    """Sync completion artifacts to AppStream persistent storage"""
    case_name = case_dir.name
    persistent_case_path = persistent_path / assigned_user / case_name
    
    try:
        print(f"  [*] Syncing to persistent storage for case: {case_name}")
        
        # Create persistent case directory
        persistent_case_path.mkdir(parents=True, exist_ok=True)
        
        files_to_sync = get_completion_files_to_sync(case_dir, assigned_user)
        
        copied_files = 0
        for local_file_path in files_to_sync:
            persistent_file_path = persistent_case_path / local_file_path.name
            
            try:
                shutil.copy2(local_file_path, persistent_file_path)
                copied_files += 1
                print(f"    [+] Copied: {local_file_path.name}")
            except Exception as e:
                print(f"    [X] Failed to copy {local_file_path.name}: {e}")
        
        # Create completion timestamp
        if copied_files > 0:
            timestamp_file = persistent_case_path / "completion_sync_timestamp.txt"
            timestamp_content = (
                f"Case {case_name} completion artifacts synced by {assigned_user}\n"
                f"Timestamp: {datetime.now().isoformat()}\n"
                f"Files synced: {copied_files}\n"
                f"Sync target: AppStream Persistent Storage"
            )
            
            try:
                timestamp_file.write_text(timestamp_content)
                print(f"    [+] Created sync timestamp")
            except Exception as e:
                print(f"    [!] Could not create sync timestamp: {e}")
        
        print(f"  [+] Persistent storage sync completed: {copied_files} files")
        return copied_files > 0
        
    except Exception as e:
        print(f"  [X] Error syncing case {case_name} to persistent storage: {e}")
        return False

def sync_case_to_s3(s3_client, bucket_name, user_folder, case_dir, actual_username):
    """
    Sync completion artifacts for a specific case to S3 under ibd_root/
    
    Args:
        s3_client: boto3 S3 client
        bucket_name: S3 bucket name
        user_folder: folder name (e.g., "user1", "user2")
        case_dir: Path to case directory
        actual_username: actual user pool username for metadata
    """
    case_name = case_dir.name
    # Sync to ibd_root/{user}/{case}/ structure
    s3_case_prefix = f"ibd_root/{user_folder}/{case_name}/"
    
    uploaded_files = 0
    
    try:
        print(f"  [*] Syncing to S3 (ibd_root/{user_folder}/) for case: {case_name}")
        
        files_to_upload = get_completion_files_to_sync(case_dir, user_folder)
        
        # Upload filtered files
        for local_file_path in files_to_upload:
            relative_path = local_file_path.name
            s3_key = s3_case_prefix + relative_path
            
            try:
                # Add metadata to track upload
                metadata = {
                    'uploaded_by': actual_username,  # Changed from assigned_user
                    'upload_timestamp': datetime.now().isoformat(),
                    'case_name': case_name,
                    'sync_type': 'completion_artifacts',
                    'user_folder': user_folder  # Add this line
                }
                
                s3_client.upload_file(
                    str(local_file_path),
                    bucket_name,
                    s3_key,
                    ExtraArgs={
                        'Metadata': metadata,
                        'ContentType': 'application/octet-stream'
                    }
                )
                uploaded_files += 1
                print(f"    [+] Uploaded: {relative_path}")
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                print(f"    [X] Failed to upload {relative_path}: {error_code}")
            except Exception as e:
                print(f"    [X] Failed to upload {relative_path}: {e}")
        
        # Create completion timestamp in S3
        if uploaded_files > 0:
            timestamp_key = s3_case_prefix + "completion_sync_timestamp.txt"
            timestamp_content = (
                f"Case {case_name} completion artifacts synced by {actual_username}\n"
                f"Timestamp: {datetime.now().isoformat()}\n"
                f"Files synced: {uploaded_files}\n"
                f"S3 location: s3://{bucket_name}/{s3_case_prefix}"
            )
            
            try:
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=timestamp_key,
                    Body=timestamp_content,
                    ContentType='text/plain',
                    Metadata={
                        'completed_by': actual_username,
                        'sync_timestamp': datetime.now().isoformat(),
                        'files_synced': str(uploaded_files)
                    }
                )
                print(f"    [+] Created S3 sync timestamp")
            except Exception as e:
                print(f"    [!] Could not create S3 sync timestamp: {e}")
        
        print(f"  [+] S3 sync completed: {uploaded_files} files uploaded")
        return uploaded_files > 0
        
    except Exception as e:
        print(f"  [X] Error syncing case {case_name} to S3: {e}")
        return False

def sync_completed_cases(bucket_name, actual_username, user_home_dir):
    """Main function to sync all completed cases"""
    base_dir = detect_base_directory(user_home_dir)
    
    # ✅ ADD THIS - Get user folder from assignment_info.txt
    assigned_username, user_folder = get_assigned_user_from_folder(user_home_dir)
    
    if not user_folder:
        print("[X] Could not determine user folder from assignment_info.txt")
        return False
    
    print("=" * 60)
    print("   IBD CASE COMPLETION SYNC")
    print("=" * 60)
    print(f"Actual User: {actual_username}") 
    print(f"User Folder: {user_folder}")     
    print(f"Home Directory: {user_home_dir}")
    print(f"Base Directory: {base_dir}")
    print(f"S3 Bucket: {bucket_name}")
    print(f"S3 Path: ibd_root/{user_folder}/")
    print(f"AWS Profile: {AWS_PROFILE}")
    print("")
    
    # Check if we're running from HomeFolder (AppStream persistent storage)
    homefolder_path = Path.home() / "MyFiles" / "HomeFolder"
    running_from_homefolder = str(base_dir).startswith(str(homefolder_path))
    
    if running_from_homefolder:
        print("[*] Running from AppStream persistent storage (~/MyFiles/HomeFolder)")
        print("[*] HomeFolder automatically syncs to S3")
        print("[+] No manual sync needed - skipping all sync operations")
        print("")
        print("=" * 60)
        print("SYNC SKIPPED: Using persistent storage with automatic S3 sync")
        print("=" * 60)
        return True  # Return success since no sync is needed
    
    # Not in HomeFolder - we're using /opt/appstream fallback
    print("[*] Running from temporary storage (/opt/appstream)")
    print("[*] Will sync completed cases to S3")
    print("")
    
    # Find completed cases first
    completed_cases = find_completed_cases(user_home_dir)
    if not completed_cases:
        print("[*] No completed cases found - nothing to sync")
        return True
    
    # Initialize S3 client for sync (ONLY sync target when using /opt/appstream)
    s3_client = initialize_s3_client_for_sync(bucket_name)
    
    if not s3_client:
        print("[!] S3 not available - cannot sync from temporary storage")
        print("[!] Completed cases remain in local directory only")
        print("[!] WARNING: Data may be lost when AppStream session ends")
        return False
    
    print("[*] S3 sync enabled")
    sync_targets = [('s3', s3_client)]
    
    # Load sync tracking
    sync_tracking_file = get_sync_tracking_file(user_home_dir)
    sync_tracking = load_sync_tracking(sync_tracking_file)
    
    # Initialize sync tracking structure
    if 'sync_sessions' not in sync_tracking:
        sync_tracking['sync_sessions'] = []
    
    session_info = {
        'timestamp': datetime.now().isoformat(),
        'sync_targets': ['s3'],
        'total_cases': len(completed_cases),
        'aws_profile': AWS_PROFILE,
        'base_directory': str(base_dir),
        'storage_mode': 'temporary_with_s3_sync'
    }
    
    # Sync each completed case
    synced_cases = 0
    total_cases = len(completed_cases)
    
    for case_dir in completed_cases:
        case_name = case_dir.name
        
        # Check if already synced
        completion_flag = case_dir / "01_labeling_complete.txt"
        completion_time = completion_flag.stat().st_mtime if completion_flag.exists() else 0
        
        last_synced = sync_tracking.get(case_name, {}).get('last_synced_timestamp', 0)
        
        # Check for existing sync flag
        s3_sync_flag = case_dir / "02_synced_to_s3.txt"
        
        if completion_time <= last_synced and s3_sync_flag.exists():
            print(f"  [=] Case {case_name} already synced to S3")
            continue
        
        # Sync the case to S3
        print(f"  [*] Syncing case {synced_cases + 1}/{total_cases}: {case_name}")
        
        if sync_case_to_s3(s3_client, bucket_name, user_folder, case_dir, actual_username):
            s3_sync_flag.write_text(
                f"Synced to S3 (ibd_root/{user_folder}/) at {datetime.now().isoformat()}"
            )
            sync_tracking[case_name] = {
                'last_synced_timestamp': datetime.now().timestamp(),
                'last_synced_iso': datetime.now().isoformat(),
                'sync_successful': True,
                'sync_target': 's3'
            }
            synced_cases += 1
        else:
            sync_tracking[case_name] = {
                'last_synced_timestamp': sync_tracking.get(case_name, {}).get('last_synced_timestamp', 0),
                'last_attempted_iso': datetime.now().isoformat(),
                'sync_successful': False,
                'sync_target': 's3'
            }
    
    # Update session info
    session_info['synced_cases'] = synced_cases
    session_info['failed_cases'] = total_cases - synced_cases
    sync_tracking['sync_sessions'].append(session_info)
    sync_tracking['last_sync_targets'] = ['s3']
    
    # Save tracking information
    save_sync_tracking(sync_tracking_file, sync_tracking)
    
    print("")
    print("=" * 60)
    print(f"SYNC COMPLETED: {synced_cases}/{total_cases} cases synced to S3")
    print(f"Base Directory: {base_dir} (temporary)")
    print(f"S3 Path: s3://{bucket_name}/ibd_root/{user_folder}/")
    print(f"AWS Profile: {AWS_PROFILE}")
    print("=" * 60)
    
    return synced_cases > 0 or total_cases == 0

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python completion_sync.py <bucket_name> [user_home_dir]")
        print("Example: python completion_sync.py my-bucket /home/appstream/user1")
        print("")
        print(f"AWS Profile: {AWS_PROFILE}")
        print("Note: user_home_dir defaults to USER_HOME_DIR environment variable")
        print("      Works with both /home/appstream and /opt/appstream")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    
    # Get user_home_dir from command line or environment
    if len(sys.argv) > 2:
        user_home_dir = sys.argv[2]
    else:
        user_home_dir = os.environ.get('USER_HOME_DIR')
        if not user_home_dir:
            print("[X] No user home directory specified")
            print("[!] Pass as argument or set USER_HOME_DIR environment variable")
            sys.exit(1)
    
    print(f"[*] User home directory: {user_home_dir}")
    print(f"[*] AWS Profile: {AWS_PROFILE}")

    # Get the actual current username
    actual_username = get_current_username()
    print(f"[*] Current user: {actual_username}")
    
    # Read the assigned username from the local assignment file
    assigned_username, user_folder = get_assigned_user_from_folder(user_home_dir)
    
    if not assigned_username:
        print("[X] Could not determine assigned user from assignment_info.txt")
        print(f"[!] Expected file: {user_home_dir}/assignment_info.txt")
        print("[!] File should contain:")
        print("    Line 1: user folder name (e.g., user1)")
        print("    Line 2: username (e.g., hoda)")
        sys.exit(1)

    print(f"[+] Syncing for user: {actual_username}")
    print(f"[+] User folder: {user_folder}")
    
    try:
        success = sync_completed_cases(bucket_name, actual_username, user_home_dir)
        if success:
            print("[+] Completion sync finished successfully")
            sys.exit(0)
        else:
            print("[!] Completion sync finished with issues")
            sys.exit(1)
    except Exception as e:
        print(f"[X] Completion sync failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()