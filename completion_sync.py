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
    """Get the current username from various sources"""
    username = (os.environ.get('USERNAME') or 
               os.environ.get('USER') or 
               os.environ.get('APPSTREAM_USER') or
               'unknown_user')
    return username.lower()

def detect_base_directory(user_home_dir):
    """
    Detect which base directory we're using based on user_home_dir path
    Returns: Path object for base directory
    """
    user_path = Path(user_home_dir)
    
    # Check if we're under /home/appstream
    if '/home/appstream/' in str(user_path):
        return Path('/home/appstream')
    
    # Check if we're under /opt/appstream
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

def check_appstream_persistent_storage():
    """Check if AppStream persistent storage is available"""
    # AppStream persistent storage location
    persistent_path = Path.home() / "MyFiles" / "HomeFolder"
    
    if persistent_path.exists() and persistent_path.is_dir():
        print(f"[+] AppStream persistent storage available at: {persistent_path}")
        return persistent_path
    
    print(f"[!] AppStream persistent storage not available at: {persistent_path}")
    return None

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

def sync_case_to_s3(s3_client, bucket_name, assigned_user, case_dir):
    """Sync completion artifacts for a specific case to S3 under ibd_root/"""
    case_name = case_dir.name
    # Sync to ibd_root/{user}/{case}/ structure
    s3_case_prefix = f"ibd_root/{assigned_user}/{case_name}/"
    
    uploaded_files = 0
    
    try:
        print(f"  [*] Syncing to S3 (ibd_root/{assigned_user}/) for case: {case_name}")
        
        files_to_upload = get_completion_files_to_sync(case_dir, assigned_user)
        
        # Upload filtered files
        for local_file_path in files_to_upload:
            relative_path = local_file_path.name
            s3_key = s3_case_prefix + relative_path
            
            try:
                # Add metadata to track upload
                metadata = {
                    'uploaded_by': assigned_user,
                    'upload_timestamp': datetime.now().isoformat(),
                    'case_name': case_name,
                    'sync_type': 'completion_artifacts'
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
                f"Case {case_name} completion artifacts synced by {assigned_user}\n"
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
                        'completed_by': assigned_user,
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

def sync_completed_cases(bucket_name, assigned_user, user_home_dir):
    """Main function to sync all completed cases"""
    base_dir = detect_base_directory(user_home_dir)
    
    print("=" * 60)
    print("   IBD CASE COMPLETION SYNC")
    print("=" * 60)
    print(f"User: {assigned_user}")
    print(f"Home Directory: {user_home_dir}")
    print(f"Base Directory: {base_dir}")
    print(f"S3 Bucket: {bucket_name}")
    print(f"S3 Path: ibd_root/{assigned_user}/")
    print(f"AWS Profile: {AWS_PROFILE}")
    print("")
    
    # Find completed cases first
    completed_cases = find_completed_cases(user_home_dir)
    if not completed_cases:
        print("[*] No completed cases found - nothing to sync")
        return True
    
    # Check sync targets (priority order)
    print("[*] Checking sync targets...")
    
    # 1. AppStream persistent storage (PRIMARY)
    persistent_path = check_appstream_persistent_storage()
    
    # 2. S3 (SECONDARY)
    s3_client = initialize_s3_client_for_sync(bucket_name)
    
    # Determine sync strategy
    sync_targets = []
    if persistent_path:
        sync_targets.append(('persistent', persistent_path))
    if s3_client:
        sync_targets.append(('s3', s3_client))
    
    if not sync_targets:
        print("[!] No sync targets available")
        print("[!] Completed cases remain in local directory only")
        return False
    
    print(f"[*] Active sync targets: {[t[0] for t in sync_targets]}")
    
    # Load sync tracking
    sync_tracking_file = get_sync_tracking_file(user_home_dir)
    sync_tracking = load_sync_tracking(sync_tracking_file)
    
    # Initialize sync tracking structure
    if 'sync_sessions' not in sync_tracking:
        sync_tracking['sync_sessions'] = []
    
    session_info = {
        'timestamp': datetime.now().isoformat(),
        'sync_targets': [t[0] for t in sync_targets],
        'total_cases': len(completed_cases),
        'aws_profile': AWS_PROFILE,
        'base_directory': str(base_dir)
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
        
        # Check for existing sync flags
        persistent_sync_flag = case_dir / "02_synced_to_persistent.txt"
        s3_sync_flag = case_dir / "02_synced_to_s3.txt"
        
        if completion_time <= last_synced and persistent_sync_flag.exists():
            print(f"  [=] Case {case_name} already synced to persistent storage")
            continue
        
        # Sync the case to all available targets
        print(f"  [*] Syncing case {synced_cases + 1}/{total_cases}: {case_name}")
        
        sync_results = {}
        
        for sync_type, sync_target in sync_targets:
            if sync_type == 'persistent':
                if sync_case_to_persistent_storage(sync_target, assigned_user, case_dir):
                    persistent_sync_flag.write_text(
                        f"Synced to persistent storage at {datetime.now().isoformat()}"
                    )
                    sync_results['persistent'] = True
                else:
                    sync_results['persistent'] = False
                    
            elif sync_type == 's3':
                if sync_case_to_s3(sync_target, bucket_name, assigned_user, case_dir):
                    s3_sync_flag.write_text(
                        f"Synced to S3 (ibd_root/{assigned_user}/) at {datetime.now().isoformat()}"
                    )
                    sync_results['s3'] = True
                else:
                    sync_results['s3'] = False
        
        # Consider sync successful if ANY target succeeded
        if any(sync_results.values()):
            sync_tracking[case_name] = {
                'last_synced_timestamp': datetime.now().timestamp(),
                'last_synced_iso': datetime.now().isoformat(),
                'sync_successful': True,
                'sync_targets': sync_results
            }
            synced_cases += 1
        else:
            sync_tracking[case_name] = {
                'last_synced_timestamp': sync_tracking.get(case_name, {}).get('last_synced_timestamp', 0),
                'last_attempted_iso': datetime.now().isoformat(),
                'sync_successful': False,
                'sync_targets': sync_results
            }
    
    # Update session info
    session_info['synced_cases'] = synced_cases
    session_info['failed_cases'] = total_cases - synced_cases
    sync_tracking['sync_sessions'].append(session_info)
    sync_tracking['last_sync_targets'] = [t[0] for t in sync_targets]
    
    # Save tracking information
    save_sync_tracking(sync_tracking_file, sync_tracking)
    
    print("")
    print("=" * 60)
    print(f"SYNC COMPLETED: {synced_cases}/{total_cases} cases synced")
    print(f"Base Directory: {base_dir}")
    print(f"Targets: {[t[0] for t in sync_targets]}")
    print(f"AWS Profile: {AWS_PROFILE}")
    if s3_client:
        print(f"S3 Path: s3://{bucket_name}/ibd_root/{assigned_user}/")
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
    
    # Read the assigned username from the local assignment file
    assigned_username, user_folder = get_assigned_user_from_folder(user_home_dir)
    
    if not assigned_username:
        print("[X] Could not determine assigned user from assignment_info.txt")
        print(f"[!] Expected file: {user_home_dir}/assignment_info.txt")
        print("[!] File should contain:")
        print("    Line 1: user folder name (e.g., user1)")
        print("    Line 2: username (e.g., hoda)")
        sys.exit(1)
    
    print(f"[+] Syncing for user: {assigned_username} (folder: {user_folder})")
    
    try:
        success = sync_completed_cases(bucket_name, user_folder, user_home_dir)
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