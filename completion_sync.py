#!/usr/bin/env python3
"""
IBD Case Completion Sync Script

Scans user directories for completed cases (marked by 01_labeling_complete.txt)
and syncs completion artifacts to S3 or mounted S3, excluding large original files.
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

def get_current_username():
    """Get the current username from various sources"""
    username = (os.environ.get('USERNAME') or 
               os.environ.get('USER') or 
               os.environ.get('APPSTREAM_USER') or
               'unknown_user')
    return username.lower()

def initialize_s3_client_for_sync(bucket_name):
    """Initialize S3 client specifically for sync operations - no username checks"""
    try:
        print("[*] Initializing S3 client for completion sync...")
        region = os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')
        s3_client = boto3.client('s3', region_name=region)
        
        # Quick connection test
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"[+] S3 connection established for sync")
        return s3_client
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"[!] S3 client initialization failed: {error_code}")
        if error_code == 'AccessDenied':
            print("[*] No S3 permissions - will attempt mount fallback")
        return None
    except NoCredentialsError:
        print("[!] No AWS credentials found - will attempt mount fallback")
        return None
    except Exception as e:
        print(f"[!] S3 connection failed: {e} - will attempt mount fallback")
        return None

def check_s3_mount_available():
    """Check if S3 bucket is mounted locally"""
    mount_path = Path("C:/s3_bucket/ibd_root")
    if mount_path.exists() and mount_path.is_dir():
        print(f"[+] S3 mount available at: {mount_path}")
        return mount_path
    
    print(f"[!] S3 mount not available at: {mount_path}")
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
        "organs_*_ibd.nii.gz",              # Original organ files (not user-created)
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

def sync_case_completion_artifacts(s3_client, bucket_name, assigned_user, case_dir):
    """Sync completion artifacts for a specific case to S3"""
    case_name = case_dir.name
    s3_case_prefix = f"ibd_root/{assigned_user}/{case_name}/"
    
    uploaded_files = 0
    
    try:
        print(f"  [*] Syncing completion artifacts for case: {case_name}")
        
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
                if error_code == 'AccessDenied':
                    print(f"    [X] Access denied uploading: {relative_path}")
                else:
                    print(f"    [X] Failed to upload {relative_path}: {error_code}")
            except Exception as e:
                print(f"    [X] Failed to upload {relative_path}: {e}")
        
        # Create completion timestamp in S3
        if uploaded_files > 0:
            timestamp_key = s3_case_prefix + "completion_sync_timestamp.txt"
            timestamp_content = f"Case {case_name} completion artifacts synced by {assigned_user} at {datetime.now().isoformat()}\nFiles synced: {uploaded_files}"
            
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
                print(f"    [+] Created sync timestamp")
            except Exception as e:
                print(f"    [!] Could not create sync timestamp: {e}")
        
        print(f"  [+] Case sync completed: {uploaded_files} files uploaded")
        return uploaded_files > 0
        
    except Exception as e:
        print(f"  [X] Error syncing case {case_name}: {e}")
        return False

def sync_case_to_mount(mount_path, assigned_user, case_dir):
    """Sync completion artifacts for a specific case to mounted S3"""
    case_name = case_dir.name
    mount_case_path = mount_path / assigned_user / case_name
    
    try:
        print(f"  [*] Syncing completion artifacts to mount for case: {case_name}")
        
        # Create mount case directory
        mount_case_path.mkdir(parents=True, exist_ok=True)
        
        files_to_sync = get_completion_files_to_sync(case_dir, assigned_user)
        
        copied_files = 0
        for local_file_path in files_to_sync:
            mount_file_path = mount_case_path / local_file_path.name
            
            try:
                shutil.copy2(local_file_path, mount_file_path)
                copied_files += 1
                print(f"    [+] Copied: {local_file_path.name}")
            except Exception as e:
                print(f"    [X] Failed to copy {local_file_path.name}: {e}")
        
        # Create completion timestamp in mount
        if copied_files > 0:
            timestamp_file = mount_case_path / "completion_sync_timestamp.txt"
            timestamp_content = f"Case {case_name} completion artifacts synced by {assigned_user} at {datetime.now().isoformat()}\nFiles synced: {copied_files}"
            
            try:
                timestamp_file.write_text(timestamp_content)
                print(f"    [+] Created sync timestamp")
            except Exception as e:
                print(f"    [!] Could not create sync timestamp: {e}")
        
        print(f"  [+] Case sync to mount completed: {copied_files} files copied")
        return copied_files > 0
        
    except Exception as e:
        print(f"  [X] Error syncing case {case_name} to mount: {e}")
        return False

def sync_completed_cases(bucket_name, assigned_user, user_home_dir):
    """Main function to sync all completed cases"""
    print("=" * 60)
    print("   IBD CASE COMPLETION SYNC")
    print("=" * 60)
    print(f"User: {assigned_user}")
    print(f"Home Directory: {user_home_dir}")
    print(f"S3 Bucket: {bucket_name}")
    print("")
    
    # Find completed cases first
    completed_cases = find_completed_cases(user_home_dir)
    if not completed_cases:
        print("[*] No completed cases found - nothing to sync")
        return True
    
    # Try S3 first, then mount fallback based on actual connectivity
    s3_client = initialize_s3_client_for_sync(bucket_name)
    sync_mode = None
    mount_path = None
    
    if s3_client is not None:
        sync_mode = "s3"
        print("[*] Using S3 sync mode")
    else:
        mount_path = check_s3_mount_available()
        if mount_path:
            sync_mode = "mount"
            print("[*] Using mount sync mode")
        else:
            print("[!] Neither S3 nor mount available - cannot sync completed cases")
            print("[!] Completed cases remain in local directory only")
            return False
    
    # Load sync tracking
    sync_tracking_file = get_sync_tracking_file(user_home_dir)
    sync_tracking = load_sync_tracking(sync_tracking_file)
    
    # Add sync mode to tracking
    if 'sync_sessions' not in sync_tracking:
        sync_tracking['sync_sessions'] = []
    
    session_info = {
        'timestamp': datetime.now().isoformat(),
        'sync_mode': sync_mode,
        'total_cases': len(completed_cases)
    }
    
    # Sync each completed case
    synced_cases = 0
    total_cases = len(completed_cases)
    
    for case_dir in completed_cases:
        case_name = case_dir.name
        
        # Check if already synced (check for mount or S3 sync flags)
        completion_flag = case_dir / "01_labeling_complete.txt"
        completion_time = completion_flag.stat().st_mtime if completion_flag.exists() else 0
        
        last_synced = sync_tracking.get(case_name, {}).get('last_synced_timestamp', 0)
        
        # Check for existing sync flag files
        s3_sync_flag = case_dir / "02_synced_to_s3.txt"
        mount_sync_flag = case_dir / "02_synced_to_mount.txt"
        
        if completion_time <= last_synced and (s3_sync_flag.exists() or mount_sync_flag.exists()):
            print(f"  [=] Case {case_name} already synced")
            continue
        
        # Sync the case
        print(f"  [*] Syncing case {synced_cases + 1}/{total_cases}: {case_name}")
        
        sync_successful = False
        
        if sync_mode == "s3":
            if sync_case_completion_artifacts(s3_client, bucket_name, assigned_user, case_dir):
                # Create local sync flag
                s3_sync_flag.write_text(f"Synced to S3 at {datetime.now().isoformat()}")
                sync_successful = True
        elif sync_mode == "mount":
            if sync_case_to_mount(mount_path, assigned_user, case_dir):
                # Create local sync flag
                mount_sync_flag.write_text(f"Synced to mount at {datetime.now().isoformat()}")
                sync_successful = True
        
        if sync_successful:
            # Update tracking
            sync_tracking[case_name] = {
                'last_synced_timestamp': datetime.now().timestamp(),
                'last_synced_iso': datetime.now().isoformat(),
                'sync_successful': True,
                'sync_mode': sync_mode
            }
            synced_cases += 1
        else:
            # Mark as attempted but failed
            sync_tracking[case_name] = {
                'last_synced_timestamp': sync_tracking.get(case_name, {}).get('last_synced_timestamp', 0),
                'last_attempted_iso': datetime.now().isoformat(),
                'sync_successful': False,
                'sync_mode': sync_mode
            }
    
    # Update session info
    session_info['synced_cases'] = synced_cases
    session_info['failed_cases'] = total_cases - synced_cases
    sync_tracking['sync_sessions'].append(session_info)
    sync_tracking['last_sync_mode'] = sync_mode
    
    # Save tracking information
    save_sync_tracking(sync_tracking_file, sync_tracking)
    
    print("")
    print("=" * 60)
    print(f"SYNC COMPLETED ({sync_mode.upper()}): {synced_cases}/{total_cases} cases synced")
    print("=" * 60)
    
    return synced_cases > 0 or total_cases == 0

def main():
    """Main entry point"""
    if len(sys.argv) < 3:
        print("Usage: python completion_sync.py <bucket_name> <assigned_user> [user_home_dir]")
        print("Example: python completion_sync.py my-bucket user1 C:/AppStreamUsers/user1")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    assigned_user = sys.argv[2]
    
    if len(sys.argv) > 3:
        user_home_dir = sys.argv[3]
    else:
        user_home_dir = f"C:/AppStreamUsers/{assigned_user}"
    
    try:
        success = sync_completed_cases(bucket_name, assigned_user, user_home_dir)
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