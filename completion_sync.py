#!/usr/bin/env python3
"""
IBD Case Completion Sync Script

Scans user directories for completed cases (marked by 01_labeling_complete.txt)
and syncs completion artifacts to S3, excluding large original files.
"""

import os
import sys
import json
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

def should_skip_s3_operations():
    """Check if we should skip S3 operations (test environment)"""
    skip_s3 = os.environ.get('SKIP_S3_OPERATIONS', '0')
    current_username = get_current_username()
    
    if skip_s3 == '1' or current_username == 'imagebuildertest':
        return True
    return False

def initialize_s3_client_for_sync(bucket_name):
    """Initialize S3 client specifically for sync operations"""
    if should_skip_s3_operations():
        print("[*] Test environment detected - skipping S3 operations")
        return None
    
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
        return None
    except NoCredentialsError:
        print("[!] No AWS credentials found - sync disabled")
        return None
    except Exception as e:
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

def sync_case_completion_artifacts(s3_client, bucket_name, assigned_user, case_dir):
    """Sync completion artifacts for a specific case to S3"""
    case_name = case_dir.name
    s3_case_prefix = f"ibd_root/{assigned_user}/{case_name}/"
    
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
        "*.temp"
    ]
    
    uploaded_files = 0
    
    try:
        print(f"  [*] Syncing completion artifacts for case: {case_name}")
        
        # Collect all files to potentially upload
        files_to_upload = []
        
        for pattern in completion_patterns:
            matching_files = list(case_dir.glob(pattern))
            files_to_upload.extend(matching_files)
        
        # Filter out excluded files
        filtered_files = []
        for file_path in files_to_upload:
            should_exclude = False
            for exclude_pattern in exclude_patterns:
                if fnmatch.fnmatch(file_path.name, exclude_pattern):
                    should_exclude = True
                    print(f"    [~] Excluding: {file_path.name}")
                    break
            
            if not should_exclude and file_path.is_file():
                filtered_files.append(file_path)
        
        # Upload filtered files
        for local_file_path in filtered_files:
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

def sync_completed_cases(bucket_name, assigned_user, user_home_dir):
    """Main function to sync all completed cases"""
    print("=" * 60)
    print("   IBD CASE COMPLETION SYNC")
    print("=" * 60)
    print(f"User: {assigned_user}")
    print(f"Home Directory: {user_home_dir}")
    print(f"S3 Bucket: {bucket_name}")
    print("")
    
    # Check if we should skip S3
    if should_skip_s3_operations():
        print("[*] Test environment - S3 sync disabled")
        return True
    
    # Initialize S3 client
    s3_client = initialize_s3_client_for_sync(bucket_name)
    if not s3_client:
        print("[!] S3 not available - sync skipped")
        return False
    
    # Find completed cases
    completed_cases = find_completed_cases(user_home_dir)
    if not completed_cases:
        print("[*] No completed cases found - nothing to sync")
        return True
    
    # Load sync tracking
    sync_tracking_file = get_sync_tracking_file(user_home_dir)
    sync_tracking = load_sync_tracking(sync_tracking_file)
    
    # Sync each completed case
    synced_cases = 0
    total_cases = len(completed_cases)
    
    for case_dir in completed_cases:
        case_name = case_dir.name
        
        # Check if already synced
        completion_flag = case_dir / "01_labeling_complete.txt"
        completion_time = completion_flag.stat().st_mtime if completion_flag.exists() else 0
        
        last_synced = sync_tracking.get(case_name, {}).get('last_synced_timestamp', 0)
        
        if completion_time <= last_synced:
            print(f"  [=] Case {case_name} already synced")
            continue
        
        # Sync the case
        print(f"  [*] Syncing case {synced_cases + 1}/{total_cases}: {case_name}")
        
        if sync_case_completion_artifacts(s3_client, bucket_name, assigned_user, case_dir):
            # Update tracking
            sync_tracking[case_name] = {
                'last_synced_timestamp': datetime.now().timestamp(),
                'last_synced_iso': datetime.now().isoformat(),
                'sync_successful': True
            }
            synced_cases += 1
        else:
            # Mark as attempted but failed
            sync_tracking[case_name] = {
                'last_synced_timestamp': sync_tracking.get(case_name, {}).get('last_synced_timestamp', 0),
                'last_attempted_iso': datetime.now().isoformat(),
                'sync_successful': False
            }
    
    # Save tracking information
    save_sync_tracking(sync_tracking_file, sync_tracking)
    
    print("")
    print("=" * 60)
    print(f"SYNC COMPLETED: {synced_cases}/{total_cases} cases synced")
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