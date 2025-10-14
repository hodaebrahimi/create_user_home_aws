#!/usr/bin/env python3
"""
S3-based user assignment for AppStream 2.0 (Linux)
Uses AWS profile 'appstream_machine_role' for credentials
Stores data in Linux paths (/home/appstream/)
Uses ibd_root/ as parent directory for user folders
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# AWS Profile configuration
AWS_PROFILE = 'appstream_machine_role'

def get_current_username():
    """Get the current username from various sources"""
    # Try AppStream user ID first (unique for each user pool user)
    username = (os.environ.get('APPSTREAM_USER_ID') or
               os.environ.get('USERNAME') or 
               os.environ.get('USER') or 
               os.environ.get('APPSTREAM_USER') or
               'unknown_user')
    return username.lower()

def initialize_s3_client(bucket_name, region='us-west-2'):
    """
    Initialize S3 client using AWS profile
    Returns: (s3_client or None, success: bool)
    """
    try:
        print(f"[*] Initializing S3 client with AWS profile: {AWS_PROFILE}")
        print(f"[*] Using AWS region: {region}")
        
        # Create boto3 session with profile
        session = boto3.Session(profile_name=AWS_PROFILE, region_name=region)
        s3_client = session.client('s3')
        
        # Test basic S3 connection
        print("[*] Testing S3 connection...")
        s3_client.head_bucket(Bucket=bucket_name)
        
        print(f"[+] S3 connection established for bucket: {bucket_name}")
        return s3_client, True
            
    except NoCredentialsError:
        print(f"[X] No AWS credentials found for profile: {AWS_PROFILE}")
        print("[!] AWS profile may not be configured")
        print(f"[!] Run: aws configure --profile {AWS_PROFILE}")
        return None, False
    
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        
        print(f"[X] S3 client initialization failed: {error_code}")
        
        if error_code == '403' or error_code == 'Forbidden' or error_code == 'AccessDenied':
            print(f"[!] Access denied - profile '{AWS_PROFILE}' lacks S3 permissions")
            print("[!] Required permissions:")
            print("    - s3:ListBucket on bucket")
            print("    - s3:GetObject, s3:PutObject on bucket objects")
        elif error_code == '404' or error_code == 'NoSuchBucket':
            print(f"[!] Bucket not found: {bucket_name}")
            print(f"[!] Verify bucket name and region ({region})")
        else:
            print(f"[!] Error: {error_msg}")
        
        return None, False
    
    except Exception as e:
        # Check if it's a profile not found error
        if 'could not be found' in str(e).lower():
            print(f"[X] AWS profile '{AWS_PROFILE}' not found")
            print(f"[!] Configure the profile with: aws configure --profile {AWS_PROFILE}")
        else:
            print(f"[X] S3 connection failed: {e}")
        return None, False

def list_user_folders_s3(bucket_name, s3_client):
    """
    List all user{i} folders in ibd_root/ from S3
    Returns: list of user folder names (e.g., ['user1', 'user2'])
    """
    try:
        print("[*] Scanning S3 bucket for user folders in ibd_root/...")
        
        # List objects with delimiter to get "folders" under ibd_root/
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Delimiter='/',
            Prefix='ibd_root/'
        )
        
        user_folders = []
        
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                folder_name = prefix['Prefix'].replace('ibd_root/', '').rstrip('/')
                # Only include folders that match user{number} pattern
                if folder_name.startswith('user') and folder_name[4:].isdigit():
                    user_folders.append(folder_name)
        
        user_folders.sort(key=lambda x: int(x[4:]))  # Sort by user number
        
        if user_folders:
            print(f"[+] Found {len(user_folders)} user folders in ibd_root/: {', '.join(user_folders)}")
        else:
            print("[!] No user{N} folders found in ibd_root/")
            print("[DEBUG] Make sure your S3 bucket contains folders like: ibd_root/user1/, ibd_root/user2/")
        
        return user_folders
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDenied':
            print("[X] Access denied when listing S3 folders")
        else:
            print(f"[X] Error listing S3 user folders: {error_code}")
        return []
    except Exception as e:
        print(f"[X] Unexpected error listing S3 user folders: {e}")
        return []

def check_user_assignment_s3(bucket_name, s3_client, username):
    """
    Check if username already has an assignment by scanning user folders
    Returns: (assigned_user_folder or None, is_assigned: bool)
    """
    try:
        print(f"[*] Checking for existing assignment for '{username}'...")
        
        # Scan all user{i} folders for assignment_info.txt
        user_folders = list_user_folders_s3(bucket_name, s3_client)
        
        for user_folder in user_folders:
            assignment_key = f"ibd_root/{user_folder}/assignment_info.txt"
            
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=assignment_key)
                assignment_content = response['Body'].read().decode('utf-8').strip()
                lines = assignment_content.split('\n')
                
                # Format: line 0 = folder name, line 1 = username
                if len(lines) >= 2:
                    assigned_username = lines[1].strip().lower()
                    
                    if assigned_username == username.lower():
                        print(f"[+] Found existing assignment: {username} → {user_folder}")
                        return user_folder, True
                        
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchKey':
                    print(f"[!] Error reading {assignment_key}: {e}")
                continue
        
        print(f"[*] No existing assignment found for '{username}'")
        return None, False
        
    except Exception as e:
        print(f"[!] Error checking assignment: {e}")
        return None, False

def get_all_assignments_s3(bucket_name, s3_client):
    """
    Get all current user assignments by scanning user folders
    Returns: dict mapping username -> user_folder
    """
    assignments = {}
    
    try:
        print("[*] Checking which user folders are already assigned...")
        
        user_folders = list_user_folders_s3(bucket_name, s3_client)
        
        for user_folder in user_folders:
            assignment_key = f"ibd_root/{user_folder}/assignment_info.txt"
            
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=assignment_key)
                assignment_content = response['Body'].read().decode('utf-8').strip()
                lines = assignment_content.split('\n')
                
                if len(lines) >= 2:
                    assigned_username = lines[1].strip().lower()
                    assignments[assigned_username] = user_folder
                    
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchKey':
                    print(f"[!] Error reading {assignment_key}: {e}")
                continue
        
        if assignments:
            print(f"[*] Current assignments: {assignments}")
        else:
            print("[*] No folders currently assigned")
        
        return assignments
        
    except Exception as e:
        print(f"[!] Error checking assignments: {e}")
        return {}

def assign_user_folder_s3(bucket_name, s3_client, username, user_folder, base_dir):
    """
    Assign a user folder to username by creating assignment file IN the user folder
    Returns: success: bool
    """
    try:
        print(f"[*] Assigning {username} → {user_folder}")
        
        # Create assignment content
        assignment_content = f"{user_folder}\n{username}\nAssigned at: {datetime.now().isoformat()}"
        
        # Write assignment to S3
        assignment_key = f"ibd_root/{user_folder}/assignment_info.txt"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=assignment_key,
            Body=assignment_content.encode('utf-8'),
            ContentType='text/plain'
        )
        
        print(f"[+] Successfully assigned {username} → {user_folder} in S3")
        
        # Try to create local tracking file (may fail in Image Builder, that's OK)
        try:
            local_assignment_file = base_dir / user_folder / "assignment_info.txt"
            local_assignment_file.parent.mkdir(parents=True, exist_ok=True)
            local_assignment_file.write_text(assignment_content)
            print(f"[+] Created local assignment file: {local_assignment_file}")
        except PermissionError:
            print(f"[!] Cannot create local assignment file (permission denied)")
            print(f"[*] This is expected in Image Builder - file will be created when deployed")
        except Exception as local_err:
            print(f"[!] Could not create local assignment file: {local_err}")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"[X] Failed to create assignment in S3: {error_code}")
        return False
    except Exception as e:
        print(f"[X] Error assigning user folder: {e}")
        return False
        
def ensure_local_assignment_file(user_folder, username, base_dir):
    """Ensure the local assignment file exists"""
    try:
        assignment_content = f"{user_folder}\n{username}\nVerified at: {datetime.now().isoformat()}"
        local_assignment_file = base_dir / user_folder / "assignment_info.txt"  # ✅ Uses base_dir parameter
        local_assignment_file.parent.mkdir(parents=True, exist_ok=True)
        local_assignment_file.write_text(assignment_content)
        print(f"[+] Ensured local assignment file exists: {local_assignment_file}")
        return True
    except PermissionError:
        print(f"[!] Cannot create local assignment file (permission denied)")
        print(f"[*] This is expected in Image Builder - file will be created when deployed")
        return True  # Not a critical failure
    except Exception as e:
        print(f"[!] Warning: Could not create local assignment file: {e}")
        return True  # Not a critical failure

def sync_s3_to_local(bucket_name, s3_client, assigned_user, base_dir):
    """
    Sync S3 user folder to local directory
    Downloads all files from s3://bucket/ibd_root/userN/ to {base_dir}/userN/
    """
    s3_prefix = f"ibd_root/{assigned_user}/"
    local_dir = base_dir / assigned_user
    
    try:
        print(f"[*] Syncing S3 folder to local: {local_dir}")
        local_dir.mkdir(parents=True, exist_ok=True)
        
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)
        
        downloaded_files = 0
        failed_files = 0
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                s3_key = obj['Key']
                
                # Skip directory markers
                if s3_key.endswith('/'):
                    continue
                
                # Calculate relative path
                relative_path = s3_key[len(s3_prefix):]
                if not relative_path:  # Skip if empty
                    continue
                
                # Skip assignment_info.txt - we already created it locally
                if relative_path == "assignment_info.txt":
                    print(f"  [~] Skipping assignment_info.txt (already exists locally)")
                    continue
                    
                local_file_path = local_dir / relative_path
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                try:
                    print(f"  [*] Downloading: {relative_path}")
                    s3_client.download_file(bucket_name, s3_key, str(local_file_path))
                    downloaded_files += 1
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    if error_code == 'AccessDenied':
                        print(f"  [X] Access denied: {relative_path}")
                    else:
                        print(f"  [X] Failed ({error_code}): {relative_path}")
                    failed_files += 1
                except Exception as e:
                    print(f"  [X] Failed: {relative_path} - {e}")
                    failed_files += 1
        
        if downloaded_files == 0 and failed_files == 0:
            print(f"[*] No case files found in S3 for {assigned_user} (new user)")
        elif failed_files > 0:
            print(f"[!] S3 sync completed with issues: {downloaded_files} succeeded, {failed_files} failed")
        else:
            print(f"[+] S3 sync completed successfully: {downloaded_files} files")
        
        return local_dir, downloaded_files, failed_files
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"[X] S3 sync error: {error_code}")
        return local_dir, 0, 0
    except Exception as e:
        print(f"[X] Error syncing from S3: {e}")
        return local_dir, 0, 0

def find_and_assign_user(bucket_name, base_dir, region='us-west-2'):
    """
    Main function to find and assign user using S3 only
    Returns: assigned_user_folder or None
    """
    print("=" * 60)
    print(f"   S3 USER ASSIGNMENT (AWS Profile: {AWS_PROFILE})")
    print("=" * 60)
    print(f"Target bucket: {bucket_name}")
    print(f"Region: {region}")
    print("")
    
    current_username = get_current_username()
    print(f"[*] Current username: {current_username}")
    
    print("\n" + "-" * 40)
    print("Connecting to S3...")
    print("-" * 40)
    
    # Initialize S3 client
    s3_client, success = initialize_s3_client(bucket_name, region)
    
    if not success or s3_client is None:
        print("\n" + "-" * 40)
        print("[X] Failed to connect to S3")
        return None
    
    print("\n" + "-" * 40)
    print("Checking user assignment...")
    print("-" * 40)
    
    # Check if user already has an assignment
    existing_assignment, is_assigned = check_user_assignment_s3(bucket_name, s3_client, current_username)
    
    if is_assigned and existing_assignment:
        print(f"[+] User already assigned: {existing_assignment}")
        ensure_local_assignment_file(existing_assignment, current_username, base_dir)
        return existing_assignment
    
    # Get list of available user folders
    user_folders = list_user_folders_s3(bucket_name, s3_client)
    
    if not user_folders:
        print("\n" + "-" * 40)
        print("[X] No user{i} folders found in ibd_root/")
        return None
    
    # Get all current assignments
    current_assignments = get_all_assignments_s3(bucket_name, s3_client)
    assigned_folders = set(current_assignments.values())
    
    # Find first available folder
    print("\n" + "-" * 40)
    print("Finding available user folder...")
    print("-" * 40)
    
    assigned_user = None
    for user_folder in user_folders:
        if user_folder not in assigned_folders:
            print(f"[+] Found available folder: {user_folder}")
            if assign_user_folder_s3(bucket_name, s3_client, current_username, user_folder, base_dir):
                assigned_user = user_folder
                break
            else:
                print(f"[!] Failed to assign {user_folder}, trying next...")
    
    if not assigned_user:
        print("\n" + "-" * 40)
        print("[X] All user folders are currently assigned")
        print(f"[*] Current assignments:")
        for username, folder in current_assignments.items():
            print(f"    {username} → {folder}")
        print("\n" + "-" * 40)
        return None
    
    return assigned_user

def main():
    """Main execution"""
    BUCKET_NAME = "hoda2-ibd-sample-cases-us-west-2"
    REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')
    
    # Try ~/MyFiles/HomeFolder first (AppStream persistent - auto-syncs to S3), 
    # then fall back to /opt/appstream (temporary - requires manual S3 sync)
    home_dir = Path.home()
    preferred_base = home_dir / "MyFiles" / "HomeFolder"
    fallback_base = Path("/opt/appstream")
    
    # Check if preferred directory exists, otherwise use fallback
    if preferred_base.exists():
        base_dir = preferred_base
        print(f"[*] Using AppStream HomeFolder: {base_dir}")
        print(f"[*] This location auto-syncs to S3 - no manual sync needed")
    else:
        base_dir = fallback_base
        print(f"[!] HomeFolder not found at {preferred_base}")
        print(f"[*] Using fallback location: {base_dir}")
        print(f"[*] This is temporary storage - will need manual S3 sync")
        # Create fallback if it doesn't exist
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            print(f"[X] Cannot create {base_dir} - permission denied")
            sys.exit(1)
    
    current_username = get_current_username()
    print(f"[DEBUG] Detected username: '{current_username}'")
    print(f"[DEBUG] Using AWS profile: '{AWS_PROFILE}'")
    print(f"[DEBUG] Base directory: '{base_dir}'")
    print("")
    
    try:
        assigned_user = find_and_assign_user(BUCKET_NAME, base_dir, REGION)
        
        if assigned_user:
            print("\n" + "=" * 60)
            print("   ASSIGNMENT COMPLETED")
            print("=" * 60)
            print(f"[+] Assigned user: {assigned_user}")
            
            # Initialize S3 client for sync
            s3_client, _ = initialize_s3_client(BUCKET_NAME, REGION)
            
            if s3_client:
                print("\n" + "-" * 40)
                print("Syncing data from S3...")
                print("-" * 40)
                # Use base_dir parameter here!
                local_dir, downloaded, failed = sync_s3_to_local(BUCKET_NAME, s3_client, assigned_user, base_dir)
                
                print("\n" + "-" * 40)
                print("Sync Summary:")
                print(f"  Local directory: {local_dir}")
                print(f"  Files downloaded: {downloaded}")
                if failed > 0:
                    print(f"  Files failed: {failed}")
                print("-" * 40)
            
            # Output for bash script to parse - use base_dir
            print(f"\nASSIGNED_USER={assigned_user}")
            print(f"USER_HOME_DIR={base_dir}/{assigned_user}")
            
            print("\n" + "=" * 60)
            sys.exit(0)
        else:
            print("\n" + "=" * 60)
            print("[X] Failed to assign user")
            print("=" * 60)
            sys.exit(1)
        
    except Exception as e:
        print(f"\n[X] Unexpected error: {e}")
        import traceback
        print("[DEBUG] Full traceback:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()