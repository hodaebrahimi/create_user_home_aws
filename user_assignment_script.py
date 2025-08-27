#!/usr/bin/env python3

import os
import sys
import json
import yaml
from pathlib import Path
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

def initialize_s3_client(bucket_name):
    # Check environment variable from batch file
    skip_s3 = os.environ.get('SKIP_S3_OPERATIONS', '0')
    current_username = get_current_username()
    
    if skip_s3 == '1' or current_username == 'imagebuildertest':
        print("[*] Test environment detected - skipping S3 operations")
        return None
    if os.environ.get('USERNAME') == 'ImageBuilderTest':
        print("[*] Image building mode - skipping S3 operations")
        return None
    
    try:
        # Normal IAM role logic for production
        print("[*] Using AWS default credential chain (AppStream IAM role)")
        s3_client = boto3.client('s3', region_name='us-west-2')
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"[+] S3 connection established for bucket: {bucket_name}")
        return s3_client
    except Exception as e:
        print(f"[X] S3 connection failed: {e}")
        print("[*] Will attempt to use mounted S3 data instead")
        return None

def check_s3_mount_available():
    """
    Check if S3 bucket is mounted at C:/s3_bucket/ibd_root
    """
    mount_path = Path("C:/s3_bucket/ibd_root")
    if mount_path.exists() and mount_path.is_dir():
        # Check if it has expected structure
        if any(mount_path.iterdir()):  # Not empty
            print(f"[+] S3 mount available at: {mount_path}")
            return mount_path
    
    print(f"[!] S3 mount not available at: {mount_path}")
    return None

def get_current_username():
    """Get the current username from various sources"""
    username = (os.environ.get('USERNAME') or 
               os.environ.get('USER') or 
               os.environ.get('APPSTREAM_USER') or
               'unknown_user')
    return username.lower()


def list_user_folders_s3(bucket_name, s3_client):
    """List all user{i} folders in ibd_root/ from S3"""
    try:
        print("[*] Scanning S3 ibd_root/ for user folders...")
        
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='ibd_root/',
            Delimiter='/'
        )
        
        user_folders = []
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                folder_name = prefix['Prefix'].replace('ibd_root/', '').rstrip('/')
                if folder_name.startswith('user') and folder_name[4:].isdigit():
                    user_folders.append(folder_name)
        
        user_folders.sort(key=lambda x: int(x[4:]))
        print(f"[+] Found {len(user_folders)} user folders in S3: {user_folders}")
        return user_folders
        
    except ClientError as e:
        print(f"[X] Error listing S3 user folders: {e.response['Error']['Code']}")
        return []

def list_user_folders_mount(mount_path):
    """List all user{i} folders from mounted S3"""
    try:
        print(f"[*] Scanning mount {mount_path} for user folders...")
        
        user_folders = []
        for item in mount_path.iterdir():
            if item.is_dir() and item.name.startswith('user') and item.name[4:].isdigit():
                user_folders.append(item.name)
        
        user_folders.sort(key=lambda x: int(x[4:]))
        print(f"[+] Found {len(user_folders)} user folders in mount: {user_folders}")
        return user_folders
        
    except Exception as e:
        print(f"[X] Error listing mount user folders: {e}")
        return []

def check_user_taken_s3(bucket_name, s3_client, user_folder):
    """Check if a user folder is taken by looking for taken_by.txt in S3"""
    try:
        taken_by_key = f"ibd_root/{user_folder}/taken_by.txt"
        response = s3_client.get_object(Bucket=bucket_name, Key=taken_by_key)
        taken_by_content = response['Body'].read().decode('utf-8').strip()
        print(f"[*] {user_folder} is taken by: {taken_by_content}")
        return True, taken_by_content
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"[*] {user_folder} is available")
            return False, None
        else:
            print(f"[!] Error checking {user_folder}: {e.response['Error']['Code']}")
            return True, "error"

def check_user_taken_mount(mount_path, user_folder):
    """Check if a user folder is taken by looking for taken_by.txt in mount"""
    try:
        taken_by_file = mount_path / user_folder / "taken_by.txt"
        if taken_by_file.exists():
            taken_by_content = taken_by_file.read_text().strip()
            print(f"[*] {user_folder} is taken by: {taken_by_content}")
            return True, taken_by_content
        else:
            print(f"[*] {user_folder} is available")
            return False, None
    except Exception as e:
        print(f"[!] Error checking {user_folder}: {e}")
        return True, "error"

def claim_user_folder_s3(bucket_name, s3_client, user_folder, current_username):
    """Claim a user folder by creating taken_by.txt in S3 and locally"""
    try:
        taken_by_key = f"ibd_root/{user_folder}/taken_by.txt"
        # S3 version - only username
        s3_content = f"{current_username}\nClaimed at: {datetime.now().isoformat()}"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=taken_by_key,
            Body=s3_content,
            ContentType='text/plain'
        )
        
        print(f"[+] Successfully claimed {user_folder} for {current_username} in S3")
        
        # Local version - user folder + username
        local_content = f"{user_folder}\n{current_username}\nClaimed at: {datetime.now().isoformat()}"
        local_taken_by_file = Path(f"C:/AppStreamUsers/{user_folder}/taken_by.txt")
        local_taken_by_file.parent.mkdir(parents=True, exist_ok=True)
        local_taken_by_file.write_text(local_content)
        
        print(f"[+] Also saved taken_by.txt locally: {local_taken_by_file}")
        
        return True
    except ClientError as e:
        print(f"[X] Error claiming {user_folder} in S3: {e.response['Error']['Code']}")
        return False
    except Exception as e:
        print(f"[X] Error saving local taken_by.txt: {e}")
        return False

def claim_user_folder_mount(mount_path, user_folder, current_username):
    """Claim a user folder by creating taken_by.txt in mount and locally"""
    try:
        # Mount version - only username
        taken_by_file = mount_path / user_folder / "taken_by.txt"
        taken_by_file.parent.mkdir(parents=True, exist_ok=True)
        
        mount_content = f"{current_username}\nClaimed at: {datetime.now().isoformat()}"
        taken_by_file.write_text(mount_content)
        
        print(f"[+] Successfully claimed {user_folder} for {current_username} in mount")
        
        # Local version - user folder + username
        local_content = f"{user_folder}\n{current_username}\nClaimed at: {datetime.now().isoformat()}"
        local_taken_by_file = Path(f"C:/AppStreamUsers/{user_folder}/taken_by.txt")
        local_taken_by_file.parent.mkdir(parents=True, exist_ok=True)
        local_taken_by_file.write_text(local_content)
        
        print(f"[+] Also saved taken_by.txt locally: {local_taken_by_file}")
        
        return True
    except Exception as e:
        print(f"[X] Error claiming {user_folder} in mount: {e}")
        return False

def sync_s3_to_local(bucket_name, s3_client, assigned_user):
    """Sync S3 user folder to local AppStreamUsers directory"""
    s3_prefix = f"ibd_root/{assigned_user}/"
    local_dir = Path(f"C:/AppStreamUsers/{assigned_user}")
    
    try:
        print(f"[*] Syncing S3 folder to local: {local_dir}")
        local_dir.mkdir(parents=True, exist_ok=True)
        
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)
        
        downloaded_files = 0
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                s3_key = obj['Key']
                if s3_key.endswith('/') or s3_key.endswith('taken_by.txt'):
                    continue
                
                relative_path = s3_key[len(s3_prefix):]
                local_file_path = local_dir / relative_path
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                s3_client.download_file(bucket_name, s3_key, str(local_file_path))
                downloaded_files += 1
                print(f"  [+] Downloaded: {relative_path}")
        
        print(f"[+] S3 sync completed: {downloaded_files} files")
        return local_dir
        
    except Exception as e:
        print(f"[X] Error syncing from S3: {e}")
        return local_dir

def sync_mount_to_local_from_path(source_dir, assigned_user_folder):
    """Sync specific mount path to local AppStreamUsers directory"""
    import shutil
    
    # Use the assigned_user_folder name directly (e.g., "user1")
    local_dir = Path(f"C:/AppStreamUsers/{assigned_user_folder}")
    
    try:
        print(f"[*] Syncing mount path to local: {source_dir} -> {local_dir}")
        local_dir.mkdir(parents=True, exist_ok=True)
        
        if not source_dir.exists():
            print(f"[!] Source directory doesn't exist: {source_dir}")
            return local_dir
        
        copied_files = 0
        for item in source_dir.rglob("*"):
            if item.is_file() and item.name != "taken_by.txt":
                relative_path = item.relative_to(source_dir)
                local_file_path = local_dir / relative_path
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                shutil.copy2(item, local_file_path)
                copied_files += 1
                print(f"  [+] Copied: {relative_path}")
        
        print(f"[+] Mount path sync completed: {copied_files} files")
        return local_dir
        
    except Exception as e:
        print(f"[X] Error syncing from mount path: {e}")
        return local_dir

def sync_mount_to_local(mount_path, assigned_user):
    """Sync mount user folder to local AppStreamUsers directory"""
    import shutil
    
    source_dir = mount_path / assigned_user
    local_dir = Path(f"C:/AppStreamUsers/{assigned_user}")
    
    try:
        print(f"[*] Syncing mount folder to local: {local_dir}")
        local_dir.mkdir(parents=True, exist_ok=True)
        
        if not source_dir.exists():
            print(f"[!] Source directory doesn't exist: {source_dir}")
            return local_dir
        
        copied_files = 0
        for item in source_dir.rglob("*"):
            if item.is_file() and item.name != "taken_by.txt":
                relative_path = item.relative_to(source_dir)
                local_file_path = local_dir / relative_path
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                shutil.copy2(item, local_file_path)
                copied_files += 1
                print(f"  [+] Copied: {relative_path}")
        
        print(f"[+] Mount sync completed: {copied_files} files")
        return local_dir
        
    except Exception as e:
        print(f"[X] Error syncing from mount: {e}")
        return local_dir

def update_prep_seg_yaml(assigned_user_folder):
    """Update prep_seg.yaml file to set output directory to user's folder"""
    yaml_file = Path("C:/Scripts/ibd_labeling_local_1-main/prep_seg.yaml")
    
    try:
        if not yaml_file.exists():
            print(f"[!] prep_seg.yaml not found at {yaml_file}")
            return False
        
        with open(yaml_file, 'r') as f:
            yaml_data = yaml.safe_load(f)
        
        # Use the assigned_user_folder (e.g., "user1") directly
        new_output_dir = f"C:/AppStreamUsers/{assigned_user_folder}"
        
        # Remove existing output directory entries
        output_keys = ['output_directory', 'output_dir', 'outputDirectory', 'output_path', 'outputPath']
        for key in list(yaml_data.keys()):
            if key in output_keys:
                del yaml_data[key]
        
        yaml_data['output_directory'] = new_output_dir
        print(f"[+] Set output_directory: {new_output_dir}")
        
        with open(yaml_file, 'w') as f:
            yaml.dump(yaml_data, f, default_flow_style=False, indent=2)
        
        print(f"[+] prep_seg.yaml updated successfully")
        return True
        
    except Exception as e:
        print(f"[X] Error updating prep_seg.yaml: {e}")
        return False

def find_and_assign_user(bucket_name):
    """
    Main function to find and assign user with special handling for test environment
    """
    print("=" * 60)
    print("   HYBRID S3/MOUNT USER ASSIGNMENT SYSTEM")
    print("=" * 60)
    print(f"Target bucket: {bucket_name}")
    print("")
    
    current_username = get_current_username()
    print(f"[*] Current username: {current_username}")
    
    # Special handling for ImageBuilderTest - use mount-only workflow
    if current_username == 'imagebuildertest':
        print("[*] Test environment detected - using mount-only workflow")
        
        # Check mount availability
        mount_path = check_s3_mount_available()
        if not mount_path:
            print("[X] Mount not available for test environment")
            return None
        
        # For test environment, first check if user is already assigned
        print("Scanning mount for available user folders...")
        user_folders = list_user_folders_mount(mount_path)

        if user_folders:
            assigned_user = None
            
            # First pass: check if current user already has an assignment
            for user_folder in user_folders:
                is_taken, taken_by_content = check_user_taken_mount(mount_path, user_folder)
                
                if is_taken and taken_by_content:
                    # Extract username from first line (mount format: username only)
                    taken_by_user = taken_by_content.split('\n')[0].strip().lower()
                    if taken_by_user == current_username:
                        print(f"[+] Found existing assignment: {user_folder} for {current_username}")
                        assigned_user = user_folder
                        print(f"[+] Test environment using existing assignment: {assigned_user}")
                        break
            
            # Second pass: if no existing assignment, find available folder
            if not assigned_user:
                for user_folder in user_folders:
                    is_taken, taken_by = check_user_taken_mount(mount_path, user_folder)
                    
                    if not is_taken:
                        print(f"[+] Found available folder: {user_folder}")
                        if claim_user_folder_mount(mount_path, user_folder, current_username):
                            assigned_user = user_folder
                            print(f"[+] Test environment assigned to: {assigned_user}")
                            break
        
        return assigned_user
    
    # Production workflow - try S3 first, then mount fallback
    print("\n" + "-" * 40)
    print("Attempting S3 connection...")
    s3_client = initialize_s3_client(bucket_name)

    assigned_user = None
    
    if s3_client is not None:
        # S3 available - use S3 workflow
        print("Using S3 workflow...")
        user_folders = list_user_folders_s3(bucket_name, s3_client)

        if user_folders:
            # First pass: check if current user already has an assignment
            for user_folder in user_folders:
                is_taken, taken_by_content = check_user_taken_s3(bucket_name, s3_client, user_folder)
                
                if is_taken and taken_by_content:
                    # Extract username from first line (S3 format: username only)
                    taken_by_user = taken_by_content.split('\n')[0].strip().lower()
                    if taken_by_user == current_username:
                        print(f"[+] Found existing assignment: {user_folder} for {current_username}")
                        assigned_user = user_folder
                        break
            
            # Second pass: if no existing assignment, find available folder
            if not assigned_user:
                for user_folder in user_folders:
                    is_taken, taken_by = check_user_taken_s3(bucket_name, s3_client, user_folder)
                    
                    if not is_taken:
                        print(f"[+] Found available folder: {user_folder}")
                        if claim_user_folder_s3(bucket_name, s3_client, user_folder, current_username):
                            assigned_user = user_folder
                            break
        
        if not assigned_user:
            print("[X] No available user folders found in S3")
    
    # Fallback to mounted S3 if S3 direct access failed
    if not assigned_user:
        print("\n" + "-" * 40)
        print("Attempting mount fallback...")
        mount_path = check_s3_mount_available()
        
        if mount_path:
            print("Using mount workflow...")
            user_folders = list_user_folders_mount(mount_path)
            
            if user_folders:
                assigned_user = None
                # First pass: check if current user already has an assignment
                for user_folder in user_folders:
                    is_taken, taken_by_content = check_user_taken_mount(mount_path, user_folder)
                    
                    if is_taken and taken_by_content:
                        # Extract username from first line (mount format: username only)
                        taken_by_user = taken_by_content.split('\n')[0].strip().lower()
                        if taken_by_user == current_username:
                            print(f"[+] Found existing assignment: {user_folder} for {current_username}")
                            assigned_user = user_folder
                            break
                
                # Second pass: if no existing assignment, find available folder
                if not assigned_user:
                    for user_folder in user_folders:
                        is_taken, taken_by = check_user_taken_mount(mount_path, user_folder)
                        
                        if not is_taken:
                            print(f"[+] Found available folder: {user_folder}")
                            if claim_user_folder_mount(mount_path, user_folder, current_username):
                                assigned_user = user_folder
                                break
    
    # Final fallback - generate user assignment
    if not assigned_user:
        print("\n" + "-" * 40)
        print("No user folders available - cannot assign user")
        print("[X] No user{i} folders found in S3 or mount")
        print("[DEBUG] Make sure your S3 bucket or mount contains folders like: user1, user2, user3, etc.")
        return None 
    
    return assigned_user

if __name__ == "__main__":

    current_username = get_current_username()
    print(f"[DEBUG] Detected username: '{current_username}'")
    print(f"[DEBUG] Username type: {type(current_username)}")
    print(f"[DEBUG] Is imagebuildertest?: {current_username == 'imagebuildertest'}")
    
    BUCKET_NAME = "hoda2-ibd-sample-cases-us-west-2"
    
    try:
        assigned_user = find_and_assign_user(BUCKET_NAME)
        
        if assigned_user:
            print("\n" + "=" * 60)
            print("   ASSIGNMENT COMPLETED")
            print("=" * 60)
            print(f"[+] Assigned user: {assigned_user}")
            
            current_username = get_current_username()
            
            # Special sync handling for test environment
            # In the main assignment function, around line where sync happens:
            if current_username == 'imagebuildertest':
                print("\n" + "-" * 40)
                print("Test environment - syncing from mount only...")
                mount_path = check_s3_mount_available()
                if mount_path:
                    # Use assigned_user (which should be like "user1") directly
                    source_dir = mount_path / assigned_user  # This should be mount_path / "user1"
                    local_dir = sync_mount_to_local_from_path(source_dir, assigned_user)
                    print(f"[+] Test user data synced from mount: {local_dir}")
                else:
                    print("[!] Mount not available for test environment")
                    # Create the user{i} directory, not defaultprofileuser
                    local_dir = Path(f"C:/AppStreamUsers/{assigned_user}")
                    local_dir.mkdir(parents=True, exist_ok=True)
            else:
                # Production sync - try S3 first, then mount fallback
                s3_client = initialize_s3_client(BUCKET_NAME)
                if s3_client is not None:
                    print("\n" + "-" * 40)
                    print("Syncing from S3...")
                    local_dir = sync_s3_to_local(BUCKET_NAME, s3_client, assigned_user)
                else:
                    mount_path = check_s3_mount_available()
                    if mount_path:
                        print("\n" + "-" * 40)
                        print("Syncing from mount...")
                        # Use the same function as test environment
                        source_dir = mount_path / assigned_user
                        local_dir = sync_mount_to_local_from_path(source_dir, assigned_user)
                    else:
                        print("[!] Neither S3 nor mount available - creating empty user dir")
                        local_dir = Path(f"C:/AppStreamUsers/{assigned_user}")
                        local_dir.mkdir(parents=True, exist_ok=True)
            
            # Update configuration
            if update_prep_seg_yaml(assigned_user):
                print(f"[+] Configuration updated for user folder: {assigned_user}")
            
            # Output for batch parsing - use the user folder name
            print(f"\nASSIGNED_USER={assigned_user}")
            print(f"USER_HOME_DIR=C:/AppStreamUsers/{assigned_user}")
            
        else:
            print("\n[X] Failed to assign user")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n[X] Unexpected error: {e}")
        sys.exit(1)