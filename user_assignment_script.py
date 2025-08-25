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
    """
    Initialize S3 client with automatic region detection using personal credentials
    """
    try:
        # CRITICAL: Disable EC2 metadata to force personal credentials
        os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'
        
        print("[*] Using personal AWS credentials (EC2 metadata disabled)")
        
        # List of regions to try (based on bucket name, us-west-2 is most likely)
        regions_to_try = [
            'us-west-2',      # Most likely based on bucket name
            'us-east-1',      # Most common default
            'ca-west-1',      # Canada West (Calgary)
            'ca-central-1',   # Canada Central (Montreal)
            'us-west-1',      # US West (N. California)
            'us-east-2',      # US East (Ohio)
            'eu-west-1',      # Europe (Ireland)
        ]
        
        print("Searching for bucket across regions...")
        
        for region in regions_to_try:
            try:
                print(f"  Testing {region}... ", end="", flush=True)
                
                # Create client for this region with personal credentials
                test_client = boto3.client('s3', region_name=region)
                
                # Test bucket access using head_bucket (doesn't require listing permissions)
                test_client.head_bucket(Bucket=bucket_name)
                
                # Success! Use this region
                print("[OK] FOUND!")
                print(f"[+] S3 connection established in region: {region}")
                print(f"[+] Bucket '{bucket_name}' is accessible")
                
                # Set environment variable for consistency
                os.environ['AWS_DEFAULT_REGION'] = region
                
                return test_client
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    print("[X] Not found")
                    continue
                elif error_code == '403':
                    print("[X] Access denied")
                    continue
                elif error_code == '400':
                    print("[!] Bad request")
                    continue
                else:
                    print(f"[X] {error_code}")
                    continue
            except Exception as e:
                print(f"[X] Error")
                continue
        
        # If no region worked, show error
        raise Exception(f"Bucket '{bucket_name}' not accessible in any tested region")
        
    except Exception as e:
        print(f"\n[X] Could not connect to S3: {e}")
        print("\n[*] Troubleshooting steps:")
        print("1. Ensure you have personal AWS credentials configured")
        print("2. Run: aws configure list (if AWS CLI is available)")
        print("3. Verify bucket name is correct")
        print("4. Check AWS account and permissions")
        raise

def get_current_username():
    """Get the current username from various sources"""
    # Try different environment variables
    username = (os.environ.get('USERNAME') or 
               os.environ.get('USER') or 
               os.environ.get('APPSTREAM_USER') or
               'unknown_user')
    return username.lower()

def check_local_user_file():
    """Check if user.txt exists in the application directory"""
    user_file = Path("C:/Scripts/ibd_labeling_local_1-main/user.txt")
    if user_file.exists():
        try:
            with open(user_file, 'r') as f:
                assigned_user = f.read().strip()
            print(f"[+] Found local user.txt - assigned to: {assigned_user}")
            return assigned_user
        except Exception as e:
            print(f"[!] Error reading user.txt: {e}")
            return None
    else:
        print("[*] No local user.txt found in C:/Scripts/ibd_labeling_local_1-main/")
        return None

def save_local_user_file(assigned_user):
    """Save the assigned user to local user.txt file in application directory"""
    try:
        user_file = Path("C:/Scripts/ibd_labeling_local_1-main/user.txt")
        # Ensure directory exists
        user_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(user_file, 'w') as f:
            f.write(assigned_user)
        print(f"[+] Saved user assignment to C:/Scripts/ibd_labeling_local_1-main/user.txt: {assigned_user}")
        return True
    except Exception as e:
        print(f"[X] Error saving user.txt: {e}")
        return False

def list_user_folders(bucket_name, s3_client):
    """List all user{i} folders in ibd_root/"""
    try:
        print("[*] Scanning ibd_root/ for user folders...")
        
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
        
        # Sort by user number
        user_folders.sort(key=lambda x: int(x[4:]))
        
        print(f"[+] Found {len(user_folders)} user folders: {user_folders}")
        return user_folders
        
    except ClientError as e:
        print(f"[X] Error listing user folders: {e.response['Error']['Code']}")
        return []

def check_user_taken(bucket_name, s3_client, user_folder):
    """Check if a user folder is taken by looking for taken_by.txt"""
    try:
        taken_by_key = f"ibd_root/{user_folder}/taken_by.txt"
        
        # Try to get the taken_by.txt file
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
            return True, "error"  # Assume taken if we can't check

def claim_user_folder(bucket_name, s3_client, user_folder, current_username):
    """Claim a user folder by creating taken_by.txt"""
    try:
        taken_by_key = f"ibd_root/{user_folder}/taken_by.txt"
        
        # Create content with timestamp
        content = f"{current_username}\nClaimed at: {datetime.now().isoformat()}"
        
        # Upload the taken_by.txt file
        s3_client.put_object(
            Bucket=bucket_name,
            Key=taken_by_key,
            Body=content,
            ContentType='text/plain'
        )
        
        print(f"[+] Successfully claimed {user_folder} for {current_username}")
        return True
        
    except ClientError as e:
        print(f"[X] Error claiming {user_folder}: {e.response['Error']['Code']}")
        return False

def sync_s3_to_local(bucket_name, s3_client, assigned_user):
    """Sync S3 user folder to local AppStreamUsers directory"""
    s3_prefix = f"ibd_root/{assigned_user}/"
    local_dir = Path(f"C:/AppStreamUsers/{assigned_user}")
    
    try:
        print(f"[*] Syncing S3 folder to local: {local_dir}")
        
        # Create local directory
        local_dir.mkdir(parents=True, exist_ok=True)
        
        # List all objects in the S3 prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)
        
        downloaded_files = 0
        skipped_files = 0
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                s3_key = obj['Key']
                
                # Skip the folder marker and taken_by.txt
                if s3_key.endswith('/') or s3_key.endswith('taken_by.txt'):
                    continue
                
                # Calculate local file path
                relative_path = s3_key[len(s3_prefix):]
                local_file_path = local_dir / relative_path
                
                # Create local directory structure if needed
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Check if we need to download
                should_download = True
                if local_file_path.exists():
                    local_mtime = datetime.fromtimestamp(local_file_path.stat().st_mtime)
                    s3_mtime = obj['LastModified'].replace(tzinfo=None)
                    should_download = s3_mtime > local_mtime
                
                if should_download:
                    s3_client.download_file(bucket_name, s3_key, str(local_file_path))
                    downloaded_files += 1
                    print(f"  [+] Downloaded: {relative_path}")
                else:
                    skipped_files += 1
        
        print(f"[+] Sync completed: {downloaded_files} downloaded, {skipped_files} up-to-date")
        return local_dir
        
    except ClientError as e:
        print(f"[X] Error syncing from S3: {e.response['Error']['Code']}")
        return local_dir
    except Exception as e:
        print(f"[X] Error syncing from S3: {e}")
        return local_dir

def update_prep_seg_yaml(assigned_user):
    """Update prep_seg.yaml file to set output directory to user's folder"""
    yaml_file = Path("C:/Scripts/ibd_labeling_local_1-main/prep_seg.yaml")
    
    try:
        if not yaml_file.exists():
            print(f"[!] prep_seg.yaml not found at {yaml_file}")
            return False
        
        # Read the YAML file
        with open(yaml_file, 'r') as f:
            yaml_data = yaml.safe_load(f)
        
        # Update the output directory
        new_output_dir = f"C:/AppStreamUsers/{assigned_user}"
        
        # List of possible keys for output directory
        output_keys = ['output_directory', 'output_dir', 'outputDirectory', 'output_path', 'outputPath', 'output directory']
        
        # Step 1: Remove all existing output directory entries
        removed_keys = []
        for key in list(yaml_data.keys()):  # Use list() to avoid dict size change during iteration
            if key in output_keys:
                del yaml_data[key]
                removed_keys.append(key)
                print(f"[+] Removed existing {key}: {yaml_data.get(key, 'N/A')}")
        
        # Also check and remove from nested structures
        for section in ['paths', 'config', 'settings']:
            if section in yaml_data:
                for key in list(yaml_data[section].keys()):
                    if key in output_keys:
                        del yaml_data[section][key]
                        removed_keys.append(f"{section}.{key}")
                        print(f"[+] Removed existing {section}.{key}")
        
        # Step 2: Add the new output_directory
        yaml_data['output_directory'] = new_output_dir
        print(f"[+] Added output_directory to prep_seg.yaml: {new_output_dir}")
        
        # Write back to file
        with open(yaml_file, 'w') as f:
            yaml.dump(yaml_data, f, default_flow_style=False, indent=2)
        
        print(f"[+] prep_seg.yaml updated successfully")
        if removed_keys:
            print(f"[*] Removed {len(removed_keys)} old output directory entries")
        
        return True
        
    except Exception as e:
        print(f"[X] Error updating prep_seg.yaml: {e}")
        return False

def get_user_assignment_info(bucket_name, assigned_user):
    """Get additional info about the assigned user"""
    if not assigned_user:
        return None
    
    try:
        s3_client = initialize_s3_client(bucket_name)
        
        # Check if worklist exists for this user
        worklist_key = f"ibd_root/{assigned_user}/worklist.{assigned_user}.yaml"
        
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=worklist_key)
            print(f"[+] Worklist found for {assigned_user}")
            
            # You could parse the YAML here if needed
            worklist_content = response['Body'].read().decode('utf-8')
            print(f"[*] Worklist preview (first 200 chars):")
            print(worklist_content[:200] + "..." if len(worklist_content) > 200 else worklist_content)
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"[!] No worklist found for {assigned_user}")
            else:
                print(f"[!] Error checking worklist: {e.response['Error']['Code']}")
        
        return assigned_user
        
    except Exception as e:
        print(f"[!] Error getting assignment info: {e}")
        return assigned_user

def find_and_assign_user(bucket_name):
    """
    Main function to find and assign user
    1. Check local user.txt
    2. If not found, scan S3 for available user folder
    3. Claim first available folder
    4. Save assignment locally
    """
    print("=" * 60)
    print("   USER ASSIGNMENT SYSTEM")
    print("=" * 60)
    print(f"Target bucket: {bucket_name}")
    print("")
    
    # Step 1: Check if user is already assigned locally
    assigned_user = check_local_user_file()
    if assigned_user:
        print(f"[+] User already assigned: {assigned_user}")
        # Don't return here - continue to sync and update YAML
    else:
        # Step 2: Get current username
        current_username = get_current_username()
        print(f"[*] Current username: {current_username}")
        
        # Step 3: Initialize S3 connection
        print("\n" + "-" * 40)
        print("Connecting to S3...")
        try:
            s3_client = initialize_s3_client(bucket_name)
        except Exception as e:
            print(f"[X] Failed to connect to S3: {e}")
            return None
        
        # Step 4: List all user folders
        print("\n" + "-" * 40)
        print("Scanning for user folders...")
        user_folders = list_user_folders(bucket_name, s3_client)
        
        if not user_folders:
            print("[X] No user folders found in ibd_root/")
            return None
        
        # Step 5: Find first available user folder
        print("\n" + "-" * 40)
        print("Checking availability...")
        
        for user_folder in user_folders:
            is_taken, taken_by = check_user_taken(bucket_name, s3_client, user_folder)
            
            if not is_taken:
                # Found an available folder - claim it
                print(f"[+] Found available folder: {user_folder}")
                
                if claim_user_folder(bucket_name, s3_client, user_folder, current_username):
                    # Save assignment locally
                    if save_local_user_file(user_folder):
                        print(f"\n[+] Successfully assigned to {user_folder}")
                        assigned_user = user_folder
                        break
                    else:
                        print(f"[!] Claimed {user_folder} but failed to save locally")
                        assigned_user = user_folder
                        break
                else:
                    print(f"[X] Failed to claim {user_folder}")
                    continue
        
        if not assigned_user:
            print("[X] No available user folders found")
            return None
    
    return assigned_user

if __name__ == "__main__":
    # Configuration - hardcoded bucket name
    BUCKET_NAME = "hoda2-ibd-sample-cases-us-west-2"
    
    try:
        # Find and assign user
        assigned_user = find_and_assign_user(BUCKET_NAME)
        
        if assigned_user:
            print("\n" + "=" * 60)
            print("   ASSIGNMENT COMPLETED")
            print("=" * 60)
            print(f"[+] Assigned user: {assigned_user}")
            print(f"[+] Local file: C:/Scripts/ibd_labeling_local_1-main/user.txt")
            print(f"[+] S3 claim: ibd_root/{assigned_user}/taken_by.txt")
            
            # Step 1: Sync S3 data to local folder
            print("\n" + "-" * 40)
            print("Syncing S3 data to local folder...")
            try:
                s3_client = initialize_s3_client(BUCKET_NAME)
                local_dir = sync_s3_to_local(BUCKET_NAME, s3_client, assigned_user)
                print(f"[+] User data synced to: {local_dir}")
            except Exception as e:
                print(f"[!] Error during S3 sync: {e}")
                # Continue anyway - sync is not critical for assignment
            
            # Step 2: Update prep_seg.yaml file
            print("\n" + "-" * 40)
            print("Updating prep_seg.yaml configuration...")
            if update_prep_seg_yaml(assigned_user):
                print(f"[+] prep_seg.yaml configured for user: {assigned_user}")
            else:
                print(f"[!] Failed to update prep_seg.yaml (application may still work)")
            
            # Get additional info
            get_user_assignment_info(BUCKET_NAME, assigned_user)
            
            # Output for easy parsing by other scripts
            print(f"\nASSIGNED_USER={assigned_user}")
            print(f"USER_HOME_DIR=C:/AppStreamUsers/{assigned_user}")
            
        else:
            print("\n[X] Failed to assign user")
            sys.exit(1)
        
    except KeyboardInterrupt:
        print("\n\n[!] Operation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n[X] Unexpected error: {e}")
        sys.exit(1)