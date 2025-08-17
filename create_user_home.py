import hashlib
import os
import sys
import json
from pathlib import Path
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import yaml

def get_user_hash(username):
    """Create a consistent hash for the user"""
    return hashlib.sha256(username.encode()).hexdigest()[:12]

def initialize_s3_client(bucket_name):
    """
    Initialize S3 client with automatic region detection using personal credentials
    Uses the same strategy as the admin distribution tool
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
                
                # Verify we're using personal credentials, not instance role
                try:
                    sts_client = boto3.client('sts', region_name=region)
                    identity = sts_client.get_caller_identity()
                    if 'assumed-role/PhotonImageBuilderInstance' in identity['Arn']:
                        print("[!] WARNING: Still using instance role instead of personal credentials!")
                        print("    This may cause permission issues.")
                    else:
                        print(f"[+] Using personal credentials: {identity['Arn'].split('/')[-1]}")
                except Exception:
                    pass  # Identity check is optional
                
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
        print("5. Try setting environment variables:")
        print("   set AWS_ACCESS_KEY_ID=your_key")
        print("   set AWS_SECRET_ACCESS_KEY=your_secret")
        raise

def create_s3_user_folder(username, bucket_name, s3_client):
    """Create user folder structure in S3 bucket under ibd_root/"""
    user_hash = get_user_hash(username)
    s3_prefix = f"ibd_root/{user_hash}/"
    
    try:
        # Check if the folder already exists by trying to list objects with the prefix
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=s3_prefix,
            MaxKeys=1
        )
        
        folder_exists = 'Contents' in response and len(response['Contents']) > 0
        
        if not folder_exists:
            # Create the folder by uploading an empty object with trailing slash
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_prefix,
                Body=b''
            )
            print(f"[+] Created S3 folder: s3://{bucket_name}/{s3_prefix}")
            
            # Also create a metadata file to track folder creation
            metadata = {
                "username": username,
                "user_hash": user_hash,
                "created_at": datetime.now().isoformat(),
                "created_by": "appstream_user_setup",
                "local_path": f"C:/AppStreamUsers/{user_hash}"
            }
            
            metadata_key = f"{s3_prefix}.folder_info.json"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType='application/json'
            )
            
            return False  # New folder created
        else:
            print(f"[+] S3 folder already exists: s3://{bucket_name}/{s3_prefix}")
            return True  # Folder already existed
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"[X] Error creating S3 folder: {error_code}")
        
        if error_code == 'AccessDenied':
            print("Solution: Contact your AWS administrator to add these permissions:")
            print("- s3:PutObject for the ibd_root/ prefix")
            print("- s3:ListBucket for the ibd_root/ prefix")
        raise
    except NoCredentialsError:
        print("[X] Error: AWS credentials not found. Please configure your AWS credentials.")
        raise

def sync_s3_to_local_boto3(username, bucket_name, s3_client, local_home_dir):
    """Sync S3 folder to local directory using boto3"""
    user_hash = get_user_hash(username)
    s3_prefix = f"ibd_root/{user_hash}/"
    
    try:
        print(f"[DOWN] Syncing from s3://{bucket_name}/{s3_prefix} to {local_home_dir}")
        
        # Ensure local directory exists
        local_home_dir.mkdir(parents=True, exist_ok=True)
        
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
                
                # Skip the folder marker and metadata files
                if s3_key.endswith('/') or '.folder_info.json' in s3_key:
                    continue
                
                # Calculate local file path
                relative_path = s3_key[len(s3_prefix):]
                local_file_path = local_home_dir / relative_path
                
                # Create local directory structure if needed
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Check if we need to download (file doesn't exist or is older)
                should_download = True
                if local_file_path.exists():
                    local_mtime = datetime.fromtimestamp(local_file_path.stat().st_mtime)
                    s3_mtime = obj['LastModified'].replace(tzinfo=None)
                    should_download = s3_mtime > local_mtime
                
                if should_download:
                    # Download the file
                    s3_client.download_file(bucket_name, s3_key, str(local_file_path))
                    downloaded_files += 1
                    print(f"  [+] Downloaded: {relative_path}")
                else:
                    skipped_files += 1
        
        if downloaded_files == 0 and skipped_files == 0:
            print("[+] No files to sync - S3 folder is empty")
        elif downloaded_files == 0:
            print(f"[+] All {skipped_files} files are up to date")
        else:
            print(f"[+] Downloaded {downloaded_files} files, {skipped_files} already up to date")
        
        print("[+] S3 to local sync completed successfully")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"[X] Error syncing from S3: {error_code}")
        raise
    except Exception as e:
        print(f"[X] Error syncing from S3: {e}")
        raise

def sync_local_to_s3_boto3(username, bucket_name, s3_client, local_home_dir):
    """Sync local directory to S3 folder using boto3"""
    user_hash = get_user_hash(username)
    s3_prefix = f"ibd_root/{user_hash}/"
    
    try:
        print(f"[UP] Syncing from {local_home_dir} to s3://{bucket_name}/{s3_prefix}")
        
        if not local_home_dir.exists():
            print("[+] No local files to sync")
            return
        
        uploaded_files = 0
        skipped_files = 0
        
        # Recursively walk through local directory
        for local_file_path in local_home_dir.rglob('*'):
            if local_file_path.is_file():
                # Skip temporary and system files
                if local_file_path.name.lower() in ['thumbs.db', '.ds_store'] or \
                   local_file_path.suffix.lower() in ['.tmp', '.temp']:
                    continue
                
                # Calculate S3 key
                relative_path = local_file_path.relative_to(local_home_dir)
                s3_key = s3_prefix + str(relative_path).replace('\\', '/')
                
                # Check if we need to upload (file doesn't exist in S3 or is newer)
                should_upload = True
                try:
                    s3_obj = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                    local_mtime = datetime.fromtimestamp(local_file_path.stat().st_mtime)
                    s3_mtime = s3_obj['LastModified'].replace(tzinfo=None)
                    should_upload = local_mtime > s3_mtime
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        should_upload = True  # File doesn't exist in S3
                    else:
                        raise
                
                if should_upload:
                    # Upload the file
                    s3_client.upload_file(str(local_file_path), bucket_name, s3_key)
                    uploaded_files += 1
                    print(f"  [+] Uploaded: {relative_path}")
                else:
                    skipped_files += 1
        
        if uploaded_files == 0 and skipped_files == 0:
            print("[+] No files to sync - local folder is empty")
        elif uploaded_files == 0:
            print(f"[+] All {skipped_files} files are up to date in S3")
        else:
            print(f"[+] Uploaded {uploaded_files} files, {skipped_files} already up to date")
            
        print("[+] Local to S3 sync completed successfully")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"[X] Error syncing to S3: {error_code}")
        raise
    except Exception as e:
        print(f"[X] Error syncing to S3: {e}")
        raise

def create_user_home_with_s3(username, bucket_name, sync_direction="both"):
    """
    Create user home directory locally and on S3, with synchronization
    
    Args:
        username: The username to create home for
        bucket_name: S3 bucket name
        sync_direction: "both", "s3_to_local", "local_to_s3", or "none"
    """
    print("=" * 60)
    print("   APPSTREAM USER HOME SETUP WITH S3")
    print("=" * 60)
    print(f"Username: {username}")
    print(f"Target bucket: {bucket_name}")
    print(f"Sync direction: {sync_direction}")
    print("")
    
    try:
        # Step 1: Create local home directory
        user_hash = get_user_hash(username)
        home_dir = Path(f"C:/AppStreamUsers/{user_hash}")
        home_dir.mkdir(parents=True, exist_ok=True)
        print(f"[+] Local home directory ready: {home_dir}")

        # Path for YAML mapping file
        mapping_file = home_dir / "user_mapping.yaml"

        # Save mapping in YAML
        mapping_data = {
            "username": username,
            "user_hash": user_hash
        }
        with open(mapping_file, "w") as f:
            yaml.safe_dump(mapping_data, f)
        
        # Step 2: Initialize S3 connection
        print("\n" + "-" * 40)
        print("Connecting to S3...")
        s3_client = initialize_s3_client(bucket_name)
        
        # Step 3: Create S3 folder and check if it already existed
        print("\n" + "-" * 40)
        print("Setting up S3 folder...")
        s3_folder_existed = create_s3_user_folder(username, bucket_name, s3_client)
        
        # Step 4: Synchronization logic
        print("\n" + "-" * 40)
        print("Synchronizing folders...")
        
        if sync_direction in ["both", "s3_to_local"] and s3_folder_existed:
            # If S3 folder already existed, sync from S3 to local first
            sync_s3_to_local_boto3(username, bucket_name, s3_client, home_dir)
        
        if sync_direction in ["both", "local_to_s3"]:
            # Sync local changes back to S3
            sync_local_to_s3_boto3(username, bucket_name, s3_client, home_dir)
        
        print("\n" + "=" * 60)
        print("   SETUP COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"[+] User: {username}")
        print(f"[+] Hash: {user_hash}")
        print(f"[+] Local path: {home_dir}")
        print(f"[+] S3 path: s3://{bucket_name}/ibd_root/{user_hash}/")
        
        return home_dir
        
    except Exception as e:
        print(f"\n[X] Error in create_user_home_with_s3: {e}")
        raise

def create_user_home(username):
    """Original function for backward compatibility"""
    user_hash = get_user_hash(username)
    home_dir = Path(f"C:/AppStreamUsers/{user_hash}")
    home_dir.mkdir(parents=True, exist_ok=True)

    # Path for YAML mapping file
    mapping_file = home_dir / "user_mapping.yaml"

    # Save mapping in YAML
    mapping_data = {
        "username": username,
        "user_hash": user_hash
    }
    with open(mapping_file, "w") as f:
        yaml.safe_dump(mapping_data, f)

    return home_dir

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <username> [bucket_name] [sync_direction]")
        print("sync_direction options: both, s3_to_local, local_to_s3, none")
        print("\nExamples:")
        print("  python script.py john.doe")
        print("  python script.py john.doe hoda2-ibd-sample-cases-us-west-2")
        print("  python script.py john.doe hoda2-ibd-sample-cases-us-west-2 both")
        sys.exit(1)
    
    username = sys.argv[1]
    bucket_name = sys.argv[2] if len(sys.argv) > 2 else None
    sync_direction = sys.argv[3] if len(sys.argv) > 3 else "both"
    
    try:
        if bucket_name:
            home = create_user_home_with_s3(username, bucket_name, sync_direction)
        else:
            # Fallback to original functionality if no bucket specified
            home = create_user_home(username)
            print(f"[+] Local-only user home created: {home}")
        
        # Output the path in a format that's easy for batch files to parse
        print(f"\nUser home path: {home}")
        print(f"BATCH_USER_HOME={home}")  # Easy to parse format for batch files
        
    except KeyboardInterrupt:
        print("\n\n[!] Operation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n[X] Failed to create user home: {e}")
        sys.exit(1)
    
    # Brief pause to show results
    if len(sys.argv) > 2:  # Only for S3 operations
        import time
        time.sleep(2)