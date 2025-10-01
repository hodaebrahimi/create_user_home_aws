#!/usr/bin/env python3
"""
Quick Python S3 Test - Compare with AWS CLI behavior
"""

import boto3
import sys
import traceback
from botocore.exceptions import ClientError, NoCredentialsError

def test_python_s3():
    """Test the exact same operation that AWS CLI performed"""
    
    bucket_name = "hoda2-ibd-sample-cases-us-west-2"
    
    print("Testing Python boto3 S3 access...")
    print(f"Bucket: {bucket_name}")
    print(f"Python version: {sys.version}")
    print("")
    
    try:
        # Test 1: Create S3 client (same as your app does)
        print("1. Creating S3 client...")
        s3_client = boto3.client('s3', region_name='us-west-2')
        print("   ✓ S3 client created successfully")
        
        # Test 2: Head bucket (basic access test)
        print("2. Testing bucket access...")
        response = s3_client.head_bucket(Bucket=bucket_name)
        print("   ✓ Bucket access successful")
        
        # Test 3: List objects (equivalent to aws s3 ls)
        print("3. Listing bucket contents...")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Delimiter='/')
        
        if 'CommonPrefixes' in response:
            print("   ✓ Found prefixes:")
            for prefix in response['CommonPrefixes']:
                folder_name = prefix['Prefix'].rstrip('/')
                print(f"     - {folder_name}/")
        
        if 'Contents' in response:
            print("   ✓ Found objects:")
            for obj in response['Contents'][:5]:  # First 5 objects
                print(f"     - {obj['Key']}")
        
        # Test 4: List ibd_root specifically (what your app needs)
        print("4. Testing ibd_root access...")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, 
            Prefix='ibd_root/', 
            Delimiter='/'
        )
        
        if 'CommonPrefixes' in response:
            print("   ✓ Found ibd_root folders:")
            for prefix in response['CommonPrefixes']:
                folder_name = prefix['Prefix'].replace('ibd_root/', '').rstrip('/')
                print(f"     - ibd_root/{folder_name}/")
        else:
            print("   ! No folders found in ibd_root/")
        
        print("\n✓ ALL TESTS PASSED - Python S3 access is working!")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        print(f"\n✗ ClientError: {error_code}")
        print(f"   Message: {error_message}")
        
        if error_code == 'AccessDenied':
            print("   This means IAM permissions issue")
        elif error_code == 'NoSuchBucket':
            print("   This means bucket doesn't exist or wrong region")
        elif error_code == 'InvalidBucketName':
            print("   This means bucket name is invalid")
        
        return False
        
    except NoCredentialsError:
        print("\n✗ NoCredentialsError: No AWS credentials found")
        print("   boto3 cannot find credentials, but AWS CLI can")
        print("   This suggests credential chain difference")
        return False
        
    except Exception as e:
        print(f"\n✗ Unexpected error: {type(e).__name__}: {e}")
        print("\nFull traceback:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("PYTHON BOTO3 S3 TEST")
    print("=" * 50)
    
    success = test_python_s3()
    
    print("\n" + "=" * 50)
    if success:
        print("RESULT: Python S3 access is WORKING")
        print("The issue is likely elsewhere in your application")
    else:
        print("RESULT: Python S3 access is FAILING")
        print("This explains why your app crashes while AWS CLI works")
    print("=" * 50)
    
    input("\nPress Enter to exit...")