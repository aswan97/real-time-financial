import boto3
from botocore.exceptions import ClientError

import os 
from datetime import datetime
import io

import pandas as pd

# Defing the s3 client
s3_client = boto3.client(
    "s3",
    region_name = os.environ.get("AWS_REGION"),
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
)

# Uploading a local file to s3
def upload_file(local_path: str, bucket: str, filename: str) -> bool:
    try:
        filename = 'features_' + datetime.now().strftime("%Y%m%d%H%M%S") + '.csv'
        s3_client.upload_file(local_path, bucket, filename)
        print(f"Uploaded {local_path} → s3://{bucket}/{filename}")
        return True
    except ClientError as e:
        print(f"Upload failed: {e}")
        return False

# Downloading a file and converting it to a dataframe
def download_file_to_df(bucket: str, key: str):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
        return df 
    except Exception as e:
        print(f"The download and conversion failed: {e}")
        return False
    
# Check if a file exists in the s3 bucket
def check_file_exists(bucket: str, prefix: str):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' not in response:
            raise FileNotFoundError(f"No files with the prefix: {prefix} found in {bucket}")
        
        files = [obj for obj in response['Contents'] if not obj['Key'].endswith('/')]
        latest_file = max(files, key= lambda x: x['LastModified'])

        latest_key = latest_file['Key']
        return latest_key
    except Exception as e:
        print(f"Unable to check for the file in the s3 bucket: {e}")
        return False
        
    
