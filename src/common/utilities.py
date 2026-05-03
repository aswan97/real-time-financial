import boto3
from botocore.exceptions import ClientError
import os 
from datetime import datetime

BUCKET = os.environ["S3_BUCKET"]

# Defing the s3 client
s3_client = boto3.client(
    "s3",
    region_name = os.environ["AWS_REGION"],
    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
)

def upload_file(local_path: str, bucket: str, filename: str) -> bool:
    try:
        filename = 'features_' + datetime.now().strftime("%Y%m%d%H%M%S") + '.csv'
        s3_client.upload_file(local_path, bucket, filename)
        print(f"Uploaded {local_path} → s3://{bucket}/{filename}")
        return True
    except ClientError as e:
        print(f"Upload failed: {e}")
        return False
    
