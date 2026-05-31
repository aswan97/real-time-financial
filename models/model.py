import os 
import sys 

# To excute this script anywhere
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Model libraries
#import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler
import joblib

# Data manipulation
import pandas as pd 
import numpy as np

# Cloud
import boto3
from botocore.exceptions import ClientError

# Utilities
from src.common.utilities import download_file_to_df, check_file_exists
from dotenv import load_dotenv

load_dotenv()

# Constants
BUCKET = os.environ.get("S3_BUCKET")

# Checking the s3 bucket for the file
try:
    key = check_file_exists(BUCKET, prefix="features")
except Exception as e:
    print(f"There was an error checking for the file: {e}")

if key:
    df = download_file_to_df(BUCKET, key)

print(df.head(5))

