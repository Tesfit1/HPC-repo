import boto3
import pandas as pd
from io import StringIO
from utils.error_log_utils import log_error


def read_s3_csv(bucket, key, aws_access_key_id, aws_secret_access_key, delimiter='|', dtype=str):
    """
    Reads a CSV file from S3 and returns a pandas DataFrame.
    """
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    response = s3.get_object(Bucket=bucket, Key=key)
    if 'Body' not in response:
        log_error(FileNotFoundError(f"File {key} not found in bucket {bucket}"))
        raise FileNotFoundError(f"File {key} not found in bucket {bucket}")
    file_content = response['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(file_content), delimiter=delimiter, dtype=dtype)