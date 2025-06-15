import json
from dotenv import load_dotenv
import requests
import os
from dateConv import convert_date_format
import pandas as pd
import boto3
from error_log import FormDataError, APIError, FileNotFoundError, InvalidSessionIDError, log_error, check_file_exists
from io import StringIO
from api_utils import import_form

# Load environment variables
load_dotenv()

# Variables
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
SESSION_FILE = "session_id.txt"
# SESSION_FILE ='/opt/airflow/scripts/session_id.txt'
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")
aws_access = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
file_name = os.getenv("Eligibility")
bucket_name = os.getenv("bucket_name")

# Read eligibility data from S3
s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secret)
try:
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
except FileNotFoundError as e:
    log_error(e)
    raise

# Rename and map columns
df = df.rename(columns={
    'Subject Number': 'subject', 
    'Randomized': 'DSCOMP_ELIG',
    'Reason Non-Randomized': 'DSNCOMP_ELIG',
    'Randomization Date': 'DSSTDAT_ELIG',
    'Randomization Number': 'RANDNO'
})
df["DSCOMP_ELIG"] = df["DSCOMP_ELIG"].map({"Yes": "Y", "No": "N"})
df["DSNCOMP_ELIG"] = df["DSNCOMP_ELIG"].map({
    "Adverse Event": "ADVERSE EVENT",
    "Screen Failure": "SCREEN FAILURE",
    "Screened in Error": "SCREENED IN ERROR",
    "Lost to Follow-Up": "LOST TO FOLLOW-UP",
    "Withdrawal by Subject": "WITHDRAWAL BY SUBJECT",
    "Other": "OTHER"
})

def preprocess_dataframe(df):
    df["DSSTDAT_ELIG"] = df["DSSTDAT_ELIG"].apply(convert_date_format)
    return df

df = preprocess_dataframe(df)
df = df.fillna("")

# Build payloads: one form per eligibility record
json_payloads = []

for idx, row in df.iterrows():
    subject = row['subject']
    itemgroup = {
        "itemgroup_name": "ig_ELIG_02_A",
        "itemgroup_sequence": 1,
        "items": [
            {"item_name": "DSCOMP_ELIG", "value": row['DSCOMP_ELIG']},
            {"item_name": "DSNCOMP_ELIG", "value": row['DSNCOMP_ELIG']},
            {"item_name": "DSSTDAT_ELIG", "value": row['DSSTDAT_ELIG']},
            {"item_name": "RANDNO", "value": row['RANDNO']}
        ]
    }

    json_body = {
        "study_name": study_name,
        "reopen": True,
        "submit": True,
        "change_reason": "Updated by the integration",
        "externally_owned": True,
        "form": {
            "study_country": study_country,
            "site": site,
            "subject": subject,
            "eventgroup_name": "eg_COMMON",
            "event_name": "ev_COMMON",
            "form_name": "ELIG_02_v001",
            "itemgroups": [itemgroup]
        }
    }
    json_payloads.append(json_body)

# API headers and endpoint
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {SESSION_ID}",
}
api_endpoint = f"{BASE_URL}/api/{API_VERSION}/app/cdm/forms/actions/setdata"

def validate_form_data(payload):
    form = payload.get("form", {})
    required_fields = ["study_country", "site", "subject", "eventgroup_name", "event_name", "form_name"]
    for field in required_fields:
        if not form.get(field):
            print(f"Validation error: {field} is missing in form for subject {form.get('subject')}")
            return False
    return True

# Import forms with error handling and user feedback
for payload in json_payloads:
    try:
        import_form(payload, api_endpoint, headers, validate_form_data)
    except (FormDataError, APIError, InvalidSessionIDError) as e:
        print(f"Error: {e}")
        log_error(e)
    except Exception as e:
        print(f"Unexpected error: {e}")
        log_error(e)