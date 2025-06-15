import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd
import boto3
from dateConv import convert_date_format
from error_log import FormDataError, APIError, FileNotFoundError, InvalidSessionIDError, log_error
from io import StringIO
from api_utils import import_form

# Load environment variables from .env file
load_dotenv()

API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
SESSION_FILE = "session_id.txt"
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")
aws_access = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
file_name = os.getenv("IE")
bucket_name = os.getenv("bucket_name")

# Read inclusion/exclusion data from S3
s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secret)
try:
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
except FileNotFoundError as e:
    log_error(e)
    raise

# Rename columns and map values
df = df.rename(columns={
    'Subject Number': 'subject',  
    'CTP Version': 'CTPNUMG',
    'Criteria Meet': 'IEYN',
    'Criterion Category': 'IECAT',
    'Criterion Number': 'IENUM',
})
df["IEYN"] = df["IEYN"].map({"Yes": "Y", "No": "N"})
df = df.fillna("")

def preprocess_dataframe(df):
    # Add any date conversion or additional preprocessing here if needed
    return df

df = preprocess_dataframe(df)

# Group by subject (and CTPNUMG if needed)
grouped = df.groupby(['subject', 'CTPNUMG'])

json_payloads = []

for (subject, CTPNUMG), group in grouped:
    # Use the first row for ig_IE_01_A (non-repeating)
    first_row = group.iloc[0]
    IEYN = first_row['IEYN']

    itemgroups = [
        {
            "itemgroup_name": "ig_IE_01_A",
            "itemgroup_sequence": 1,
            "items": [
                {"item_name": "CTPNUMG", "value": CTPNUMG},
                {"item_name": "IEYN", "value": IEYN}
            ]
        }
    ]

    # Add repeating ig_IE_01_B itemgroups for each row with IECAT and IENUM
    for idx, row in group.iterrows():
        if pd.notna(row['IECAT']) and pd.notna(row['IENUM']):
            itemgroups.append({
                "itemgroup_name": "ig_IE_01_B",
                "itemgroup_sequence": int(idx - group.index[0] + 1),  # sequence starts at 1
                "items": [
                    {"item_name": "IECAT", "value": row['IECAT']},
                    {"item_name": "IENUM", "value": row['IENUM']}
                ]
            })

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
            "form_name": "IE_01_v001",
            "itemgroups": itemgroups
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
            msg = f"Validation error: {field} is missing in form for subject {form.get('subject')}"
            print(msg)
            log_error(msg)
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