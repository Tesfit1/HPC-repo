import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd
import boto3
from dateConv import convert_date_format
from error_log import FormDataError, APIError, FileNotFoundError, InvalidSessionIDError, log_error, check_file_exists
from io import StringIO
from api_utils import import_form

# Load environment variables
load_dotenv()

# Variables
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
SESSION_FILE = "session_id.txt"
# SESSION_FILE = '/opt/airflow/scripts/session_id.txt'
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()

print(f"Session ID: {SESSION_ID}")  # Debugging line to check SESSION_ID
study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")
aws_access = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
file_name = os.getenv("Event_IC")
bucket_name = os.getenv("bucket_name")

# Read consent data from S3
s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secret)
try:
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
except FileNotFoundError as e:
    log_error(e)
    raise

# Data preprocessing
df["Informed Consent Obtained"] = df["Informed Consent Obtained"].map({"Yes": "Y", "No": "N"})
df["Informed Consent Type"] = df["Informed Consent Type"].map({"Main": "MAIN"})
df = df.rename(columns={
    'Subject Number': 'subject',
    'Informed Consent Type': 'DSSCAT_IC',
    'Informed Consent Version ID': 'DSREFID_IC',
    'Informed Consent Obtained': 'IC',
    'Informed Consent Date': 'DSSTDAT_IC'
})

def preprocess_dataframe(df):
    df["DSSTDAT_IC"] = df["DSSTDAT_IC"].apply(convert_date_format)
    return df

df = preprocess_dataframe(df)
df = df.fillna("")

# Build payloads: one form per consent record, with form_sequence per subject
json_payloads = []
subject_form_counter = {}

for idx, row in df.iterrows():
    subject = row['subject']
    # Increment form sequence for each subject
    if subject not in subject_form_counter:
        subject_form_counter[subject] = 1
    else:
        subject_form_counter[subject] += 1
    form_sequence = subject_form_counter[subject]

    itemgroup = {
        "itemgroup_name": "ig_IC_01_A",
        "itemgroup_sequence": 1,
        "items": [
            {"item_name": "DSSCAT_IC", "value": row['DSSCAT_IC']},
            {"item_name": "DSREFID_IC", "value": row['DSREFID_IC']},
            {"item_name": "IC", "value": row['IC']},
            {"item_name": "DSSTDAT_IC", "value": row['DSSTDAT_IC']}
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
            "form_name": "IC_01_v002",
            "form_sequence": form_sequence,
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
    # Example: check required fields
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