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
file_name = os.getenv("EOS")
bucket_name = os.getenv("bucket_name")

# Read EOS data from S3
s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secret)
try:
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
except FileNotFoundError as e:
    log_error(e)
    raise

# Rename columns
df = df.rename(columns={
    'Subject Number': 'subject',
    'EOS Date': 'DSSTDAT_EOS',
    'Completed': 'DSCOMP_EOS',
    'Reason Non-Completion': 'DSNCOMP_EOS',
    'Death date': 'DTHDAT_EOS',
    'Lost to FUP date': 'LTFDAT'
})

# Split DSNCOMP_EOS if it contains a comma (reason in second part)
split_columns = df['DSNCOMP_EOS'].str.split(',', expand=True)
df['DSNCOMP_EOS'] = split_columns[0].str.strip()
df['reason'] = split_columns[1].str.strip() if split_columns.shape[1] > 1 else ""

# Map values
df["DSNCOMP_EOS"] = df["DSNCOMP_EOS"].map({
    "Lost to Follow-Up": "LOST TO FOLLOW-UP",
    "Death": "DEATH",
    "Withdrawal by Subject": "WITHDRAWAL BY SUBJECT",
    "other": "OTHER"
})
df["DSCOMP_EOS"] = df["DSCOMP_EOS"].map({"Yes": "Y", "No": "N"})

def preprocess_dataframe(df):
    df["DSSTDAT_EOS"] = df["DSSTDAT_EOS"].apply(convert_date_format)
    df["LTFDAT"] = df["LTFDAT"].apply(convert_date_format)
    df["DTHDAT_EOS"] = df["DTHDAT_EOS"].apply(convert_date_format)
    return df

df = preprocess_dataframe(df)
df = df.fillna("")

# Prepare the data to be sent
json_payloads = []

for _, row in df.iterrows():
    subject = row['subject']
    itemgroup = {
        "itemgroup_name": "ig_EOS_01_A",
        "itemgroup_sequence": 1,
        "items": [
            {"item_name": "DSSTDAT_EOS", "value": row['DSSTDAT_EOS']},
            {"item_name": "DSCOMP_EOS", "value": row['DSCOMP_EOS']},
            {"item_name": "DSNCOMP_EOS", "value": row['DSNCOMP_EOS']},
            {"item_name": "DSNCOMP_SPECIFY", "value": row['reason']},
            {"item_name": "DTHDAT_EOS", "value": row['DTHDAT_EOS']},
            {"item_name": "LTFDAT", "value": row['LTFDAT']}
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
            "form_name": "EOS_01_v002",
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