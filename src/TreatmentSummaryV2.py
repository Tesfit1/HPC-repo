import json
from dotenv import load_dotenv
import requests
import os
from dateConv import convert_date_format
import pandas as pd
import boto3
from error_log import FormDataError, APIError, FileNotFoundError, InvalidSessionIDError, log_error
from io import StringIO
from api_utils import import_form

# Load environment variables
load_dotenv()

API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
SESSION_FILE = "./session_id.txt"
# SESSION_FILE ='/opt/airflow/scripts/session_id.txt'
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")
aws_access = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
file_name = os.getenv("TreatmentSummary")
bucket_name = os.getenv("bucket_name")

# Read screening data from S3
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
    'Subject completed': 'DSCOMP_TPS',
    'Treatment discontinuation decision': 'DSDECIS',
    # 'If other specify': 'DSDECIS_SPECIFY',
    'Primary trt discontinuation reason': 'DSREAS',
    # 'If technical problems or other specify': 'DSREAS_SPECIFY',
    'Reason Subject Not Treated': 'DSNTRTR'
    # 'Other Reason Subject Not Treated, Specify': 'DSNTRTR_SPECIFY',
    # 'Date of last adm of study medication': 'DSSTDAT_DECTPS',
    # 'Was the study medication unblinded by the site': 'DSUNBLND',
    # 'Date Medication Code Broken': 'DSSTDAT',
    # 'Reason Medication Code Broken': 'DSNCOMP',
    # 'If Other, Specify' : 'DSNCOMP_SPECIFY',
    # 'Primary AE' : 'LK_TPS_AE'

})

def preprocess_dataframe(df):
    # df["DSSTDAT_DECTPS"] = df["DSSTDAT_DECTPS"].apply(convert_date_format)
    # df["DSSTDAT"] = df["DSSTDAT"].apply(convert_date_format)

    return df

df = preprocess_dataframe(df)
df = df.fillna("")

# Prepare the data to be sent
json_payloads = []

for _, row in df.iterrows():
    subject = row['subject']
    itemgroup = {
        "itemgroup_name": "ig_TPS_03_A",
        "itemgroup_sequence": 1,
        "items": [
            {"item_name": "DSCOMP_TPS", "value": row['DSCOMP_TPS']},
            {"item_name": "DSDECIS", "value": row['DSDECIS']},
            # {"item_name": "DSDECIS_SPECIFY", "value": row['DSDECIS_SPECIFY']},
            {"item_name": "DSREAS", "value": row['DSREAS']},
            # {"item_name": "DSREAS_SPECIFY", "value": row['DSREAS_SPECIFY']},
            {"item_name": "DSNTRTR", "value": row['DSNTRTR']},
            # {"item_name": "DSNTRTR_SPECIFY", "value": row['DSNTRTR_SPECIFY']},
            # {"item_name": "DSSTDAT_DECTPS", "value": row['DSSTDAT_DECTPS']},
            # {"item_name": "DSUNBLND", "value": row['DSUNBLND']},
            # {"item_name": "DSSTDAT", "value": row['DSSTDAT']},
            # {"item_name": "DSNCOMP", "value": row['DSNCOMP']}
            # {"item_name": "DSNCOMP_SPECIFY", "value": row['DSNCOMP_SPECIFY']}
            # {"item_name": "LK_TPS_AE", "value": row['LK_TPS_AE']}
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
            "form_name": "TPS_03_VISIT2",
            "itemgroups": [itemgroup]
        }
    }
    json_payloads.append(json_body)

    print("Sample payload:", json.dumps(json_payloads[0], indent=2))

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