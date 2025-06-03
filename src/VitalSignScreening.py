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
# SESSION_FILE = "session_id.txt"
SESSION_FILE ='/opt/airflow/scripts/session_id.txt'
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")
aws_access = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
file_name = os.getenv("VitalSignScreening")
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
    'Date of Measurement': 'VSDAT',
    'Time of Measurement': 'VSTIM',
    'Height': 'HEIGHT',
    'Weight': 'WEIGHT',
    'Pulse Rate': 'PULSE',
    'Systolic Blood Pressure': 'SYSBP',
    'Diastolic Blood Pressure': 'DIABP',
    'Respiratory Rate': 'RESP',
    'Temperature': 'TEMP'
    # Uncomment if you want to include these
    # 'Oxygen Saturation': 'OXYSAT',
})

def preprocess_dataframe(df):
    df["VSDAT"] = df["VSDAT"].apply(convert_date_format)
    return df

df = preprocess_dataframe(df)
df = df.fillna("")

# Prepare the data to be sent
json_payloads = []

for _, row in df.iterrows():
    subject = row['subject']
    itemgroup = {
        "itemgroup_name": "ig_VS_01_A",
        "itemgroup_sequence": 1,
        "items": [
            {"item_name": "VSDAT", "value": row['VSDAT']},
            # Uncomment if you want to include time
            # {"item_name": "VSTIM", "value": row['VSTIM']},
            {"item_name": "HEIGHT", "value": row['HEIGHT'], "unit_value": "Centimeter"},
            {"item_name": "WEIGHT", "value": row['WEIGHT'], "unit_value": "Kilogram"},
            {"item_name": "PULSE", "value": row['PULSE'], "unit_value": "Beats per Minute"},
            {"item_name": "SYSBP", "value": row['SYSBP'], "unit_value": "Millimeter of Mercury"},
            {"item_name": "DIABP", "value": row['DIABP'], "unit_value": "Millimeter of Mercury"},
            # Uncomment if you want to include these
            # {"item_name": "RESP", "value": row['RESP'], "unit_value": "Breaths per Minute"},
            # {"item_name": "TEMP", "value": row['TEMP'], "unit_value": "Degree-Celsius"}
            # {"item_name": "OXYSAT", "value": row['OXYSAT'], "unit_value": "Percent"}
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
            "eventgroup_name": "eg_SCREEN",
            "event_name": "ev_V01",
            "form_name": "VS_01_SCREEN",
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