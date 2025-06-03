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

# Load environment variables
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
file_name = os.getenv("VitalSignEOS")
bucket_name = os.getenv("bucket_name")

# Read EOS vital sign data from S3
s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secret)
try:
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
except FileNotFoundError as e:
    log_error(e)
    raise

# Ensure required columns exist before renaming
required_columns = [
    'Subject Number', 'Date of Measurements', 'Weight', 'Pulse Rate',
    'Systolic Blood Pressure', 'Diastolic Blood Pressure'
    # Add 'Respiratory Rate', 'Temperature' if needed
]
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    raise KeyError(f"Missing columns in input data: {missing_columns}")

# Rename columns
df = df.rename(columns={
    'Subject Number': 'subject',
    'Date of Measurements': 'VSDAT_2',
    # 'Weight': 'WEIGHT_2',
    'Pulse Rate': 'PULSE_2',
    'Systolic Blood Pressure': 'SYSBP_2',
    'Diastolic Blood Pressure': 'DIABP_2',
    # 'Respiratory Rate': 'RESP_2',
    # 'Temperature': 'TEMP_2'
})

def preprocess_dataframe(df):
    df["VSDAT_2"] = df["VSDAT_2"].apply(convert_date_format)
    return df

df = preprocess_dataframe(df)
df = df.fillna("")

# Prepare the data to be sent
json_payloads = []

for _, row in df.iterrows():
    subject = row['subject']
    itemgroup = {
        "itemgroup_name": "ig_VS_01_A_2",
        "itemgroup_sequence": 1,
        "items": [
            {"item_name": "VSDAT_2", "value": row['VSDAT_2']},
            # Uncomment if you want to include time
            # {"item_name": "VSTIM_1", "value": row.get('VSTIM_1', "")},
            # {"item_name": "HEIGHT", "value": row.get('HEIGHT', ""), "unit_value": "Centimeter"},
            # {"item_name": "WEIGHT_2", "value": row['WEIGHT_2'], "unit_value": "Kilogram"},
            {"item_name": "PULSE_2", "value": row['PULSE_2'], "unit_value": "Beats per Minute"},
            {"item_name": "SYSBP_2", "value": row['SYSBP_2'], "unit_value": "Millimeter of Mercury"},
            {"item_name": "DIABP_2", "value": row['DIABP_2'], "unit_value": "Millimeter of Mercury"},
            # Uncomment if you want to include these
            # {"item_name": "RESP_2", "value": row.get('RESP_2', ""), "unit_value": "Breaths per Minute"},
            # {"item_name": "TEMP_2", "value": row.get('TEMP_2', ""), "unit_value": "Degree-Celsius"}
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
            "eventgroup_name": "eg_EOS",
            "event_name": "ev_EOS",
            "form_name": "VS_01_EOS",
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