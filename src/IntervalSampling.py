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
file_name = os.getenv("IntervalSampling")
bucket_name = os.getenv("bucket_name")

# Read blood sampling data from S3
s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secret)
try:
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
except FileNotFoundError as e:
    log_error(e)
    raise

# Ensure required columns exist before renaming
required_columns = ['Subject Number', 'Folder', 'Planned Start Time Point', 'Planned Stop Time Point', 'Sample Start Time', 'Comment']
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    raise KeyError(f"Missing columns in input data: {missing_columns}")

# Rename columns
df = df.rename(columns={
    'Subject Number': 'subject',
    'Folder': 'Folder',
    'Planned Start Time Point': 'TPT_INT',
    'Planned Stop Time Point': 'TPTSP_INT',
    # 'Sample Taken': 'PERF_INT',
    'Sample Start Date': 'DAT_INT',
    'Sample Start Time': 'TIM_INT',
    'Sample Stop Date': 'ENDAT_INT',
    'Sample Stop Time': 'ENTIM_INT',
    'Weight Empty': 'CEWT_INT',
    'Weight Filled': 'CFWT_INT',
    'Comment': 'COM_INT'
})



def preprocess_dataframe(df):
    df["DAT_INT"] = df["DAT_INT"].apply(convert_date_format)
    df["ENDAT_INT"] = df["ENDAT_INT"].apply(convert_date_format)
    return df

df = preprocess_dataframe(df)
df = df.fillna("")

# Prepare JSON payloads
json_payloads = []
subject_event_counter = {}

for _, row in df.iterrows():
    subject = row['subject']
    event = row['Folder']

    # Track itemgroup_sequence per subject/event
    key = (subject, event)
    if key not in subject_event_counter:
        subject_event_counter[key] = 1
    else:
        subject_event_counter[key] += 1
    itemgroup_sequence = subject_event_counter[key]

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
            "eventgroup_name": (
                'eg_SCREEN' if event == 'V01' else 'eg_TREAT_SD'
            ),
            "eventgroup_sequence": 1,
            "event_name": (
                'ev_V01' if event == 'V01' else
                'ev_V02' if event == 'V02' else
                'ev_V03' if event == 'V03' else
                'ev_V04'
            ),
            "form_name": "SAMP_INT_01_v002",
            "itemgroups": [
                {
                    "itemgroup_name": "ig_SAMP_INT_01_A",
                    "itemgroup_sequence": itemgroup_sequence,
                    "items": [
                       {"item_name": "TPT_INT", "value": row['TPT_INT']},
                {"item_name": "TPTSP_INT", "value": row['TPTSP_INT']},
                # {"item_name": "PERF_INT", "value": row['PERF_INT']},  # Uncomment when needed
                {"item_name": "DAT_INT", "value": row['DAT_INT']},
                {"item_name": "TIM_INT", "value": row['TIM_INT']},
                {"item_name": "ENDAT_INT", "value": row['ENDAT_INT']},
                {"item_name": "ENTIM_INT", "value": row['ENTIM_INT']},
                {"item_name": "CEWT_INT", "value": row['CEWT_INT']},
                {"item_name": "CFWT_INT", "value": row['CFWT_INT']},
                {"item_name": "COM_INT", "value": row['COM_INT']}
                    ]
                }
            ]
        }
    }
    json_payloads.append(json_body)
    # print(f"Prepared JSON payload for subject {subject}, event {event}: {json_body}")
    print(json.dumps(json_body, indent=4))

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