import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd
import boto3
from dateConv import convert_date_format
from error_log import APIError, InvalidSessionIDError, log_error
from io import StringIO

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
file_name = os.getenv("EventDate")
bucket_name = os.getenv("bucket_name")

# Read visit data from S3
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
    'Folder': 'folder',
    'Visit Date': 'visit_date'
    # Add 'Visit Type': 'visit_type' if needed
})

def preprocess_dataframe(df):
    df["visit_date"] = df["visit_date"].apply(convert_date_format)
    return df

df = preprocess_dataframe(df)
df = df.fillna("")

def get_event_name(folder):
    return f"ev_{folder}"  # Adjust if your event names differ

def validate_event_data(event):
    required_fields = ["study_country", "site", "subject", "eventgroup_name", "event_name", "date"]
    for field in required_fields:
        if not event.get(field):
            print(f"Validation error: {field} is missing in event for subject {event.get('subject')}")
            return False
    return True

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {SESSION_ID}",
}

url = f"{BASE_URL}/api/{API_VERSION}/app/cdm/events/actions/update"

# Group by subject and send all visits for each subject in one API call
for subject, group in df.groupby('subject'):
    events = []
    for _, row in group.iterrows():
        event = {
            "study_country": study_country,
            "site": site,
            "subject": row['subject'],
            "eventgroup_name": 'eg_SCREEN' if row['folder'] == 'V01' else 'eg_TREAT_SD',
            "eventgroup_sequence": 1,
            "event_name": get_event_name(row['folder']),
            "date": row['visit_date'],
            "change_reason": "Action performed via the API",
            "method": "on_site_visit__v",  # Or row['visit_type'] if available
            "allow_planneddate_override": False,
            "externally_owned_date": True,
            "externally_owned_method": False
        }
        if validate_event_data(event):
            events.append(event)
        else:
            log_error(f"Validation failed for subject {row['subject']} visit {row['folder']}")

    if not events:
        print(f"No valid events to import for subject {subject}")
        continue

    data = {
        "study_name": study_name,
        "events": events
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response_json = response.json()

        # Event-level error reporting
        if "events" in response_json:
            for event_result in response_json["events"]:
                if event_result.get("responseStatus") == "FAILURE":
                    error_msg = (
                        f"Subject: {event_result.get('subject')}, "
                        f"Event: {event_result.get('event_name')}, "
                        f"Error: {event_result.get('errorMessage')}"
                    )
                    print(error_msg)
                    log_error(error_msg)

        if response.status_code != 200 or response_json.get("responseStatus") == "FAILURE":
            print(f"API error for subject {subject}: {response_json.get('errorMessage', response.text)}")
            log_error(f"API error for subject {subject}: {response_json}")
        else:
            print(json.dumps(response_json, indent=4))
    except (APIError, InvalidSessionIDError) as e:
        print(f"Error: {e}")
        log_error(e)
    except Exception as e:
        print(f"Unexpected error: {e}")
        log_error(e)