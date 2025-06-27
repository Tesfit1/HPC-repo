import json
from utils.error_log_utils import log_error, check_file_exists
from utils.s3_utils import read_s3_csv
from utils.config_utils import (
    API_VERSION, BASE_URL, STUDY_NAME, STUDY_COUNTRY, SITE,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME,
    FILE_NAMES, SESSION_FILE
)
from utils.dateConv_utils import convert_date_format
import requests

check_file_exists(SESSION_FILE)
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()

eventgroup_name = 'eg_SCREEN'
eventgroup_sequence = 1
event_name = 'ev_V01'

# Read data from S3 using your utility
df = read_s3_csv(
    bucket=BUCKET_NAME,
    key=FILE_NAMES["event_date_tst"],  # Add this key to your config
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    delimiter='|'
)

# Rename columns
df = df.rename(columns={
    'Subject Number': 'subject',
    'Informed Consent Date': 'DSSTDAT_IC'
})

# Preprocess (date conversion)
df["DSSTDAT_IC"] = df["DSSTDAT_IC"].apply(convert_date_format)
df = df.fillna("")

# Build the events payload
events = [
    {
        "study_country": STUDY_COUNTRY,
        "site": SITE,
        "subject": row['subject'],
        "eventgroup_name": eventgroup_name,
        "eventgroup_sequence": eventgroup_sequence,
        "event_name": event_name,
        "date": row['DSSTDAT_IC'],
        "change_reason": "Action performed via the API",
        "method": "on_site_visit__v",
        "allow_planneddate_override": False,
        "externally_owned_date": True,
        "externally_owned_method": False
    }
    for _, row in df.iterrows()
]

data = {
    "study_name": STUDY_NAME,
    "events": events
}

print(json.dumps(data, indent=4))

# Send the POST request
url = f"{BASE_URL}/api/{API_VERSION}/app/cdm/events/actions/update"
try:
    response = requests.post(url, headers={
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {SESSION_ID}",
    }, data=json.dumps(data))
    response_json = response.json()
    print(json.dumps(response_json, indent=4))
except Exception as e:
    print(f"Error: {e}")
    log_error(e)