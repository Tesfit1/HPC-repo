import json
from utils.error_log_utils import log_error, check_file_exists
from utils.s3_utils import read_s3_csv
from utils.config_utils import (
    API_VERSION, BASE_URL, STUDY_NAME, STUDY_COUNTRY, SITE,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME,
    FILE_NAMES, SESSION_FILE
)
from utils.data_utils import build_event_payloads
from utils.form_config_utils import EVENT_CONFIGS
from utils.dateConv_utils import convert_date_format
import requests

check_file_exists(SESSION_FILE)
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()

event_config = EVENT_CONFIGS["VisitThreeDate"]

# Read data from S3 using your utility
df = read_s3_csv(
    bucket=BUCKET_NAME,
    key=FILE_NAMES["event_date_tst"],  # Make sure this key is correct in your config
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    delimiter='|'
)

# Validate and rename columns
df = df[event_config["required_columns"]]
df = df.rename(columns=event_config["rename_map"])

# Preprocess (date conversion)
if event_config.get("event_date_conversion"):
    df[event_config["date_column"]] = df[event_config["date_column"]].apply(convert_date_format)
df = df.fillna("")

# Build the events payload
data = build_event_payloads(df, event_config, STUDY_NAME, STUDY_COUNTRY, SITE)

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