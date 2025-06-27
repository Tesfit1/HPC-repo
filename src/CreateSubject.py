import json
import requests
from utils.error_log_utils import (
    FormDataError, APIError, CustomFileNotFoundError, InvalidSessionIDError,
    log_error, check_file_exists
)
from utils.s3_utils import read_s3_csv
from utils.config_utils import (
    API_VERSION, BASE_URL, STUDY_NAME, STUDY_COUNTRY, SITE,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME,
    FILE_NAMES, SESSION_FILE
)

# Ensure session file exists and load session ID
check_file_exists(SESSION_FILE)
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()

# Read subject data from S3
try:
    df = read_s3_csv(
        bucket=BUCKET_NAME,
        key=FILE_NAMES["casebook"],
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        delimiter='|'
    )
    study_name = df.columns[-1]
except Exception as e:
    print(f"Error reading subject data: {e}")
    log_error(e)
    raise

# Rename columns for consistency
df = df.rename(columns={
    'Subject Number': 'subject',
    'Site': 'site'
})

# Prepare subjects list
subjects = df[['site', 'subject']].to_dict(orient='records')
for entry in subjects:
    entry['study_country'] = STUDY_COUNTRY

# Prepare API endpoint and headers
url = f"{BASE_URL}/api/{API_VERSION}/app/cdm/subjects"
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {SESSION_ID}",
}

# Prepare payload
payload = {
    "study_name": study_name,
    "subjects": subjects
}

# Send POST request to create subjects
try:
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()
    print("Subjects created successfully.")
except Exception as e:
    print(f"Error creating subjects: {e}")
    log_error(e)

# Retrieve and print subjects for this study
subjects_url = f"{BASE_URL}/api/{API_VERSION}/app/cdm/subjects?study_name={study_name}"
try:
    response_retrieve_subjects = requests.get(subjects_url, headers=headers)
    response_retrieve_subjects.raise_for_status()
    json_response_retrieve_subjects = response_retrieve_subjects.json()
    print(json.dumps(json_response_retrieve_subjects, indent=4))
    subjects = [entry["subject"] for entry in json_response_retrieve_subjects.get("subjects", [])]
    print(subjects)
except Exception as e:
    print(f"Error retrieving subjects: {e}")
    log_error(e)