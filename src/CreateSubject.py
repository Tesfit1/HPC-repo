import json
from dotenv import load_dotenv
import requests
import os
from dateConv import convert_date_format
import pandas as pd
import boto3
from error_log import FormDataError, APIError, FileNotFoundError, InvalidSessionIDError, log_error, check_file_exists
from io import StringIO 

# Load environment variables from .env file
load_dotenv()
#1.Create Subjects
# variables 
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
# SESSION_ID = os.getenv("SESSION_ID")
SESSION_FILE = "session_id.txt"
# SESSION_FILE = '/opt/airflow/scripts/session_id.txt'
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
print(f"Session ID: {SESSION_ID}")  # Debugging line to check SESSION_ID
# study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")

aws_access = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
file_name = os.getenv("Casebook_file_name")
bucket_name = os.getenv("bucket_name")

# Read a comma-delimited .txt file
# Get the current script's directory
# Read a comma-delimited .txt file
s3 = boto3.client('s3', aws_access_key_id= aws_access, aws_secret_access_key=aws_secret)

try:
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
    study_name = df.columns[-1]
except FileNotFoundError as e:
    log_error(e)
    raise


# Rename columns while keeping original data intact
df = df.rename(columns={
    'Subject Number': 'subject',  # Rename 'subject_number' to 'subject'
    'Site': 'site'          # Rename 't' to 'site'
})
# Assuming columns are named as 'study', 'subject_number', and 'study_site'
subjects = df[['site', 'subject']].to_dict(orient='records')


for entry in subjects:
    entry['study_country'] = study_country

# Display the  data
#print(subjects)

# Define the API endpoint
url =  f"{BASE_URL}/api/{API_VERSION}/app/cdm/subjects"
# Define headers, if required (e.g., Content-Type, Authorization)
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {SESSION_ID}",  
}
# Prepare the payload, including both study_name and subjects
payload = {
    "study_name": study_name,
    "subjects": subjects  # The subjects data
}
#print(payload)
# Send the POST request with -d (data)
#response = requests.post(url, json=payload, headers=headers)
response = requests.post(url, headers=headers, data=json.dumps(payload))

# Define the URL

subjects_url =  f"{BASE_URL}/api/{API_VERSION}/app/cdm/subjects?study_name=" + study_name

# Send the GET request
response_retrieve_subjects = requests.get(subjects_url, headers=headers)

# Get the JSON response
json_response_retrieve_subjects = response_retrieve_subjects.json()

# Load JSON
print(json.dumps(json_response_retrieve_subjects, indent=4))  # Prettify JSON

# Extract the 'subjects' key

subjects = [entry["subject"] for entry in json_response_retrieve_subjects["subjects"]]
print(subjects)




