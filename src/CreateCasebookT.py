import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd

# Load environment variables from .env file
load_dotenv()
#1.Create Subjects
# variables 
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
SESSION_ID = os.getenv("SESSION_ID")
study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")
# Read a comma-delimited .txt file
# Get the current script's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Move one directory level up
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

# Path to the CSV file
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_SUBJ_FULL_2023FEB130833.txt')

df = pd.read_csv(csv_file_path, delimiter='|', dtype=str)


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
#print(json.dumps(json_response_retrieve_subjects, indent=4))  # Prettify JSON

# Extract the 'subjects' key

subjects = [entry["subject"] for entry in json_response_retrieve_subjects["subjects"]]
print(subjects)




