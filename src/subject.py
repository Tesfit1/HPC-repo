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
study_country = "Germany"  # Example country, change it as needed
study_name = 'HPC_Test_Study_DEV1'
# Read a comma-delimited .txt file
current_dir = os.path.dirname(os.path.abspath(__file__))

# Move one directory level up
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

# Path to the CSV file
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_SUBJ_FULL_2023FEB130833.txt')
df = pd.read_csv(csv_file_path, delimiter='|',dtype=str) 
# Rename columns while keeping original data intact
df = df.rename(columns={
    'Subject Number': 'subject',  # Rename 'subject_number' to 'subject'
    'Site': 'site'          # Rename 'study_site' to 'site'
})



# Display the data to be sent
json_payloads = []

for _, row in df.iterrows():
    site = row['site']  # Assuming the column name is 'site'
    subject = row['subject']  # Assuming the column name is 'subject'
    json_body = {
        "study_name": "HPC_Test_Study_DEV1",
        "reopen": True,
        "submit": True,
        "change_reason": "Updated by the integration",
        "externally_owned": True,
        "form": {
            "study_country": study_country,
            "site": site,
            "subject": subject,
            "eventgroup_name": "eg_SCREEN",
            "event_name": "SUBJECT",
            "form_name": "SUBJ_01_003",
            "itemgroups": [
                {
                    "itemgroup_name": "ig_subject",
                    "itemgroup_sequence": 1,
                    "items": [
                        {
                            "item_name": "SUBJIDN",
                            "value": subject
                        },
                        {
                            "item_name": "STUDYSITE",
                            "value": site
                        },
                        {
                            "item_name" : "COUNTRY",
                            "value":"DE"
                        }
                    ]
                }
            ]                
        }
    }
    #print(json.dumps(json_body, indent=4))
    json_payloads.append(json_body)
# Define the API endpoint
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {SESSION_ID}",  
}

api_endpoint =  f"{BASE_URL}/api/{API_VERSION}/app/cdm/forms/actions/setdata"
for payload in json_payloads:
    response = requests.post(api_endpoint, headers=headers, data=json.dumps(payload))
    response_json = response.json()  # This directly parses the response if it's JSON
    print(json.dumps(response_json, indent=4))