import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd
from datetime import datetime

# Load environment variables from .env file
load_dotenv()
#1.Create Subjects
# variables 
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
SESSION_ID = os.getenv("SESSION_ID")
study_country = "Germany"  # Example country, change it as needed
study_name = 'HPC_Test_Study_DEV1'
site = 'DEU1'
# Read a comma-delimited .txt file
current_dir = os.path.dirname(os.path.abspath(__file__))

# Move one directory level up
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

# Path to the CSV file
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_EOT_FULL_2024JUL101045.txt')
df = pd.read_csv(csv_file_path , delimiter='|',dtype=str)  

# Rename columns while keeping original data intact
df = df.rename(columns={
    'Subject Number': 'subject',
    'Unblinded':'DSUNBLND',
    'Subject Completed':'DSCOMP_EOT',
    'Reason Non-Completion':'DSNCOMP_EOT'

})


split_columns = df['DSNCOMP_EOT'].str.split(',', expand=True)
df['reason'] = split_columns[1].str.strip()

df = df.fillna("")
#df['CHILDPOT'] = df['CHILDPOT'].replace("", None).fillna("No")
#df["AGE"] = pd.to_numeric(df["AGE"], errors="coerce")

# prepare the data to be sent
# Prepare JSON payloads
json_payloads = []
current_subject = None  # Track current subject
itemgroup_sequence = 1  

for _, row in df.iterrows():
    subject = row['subject']

    # Reset itemgroup_sequence if subject changes
    if subject != current_subject:
        current_subject = subject
        itemgroup_sequence = 1

    json_body = {
        "study_name": "HPC_Test_Study_DEV1",
        "reopen": True,
        "submit": True,
        "change_reason": "Updated by the integration",
        "externally_owned": True,
        "form": {
        "study_country": "Germany",
        "site": "DEU1",
        "subject": subject,
        "eventgroup_name": "eg_EOS",
        "event_name": "ev_ZEOS",
        "form_name": "EOT_03_v001",
        "itemgroups": [
            {
                "itemgroup_name": "ig_EOT_03_A",
                "itemgroup_sequence": itemgroup_sequence,
                "items": [
                    {
                        "item_name": "DSUNBLND",
                        "value": row['DSUNBLND']
                    },
                     {
                        "item_name": "DSCOMP_EOT",
                        "value": row['DSCOMP_EOT']
                    },
                    {
                        "item_name":"DSNCOMP_EOT",
                        "value":row['DSNCOMP_EOT']
                    }
                    ,
                    {
                        "item_name":"DSNCOMP_EOT_SPECIFY",
                        "value":row['reason']
                    }
                ]
            }
        ]                
    }
    }
    print(json.dumps(json_body, indent=4))
    json_payloads.append(json_body)
    itemgroup_sequence += 1


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




