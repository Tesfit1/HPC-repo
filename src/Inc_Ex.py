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
current_dir = os.path.dirname(os.path.abspath(__file__))

# Move one directory level up
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

# Path to the CSV file
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_IE_FULL_2024JUL101011.txt')
df = pd.read_csv(csv_file_path , delimiter='|',dtype=str)  

# Rename columns while keeping original data intact
df = df.rename(columns={
    'Subject Number': 'subject',  
    'CTP Version':'CTPNUMG',
    'Criteria Meet':'IEYN',
    'Criterion Category':'IECAT',
    'Criterion Number':'IENUM',

})
df["IEYN"] = df["IEYN"].map({"Yes": "Y", "No": "N"})
df = df.fillna("")
#df['CHILDPOT'] = df['CHILDPOT'].replace("", None).fillna("No")
#df["AGE"] = pd.to_numeric(df["AGE"], errors="coerce")

# prepare the data to be sent
json_payloads = []

for _, row in df.iterrows():
    subject = row['subject']  # Assuming the column name is 'subject'
    CTPNUMG = row['CTPNUMG']
    IEYN= row['IEYN']
    IECAT = row['IECAT']
    IENUM = row['IENUM']
    
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
            "eventgroup_name": "eg_COMMON",
            "event_name": "ev_COMMON",
            "form_name": "IE_01_v001",
            "itemgroups": [
            {
                "itemgroup_name": "ig_IE_01_A",
                "itemgroup_sequence": 1,
                "items": [
                    {
                        "item_name": "CTPNUMG",
                        "value": CTPNUMG
                    },
                    {
                        "item_name": "IEYN",
                        "value": IEYN
                    }
                ]
            },
            {
                "itemgroup_name": "ig_IE_01_B",
                "itemgroup_sequence": 1,
                "items": [
                    {
                        "item_name": "IECAT",
                        "value": IECAT
                    },
                    {
                            "item_name" : "IENUM",
                            "value": IENUM
                    }
                ]  
            }
        ]                  
        }
    }
    print(json.dumps(json_body, indent=4))
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

