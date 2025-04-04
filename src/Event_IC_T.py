import json
from dotenv import load_dotenv
import requests
import os
from datetime import datetime
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
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_IC_FULL_2024JUL101011.txt')

 # Update with  CSV file path
df = pd.read_csv(csv_file_path, delimiter='|',dtype=str)   
# Map "Yes" to "Y" and "No" to "N" in 'Informed Consent Obtained'
df["Informed Consent Obtained"] = df["Informed Consent Obtained"].map({"Yes": "Y", "No": "N"})
df["Informed Consent Type"] = df["Informed Consent Type"].map({"Main": "MAIN"})
# Rename columns while keeping original data intact
df = df.rename(columns={
    'Subject Number': 'subject',  
    'Informed Consent Type': 'DSSCAT_IC',
    'Informed Consent Version ID':'DSREFID_IC',
    'Informed Consent Obtained':'IC',
    'Informed Consent Date':'DSSTDAT_IC'
})

def preprocess_dataframe(df): 
    # Convert 'Informed Consent Date' to (YYYY-MM-DD) format
    def convert_date_format(date_str):
        try:
            return datetime.strptime(date_str, "%d %b %Y").strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None  # Handle missing or invalid dates

    df["DSSTDAT_IC"] = df["DSSTDAT_IC"].apply(convert_date_format)
    return df
df = preprocess_dataframe(df)
# set the event date
#date = df['DSSTDAT_IC'].tolist()
# prepare the data to be sent
json_payloads = []

for _, row in df.iterrows():
    subject = row['subject']  # Assuming the column name is 'subject'
    DSSCAT_IC = row['DSSCAT_IC']
    DSREFID_IC = row['DSREFID_IC']
    IC = row['IC']
    DSSTDAT_IC = row['DSSTDAT_IC']
    DSSTDAT_IC = row['DSSTDAT_IC']
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
            "form_name": "IC_01_v001",
            "itemgroups": [
                {
                    "itemgroup_name": "ig_IC_01_A",
                    "itemgroup_sequence": 1,
                    "items": [
                        {
                            "item_name": "DSSCAT_IC",
                            "value": DSSCAT_IC
                        },
                        {
                            "item_name": "DSREFID_IC",
                            "value": DSREFID_IC
                        },
                        {
                            "item_name" : "IC",
                            "value": IC
                        },
                        {
                            "item_name" : "DSSTDAT_IC",
                            "value": DSSTDAT_IC
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

