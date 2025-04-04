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
study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")
# Read a comma-delimited .txt file
current_dir = os.path.dirname(os.path.abspath(__file__))

# Move one directory level up
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

# Path to the CSV file
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_VSEOS_FULL_2024JUL101145.txt')
df = pd.read_csv(csv_file_path , delimiter='|',dtype=str)

# Rename columns while keeping original data intact
df = df.rename(columns={
    'Subject Number': 'subject',
    # 'Planned Time Point': 'VSTPT_2',
    'Date of Measurements':'VSDAT_2',
    # 'Time of Measurement':'VSTIM_1',
    #'Height':'HEIGHT',
    'Weight' :'WEIGHT_2',
    'Pulse Rate':'PULSE_2',
    'Systolic Blood Pressure':'SYSBP_2',
    'Diastolic Blood Pressure':'DIABP_2',
    'Respiratory Rate':'RESP_2',
    'Temperature':'TEMP_2'
})

def preprocess_dataframe(df):
    # Convert date format
    def convert_date_format(date_str):
        try:
            return datetime.strptime(date_str, "%d %b %Y").strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None  # Handle missing or invalid dates

    df["VSDAT_2"] = df["VSDAT_2"].apply(convert_date_format)
    return df
df = preprocess_dataframe(df)
# set the event date
#event_date = df['DSSTDAT_IC']
#event_date = str(df['DSSTDAT_IC'].iloc[0])


df = df.fillna("")
#df['CHILDPOT'] = df['CHILDPOT'].replace("", None).fillna("No")
#df["AGE"] = pd.to_numeric(df["AGE"], errors="coerce")

# prepare the data to be sent
json_payloads = []
current_subject = None  # Track current subject
itemgroup_sequence = 1  # Start sequence counter

for _, row in df.iterrows():
    subject = row['subject']  
    if subject != current_subject:
        current_subject = subject
        itemgroup_sequence = 1
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
            "eventgroup_name": "eg_EOS",
            "event_name": "ev_EOS",
            "form_name": "VS_01_v001_2",
            "itemgroups": [
            {
                "itemgroup_name": "ig_VS_01_A_2",
                "itemgroup_sequence": itemgroup_sequence,
                "items": [
                    {
                        "item_name": "VSDAT_2",
                        "value": row['VSDAT_2']
                    },
                    # {
                    #     "item_name": "VSTIM_1",
                    #     "value": row['VSTIM_1']
                    # },
                    #  {
                    #     "item_name": "HEIGHT",
                    #     "value": row['HEIGHT'],
                    #     "unit_value":"Centimeter"
                    # },
                    {
                        "item_name":"WEIGHT_2",
                        "value": row['WEIGHT_2'],
                        "unit_value":"Kilogram"
                    },
                    {
                        "item_name" : "PULSE_2",
                        "value": row['PULSE_2'],
                        "unit_value":"Beats per Minute"
                    },
                    {
                        "item_name" : "SYSBP_2",
                        "value": row['SYSBP_2'],
                        "unit_value":"Millimeter of Mercury"
                    },
                    {
                        "item_name" : "DIABP_2",
                        "value": row['DIABP_2'],
                        "unit_value":"Millimeter of Mercury"
                    },
                    {
                        "item_name" : "RESP_2",
                        "value": row['RESP_2'],
                        "unit_value":"Breaths per Minute"
                    },
                    {
                        "item_name" : "TEMP_2",
                        "value": row['TEMP_2'],
                        "unit_value":"Degree-Celsius"
                    }
                ]
            }
        ]
        }
    }
    itemgroup_sequence += 1  # Increment sequence for the next event
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

