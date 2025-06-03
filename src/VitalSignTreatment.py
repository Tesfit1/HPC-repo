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

# Variables
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
# SESSION_ID = os.getenv("SESSION_ID")
# SESSION_FILE = "/session/session_id.txt"
SESSION_FILE = "session_id.txt"
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")

aws_access = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
file_name = os.getenv("VitalSignTreatment")
bucket_name = os.getenv("bucket_name")

# Read a comma-delimited .txt file
s3 = boto3.client('s3', aws_access_key_id= aws_access, aws_secret_access_key=aws_secret)

try:
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
except FileNotFoundError as e:
    log_error(e)
    raise



# Rename columns while keeping original data intact
df = df.rename(columns={
    'Subject Number': 'subject',
    'Planned Time Point': 'VSTPT_1',
    'Date of Measurement':'VSDAT_1',
    'Time of Measurement':'VSTIM_1',
    #'Height':'HEIGHT',
    'Weight' :'WEIGHT_1',
    'Pulse Rate':'PULSE_1',
    'Systolic Blood Pressure':'SYSBP_1',
    'Diastolic Blood Pressure':'DIABP_1',
    'Respiratory Rate':'RESP_1',
    'Temperature':'TEMP_1'
})

def preprocess_dataframe(df):
    df["VSDAT_1"] = df["VSDAT_1"].apply(convert_date_format)
    return df

df = preprocess_dataframe(df)
df = df.fillna("")
# prepare the data to be sent
json_payloads = []
current_subject = None  # Track current subject
current_event = None  # Track current event
previous_folder = None  # Track previous folder

itemgroup_sequence = 1  # Start item sequence counter
eventgroup_sequence = 1

for _, row in df.iterrows():
    subject = row['subject']
    event = row['Folder']

    # Reset itemgroup_sequence if Folder changes
    if event != previous_folder:
        itemgroup_sequence = 1  # Reset item sequence when event changes
        previous_folder = event  # Update previous_folder to the current event

    # Reset itemgroup_sequence if subject changes
    if subject != current_subject:
        current_subject = subject
        itemgroup_sequence = 1
        previous_folder = event  # Reset previous_folder for the new subject

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
            "eventgroup_name": (
                'eg_SCREEN' if row['Folder'] == 'V01' else
                'eg_TREAT_SD'
            ),
            "eventgroup_sequence": 1,
            "event_name": (
                'ev_V01' if row['Folder'] == 'V01' else
                'ev_V02' if row['Folder'] == 'V02' else
                'ev_V03' if row['Folder'] == 'V03' else
                'ev_V04'
            ),
            "form_name": "VS_01_TREAT",
            "itemgroups": [
            {
                "itemgroup_name": "ig_VS_01_A_1",
                "itemgroup_sequence": itemgroup_sequence,
                "items": [
                    {
                        "item_name": "VSTPT_1",
                        "value": row['VSTPT_1']
                    },
                    {
                        "item_name": "VSDAT_1",
                        "value": row['VSDAT_1']
                    },
                    {
                        "item_name": "VSTIM_1",
                        "value": row['VSTIM_1']
                    },
                    #  {
                    #     "item_name": "HEIGHT",
                    #     "value": row['HEIGHT'],
                    #     "unit_value":"Centimeter"
                    # },
                    # {
                    #     "item_name":"WEIGHT_1",
                    #     "value": row['WEIGHT_1'],
                    #     "unit_value":"Kilogram"
                    # },
                    
                    {
                        "item_name" : "SYSBP_1",
                        "value": row['SYSBP_1'],
                        "unit_value":"Millimeter of Mercury"
                    },
                    {
                        "item_name" : "DIABP_1",
                        "value": row['DIABP_1'],
                        "unit_value":"Millimeter of Mercury"
                    },
                    {
                        "item_name" : "PULSE_1",
                        "value": row['PULSE_1'],
                        "unit_value":"Beats per Minute"
                    },
                    # {
                    #     "item_name" : "RESP_1",
                    #     "value": row['RESP_1'],
                    #     "unit_value":"Breaths per Minute"
                    # }
                    # {
                    #     "item_name" : "TEMP_1",
                    #     "value": row['TEMP_1'],
                    #     "unit_value":"Degree-Celsius"
                    # }
                ]
            }
            ]
        }
    }
    json_payloads.append(json_body)
    itemgroup_sequence += 1  # Increment sequence for the next event
    print(json.dumps(json_body, indent=4))


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

