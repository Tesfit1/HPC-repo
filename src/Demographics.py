import json
from dotenv import load_dotenv
import requests
import os
from dateConv import convert_date_format
import pandas as pd
import boto3
from error_log import FormDataError, APIError, FileNotFoundError, InvalidSessionIDError, log_error
from io import StringIO
from api_utils import import_form

# Load environment variables
load_dotenv()

API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
# SESSION_FILE = "session_id.txt"
SESSION_FILE ='/opt/airflow/scripts/session_id.txt'
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")
aws_access = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
file_name = os.getenv("Event_DM")
bucket_name = os.getenv("bucket_name")

# Read demographics data from S3
s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secret)
try:
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
except FileNotFoundError as e:
    log_error(e)
    raise

# Rename columns and map values
df = df.rename(columns={
    'Subject Number': 'subject',  
    'Birth Year': 'BRTHDAT',
    'Age': 'AGE',
    'Sex': 'SEX',
    'Child Bearing Potential': 'CHILDPOT_1',
    'Gender': 'GENIDENT',
    'Ethnicity': 'ETHNIC',
    'American Indian or Alaska Native': 'RACE_1',
    'Asian': 'RACE_2',
    'Black or African American': 'RACE_3',
    'Native Hawaiian or Other Pacific Islander': 'RACE_4',
    'White': 'RACE_5'
})
df["ETHNIC"] = df["ETHNIC"].map({"Not Hispanic or Latino": "NOT HISPANIC OR LATINO", "Hispanic or Latino": "HISPANIC OR LATINO"})
df["SEX"] = df["SEX"].map({"Male": "M", "Female": "F"})
df["RACE_1"] = df["RACE_1"].map({"American Indian or Alaska Native": True})
df["RACE_2"] = df["RACE_2"].map({"Asian": True})
df["RACE_3"] = df["RACE_3"].map({"Black or African American": True})
df["RACE_4"] = df["RACE_4"].map({"Native Hawaiian or Other Pacific Islander": True})
df["RACE_5"] = df["RACE_5"].map({"White": True})
df = df.fillna("")

def preprocess_dataframe(df):
    # Add any date conversion or additional preprocessing here if needed
    return df

df = preprocess_dataframe(df)

# Prepare the data to be sent
json_payloads = []

for _, row in df.iterrows():
    subject = row['subject']
    itemgroup = {
        "itemgroup_name": "ig_DM_02_A",
        "itemgroup_sequence": 1,
        "items": [
            {"item_name": "BRTHDAT", "value": row['BRTHDAT']},
            {"item_name": "AGE", "value": row['AGE'], "unit_value": "Year"},
            {"item_name": "SEX", "value": row['SEX']},
            # Uncomment if you want to include these fields:
            # {"item_name": "CHILDPOT_1", "value": row['CHILDPOT_1']},
            # {"item_name": "GENIDENT", "value": row['GENIDENT']},
            {"item_name": "ETHNIC", "value": row['ETHNIC']},
            {"item_name": "RACE_1", "value": row['RACE_1']},
            {"item_name": "RACE_2", "value": row['RACE_2']},
            {"item_name": "RACE_3", "value": row['RACE_3']},
            {"item_name": "RACE_4", "value": row['RACE_4']},
            {"item_name": "RACE_5", "value": row['RACE_5']}
        ]
    }

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
            "eventgroup_name": "eg_SCREEN",
            "event_name": "ev_V01",
            "form_name": "DM_02_v001",
            "itemgroups": [itemgroup]
        }
    }
    json_payloads.append(json_body)

# API headers and endpoint
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {SESSION_ID}",
}
api_endpoint = f"{BASE_URL}/api/{API_VERSION}/app/cdm/forms/actions/setdata"

def validate_form_data(payload):
    form = payload.get("form", {})
    required_fields = ["study_country", "site", "subject", "eventgroup_name", "event_name", "form_name"]
    for field in required_fields:
        if not form.get(field):
            print(f"Validation error: {field} is missing in form for subject {form.get('subject')}")
            return False
    return True

# Import forms with error handling and user feedback
for payload in json_payloads:
    try:
        import_form(payload, api_endpoint, headers, validate_form_data)
    except (FormDataError, APIError, InvalidSessionIDError) as e:
        print(f"Error: {e}")
        log_error(e)
    except Exception as e:
        print(f"Unexpected error: {e}")
        log_error(e)