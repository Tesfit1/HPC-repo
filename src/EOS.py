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
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_EOS_FULL_2024JUL101050.txt')
df = pd.read_csv(csv_file_path , delimiter='|',dtype=str)

# Rename columns while keeping original data intact
df = df.rename(columns={
    'Subject Number': 'subject',
    'EOS Date' : 'DSSTDAT_EOS',
    'Completed':'DSCOMP_EOS',
    'Reason Non-Completion':'DSNCOMP_EOS',
    'Death date':'DTHDAT_EOS',
    'Lost to FUP date':'LTFDAT'
})

# df['DSNCOMP_EOS'] = df['DSNCOMP_EOS'].fillna('')
split_columns = df['DSNCOMP_EOS'].str.split(',', expand=True)
df['DSNCOMP_EOS'] = split_columns[0].str.strip()
df['reason'] = split_columns[1].str.strip()
#
# df = df.fillna("")
# df['reason'] = split_columns[1].str.strip()
#
# df = df.fillna("")
df["DSNCOMP_EOS"] = df["DSNCOMP_EOS"].map({"Lost to Follow-Up": "LOST TO FOLLOW-UP",
                                           "Death": "DEATH",
                                           "Withdrawal by Subject":"WITHDRAWAL BY SUBJECT",
                                           "other":"OTHER"})
df["DSCOMP_EOS"] = df["DSCOMP_EOS"].map({"Yes": "Y", "No": "N"})
def preprocess_dataframe(df):
    # Convert 'Informed Consent Date' to (YYYY-MM-DD) format
    def convert_date_format(date_str):
        try:
            return datetime.strptime(date_str, "%d %b %Y").strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None  # Handle missing or invalid dates

    df["DSSTDAT_EOS"] = df["DSSTDAT_EOS"].apply(convert_date_format)
    df["LTFDAT"] = df["LTFDAT"].apply(convert_date_format)
    df["DTHDAT_EOS"] = df["DTHDAT_EOS"].apply(convert_date_format)
    return df
df = preprocess_dataframe(df)
df = df.fillna("")
json_payloads = []

for _, row in df.iterrows():
    subject = row['subject']
    DSSTDAT_EOS = row['DSSTDAT_EOS']
    DSCOMP_EOS = row['DSCOMP_EOS']
    DSNCOMP_EOS = row['DSNCOMP_EOS']
    DTHDAT_EOS = row['DTHDAT_EOS']
    LTFDAT = row['LTFDAT']

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
        "form_name": "EOS_01_v001",
        "itemgroups": [
            {
                "itemgroup_name": "ig_EOS_01_A",
                "itemgroup_sequence": 1,
                "items": [
                    {
                        "item_name": "DSSTDAT_EOS",
                        "value": row['DSSTDAT_EOS']
                    },
                     {
                        "item_name": "DSCOMP_EOS",
                        "value": row['DSCOMP_EOS']
                    },
                    {
                        "item_name":"DSNCOMP_EOS",
                        "value":row['DSNCOMP_EOS']
                    }
                    ,
                    {
                        "item_name":"DSNCOMP_SPECIFY",
                        "value":row['reason']
                    },
                    {
                        "item_name": "DTHDAT_EOS",
                        "value": row['DTHDAT_EOS']
                    },
                    {
                        "item_name": "LTFDAT",
                        "value": row['LTFDAT']
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




