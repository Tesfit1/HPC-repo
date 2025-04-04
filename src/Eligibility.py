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
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_ELIG_FULL_2024JUL101011.txt')
df = pd.read_csv(csv_file_path , delimiter='|',dtype=str)  

# Rename columns while keeping original data intact
df = df.rename(columns={
    'Subject Number': 'subject', 
    'Randomized':'DSCOMP_ELIG',
    'Reason Non-Randomized':'DSNCOMP_ELIG',
    'Randomization Date':'DSSTDAT_ELIG',
    'Randomization Number':'RANDNO'
})
df["DSCOMP_ELIG"] = df["DSCOMP_ELIG"].map({"Yes": "Y", "No": "N"})
df["DSNCOMP_ELIG"] = df["DSNCOMP_ELIG"].map({"Adverse Event": "ADVERSE EVENT",
                                             "Screen Failure": "SCREEN FAILURE",
                                             "Screened in Error":"SCREENED IN ERROR",
                                             "Lost to Follow-Up":"LOST TO FOLLOW-UP",
                                             "Withdrawal by Subject":"WITHDRAWAL BY SUBJECT",
                                             "Other":"OTHER"
                                             })

def preprocess_dataframe(df):
    # Convert date format
    def convert_date_format(date_str):
        try:
            return datetime.strptime(date_str, "%d %b %Y").strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None  # Handle missing or invalid dates

    df["DSSTDAT_ELIG"] = df["DSSTDAT_ELIG"].apply(convert_date_format)
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

for _, row in df.iterrows():
    subject = row['subject']  # Assuming the column name is 'subject'
  
    json_body = {
        "study_name": study_name ,
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
            "form_name": "ELIG_02_v001",
            "itemgroups": [
            {
                "itemgroup_name": "ig_ELIG_02_A",
                "itemgroup_sequence": 1,
                "items": [
                    {
                        "item_name": "DSCOMP_ELIG",
                        "value": row['DSCOMP_ELIG']
                    },
                    {
                        "item_name": "DSNCOMP_ELIG",
                        "value": row['DSNCOMP_ELIG']
                    },
                     {
                        "item_name": "DSSTDAT_ELIG",
                        "value": row['DSSTDAT_ELIG']
                    },
                    {
                        "item_name":"RANDNO",
                        "value": row['RANDNO']
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

