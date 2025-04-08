import json
from dotenv import load_dotenv
import requests
import os
from dateConv import convert_date_format
import pandas as pd
from error_log import FormDataError, APIError, FileNotFoundError, InvalidSessionIDError, log_error, check_file_exists

# Load environment variables from .env file
load_dotenv()

# Variables
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
SESSION_ID = os.getenv("SESSION_ID")
study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")

# Read a comma-delimited .txt file
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_IC_FULL_2024JUL101011.txt')

try:
    check_file_exists(csv_file_path)
    df = pd.read_csv(csv_file_path, delimiter='|', dtype=str)
except FileNotFoundError as e:
    log_error(e)
    raise

df["Informed Consent Obtained"] = df["Informed Consent Obtained"].map({"Yes": "Y", "No": "N"})
df["Informed Consent Type"] = df["Informed Consent Type"].map({"Main": "MAIN"})
df = df.rename(columns={
    'Subject Number': 'subject',
    'Informed Consent Type': 'DSSCAT_IC',
    'Informed Consent Version ID': 'DSREFID_IC',
    'Informed Consent Obtained': 'IC',
    'Informed Consent Date': 'DSSTDAT_IC'
})

def preprocess_dataframe(df):
    df["DSSTDAT_IC"] = df["DSSTDAT_IC"].apply(convert_date_format)
    return df

df = preprocess_dataframe(df)

json_payloads = []

for _, row in df.iterrows():
    subject = row['subject']
    DSSCAT_IC = row['DSSCAT_IC']
    DSREFID_IC = row['DSREFID_IC']
    IC = row['IC']
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
                            "item_name": "IC",
                            "value": IC
                        },
                        {
                            "item_name": "DSSTDAT_IC",
                            "value": DSSTDAT_IC
                        }
                    ]
                }
            ]
        }
    }
    print(json.dumps(json_body, indent=4))
    json_payloads.append(json_body)

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {SESSION_ID}",
}

api_endpoint = f"{BASE_URL}/api/{API_VERSION}/app/cdm/forms/actions/setdata"

def import_form(payload):
    try:
        # Simulate form data validation
        if not validate_form_data(payload):
            raise FormDataError(f"Invalid data in form {payload['form']['subject']}")

        # Simulate API call
        response = requests.post(api_endpoint, headers=headers, data=json.dumps(payload))
        response_json = response.json()
        if response.status_code == 200:
            if any(error['type'] == 'INVALID_SESSION_ID'for error in response_json.get('errors', [])):
                raise InvalidSessionIDError("Invalid or expired session ID.")

        if response.status_code != 200:
            raise APIError(f"API call failed for form {payload['form']['subject']} with status code {response.status_code}")

        print(json.dumps(response_json, indent=4))

    except FormDataError as e:
        log_error(e)
    except APIError as e:
        log_error(e)
    except InvalidSessionIDError as e:
        log_error(e)
        # Handle session ID renewal or prompt user to re-authenticate
        print("Session ID is invalid or expired. Please renew the session ID.")
    except Exception as e:
        log_error(f"Unexpected error importing form {payload['form']['subject']}: {e}")

def validate_form_data(payload):
    # Add your validation logic here
    return True  # Return False if validation fails

for payload in json_payloads:
    try:
        import_form(payload)
    except Exception as e:
        log_error(e)
