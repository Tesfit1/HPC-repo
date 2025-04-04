import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd
from datetime import datetime

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
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_SAMP_TPT_PK_FULL_2022FEB171150.txt')
df = pd.read_csv(csv_file_path, delimiter='|', dtype=str)

# Rename columns while keeping original data intact
df = df.rename(columns={
    'Subject Number': 'subject',
    'Time Point': 'TPT_TPT',
    'Sample Date': 'DAT_TPT',
    'Sample Time': 'TIM_TPT',
    'Comment': 'COM_TPT'
})

# Adding a new column for drug admin condition
df['PERF_TPT'] = "N"

# Updating the column to True when drug admin date is not empty
df.loc[df['DAT_TPT'].notnull(), 'PERF_TPT'] = "Y"

def preprocess_dataframe(df):
    def convert_date_format(date_str):
        try:
            return datetime.strptime(date_str, "%d %b %Y").strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    df["DAT_TPT"] = df["DAT_TPT"].apply(convert_date_format)
    return df

df = preprocess_dataframe(df)
df = df.fillna("")

# Prepare JSON payloads
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
                'eg_TREAT_CO'
            ),
            "eventgroup_sequence": 1,
            "event_name": (
                'ev_V01' if row['Folder'] == 'V01' else
                'ev_v02' if row['Folder'] == 'V02' else
                'ev_V03' if row['Folder'] == 'V03' else
                'ev_V04'
            ),
            "form_name": "SAMP_TPT_01_v001",
            "itemgroups": [
                {
                    "itemgroup_name": "ig_SAMP_TPT_01_A",
                    "itemgroup_sequence": itemgroup_sequence,
                    "items": [
                        {
                            "item_name": "TPT_TPT",
                            "value": row['TPT_TPT']
                        },
                        {
                            "item_name": "PERF_TPT",
                            "value": row['PERF_TPT']
                        },
                        {
                            "item_name": "DAT_TPT",
                            "value": row['DAT_TPT']
                        },
                        {
                            "item_name": "TIM_TPT",
                            "value": row['TIM_TPT']
                        },
                        {
                            "item_name": "COM_TPT",
                            "value": row['COM_TPT']
                        }
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

api_endpoint = f"{BASE_URL}/api/{API_VERSION}/app/cdm/forms/actions/setdata"

# Send JSON payloads to the API
for payload in json_payloads:
    response = requests.post(api_endpoint, headers=headers, data=json.dumps(payload))
    response_json = response.json()  # Parse response if it's JSON
    print(json.dumps(response_json, indent=4))
# import json
# from dotenv import load_dotenv
# import requests
# import os
# import pandas as pd
# from datetime import datetime
#
# # Load environment variables from .env file
# load_dotenv()
#
# # Variables
#
# API_VERSION = os.getenv("API_VERSION")
# BASE_URL = os.getenv("BASE_URL")
# SESSION_ID = os.getenv("SESSION_ID")
# study_name = os.getenv("Study_name")
# study_country = os.getenv("Study_country")
# site = os.getenv("site")
#
# # Read a comma-delimited .txt file
# current_dir = os.path.dirname(os.path.abspath(__file__))
# parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
# csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_SAMP_TPT_PK_FULL_2022FEB171150.txt')
# df = pd.read_csv(csv_file_path, delimiter='|', dtype=str)
#
# # Rename columns while keeping original data intact
# df = df.rename(columns={
#     'Subject Number': 'subject',
#     'Time Point': 'TPT_TPT',
#     'Sample Date': 'DAT_TPT',
#     'Sample Time': 'TIM_TPT',
#     'Comment': 'COM_TPT'
# })
# # Adding a new column for drug admin condition ?
# df['PERF_TPT'] = "N"
#
# # Updating the  column to True when drug admin date is not empty
# df.loc[df['DAT_TPT'].notnull(), 'PERF_TPT'] = "Y"
# def preprocess_dataframe(df):
#     def convert_date_format(date_str):
#         try:
#             return datetime.strptime(date_str, "%d %b %Y").strftime("%Y-%m-%d")
#         except (ValueError, TypeError):
#             return None
#
#     df["DAT_TPT"] = df["DAT_TPT"].apply(convert_date_format)
#     return df
#
# df = preprocess_dataframe(df)
# df = df.fillna("")
#
# # Prepare JSON payloads
# json_payloads = []
# current_subject = None  # Track current subject
# itemgroup_sequence = 1  # Start sequence counter
#
# for _, row in df.iterrows():
#     subject = row['subject']
#
#     # Reset itemgroup_sequence if subject changes
#     if subject != current_subject:
#         current_subject = subject
#         itemgroup_sequence = 1
#
#     json_body = {
#         "study_name": study_name,
#         "reopen": True,
#         "submit": True,
#         "change_reason": "Updated by the integration",
#         "externally_owned": True,
#         "form": {
#             "study_country": study_country,
#             "site": site,
#             "subject": subject,
#             "eventgroup_name": "eg_TREAT_CO",
#             "event_name": "ev_v02",
#             "form_name": "SAMP_TPT_01_v001",
#             "itemgroups": [
#                 {
#                     "itemgroup_name": "ig_SAMP_TPT_01_A",
#                     "itemgroup_sequence": itemgroup_sequence,
#                     "items": [
#                         {
#                             "item_name": "TPT_TPT",
#                             "value": row['TPT_TPT']
#                         },
#                         {
#                             "item_name": "PERF_TPT",
#                             "value": row['PERF_TPT']
#                         },
#                         {
#                             "item_name": "DAT_TPT",
#                             "value": row['DAT_TPT']
#                         },
#                         {
#                             "item_name": "TIM_TPT",
#                             "value": row['TIM_TPT']
#                         },
#                         {
#                             "item_name": "COM_TPT",
#                             "value": row['COM_TPT']
#                         }
#                     ]
#                 }
#             ]
#         }
#     }
#     json_payloads.append(json_body)
#     itemgroup_sequence += 1  # Increment sequence for the next event
#
#     # Optional: Print JSON for debugging
#     print(json.dumps(json_body, indent=4))
#
# # Define the API endpoint
# headers = {
#     "Content-Type": "application/json",
#     "Accept": "application/json",
#     "Authorization": f"Bearer {SESSION_ID}",
# }
#
# api_endpoint = f"{BASE_URL}/api/{API_VERSION}/app/cdm/forms/actions/setdata"
#
# # Send JSON payloads to the API
# for payload in json_payloads:
#     response = requests.post(api_endpoint, headers=headers, data=json.dumps(payload))
#     response_json = response.json()  # Parse response if it's JSON
#     print(json.dumps(response_json, indent=4))
