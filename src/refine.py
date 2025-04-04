import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Environment variables
env_vars = {
    "API_VERSION": os.getenv("API_VERSION"),
    "BASE_URL": os.getenv("BASE_URL"),
    "SESSION_ID": os.getenv("SESSION_ID"),
    "study_name": os.getenv("Study_name"),
    "study_country": os.getenv("Study_country"),
    "site": os.getenv("site")
}

# Read a comma-delimited .txt file
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_SAMP_TPT_PK_FULL_2022FEB171150.txt')

df = (pd.read_csv(csv_file_path, delimiter='|', dtype=str)
        .rename(columns={
            'Subject Number': 'subject',
            'Time Point': 'TPT_TPT',
            'Sample Date': 'DAT_TPT',
            'Sample Time': 'TIM_TPT',
            'Comment': 'COM_TPT'
        }))
df['PERF_TPT'] = df['DAT_TPT'].notnull().map({True: 'Y', False: 'N'})

# Date conversion
df['DAT_TPT'] = pd.to_datetime(df['DAT_TPT'], format='%d %b %Y', errors='coerce').dt.strftime('%Y-%m-%d')
df = df.fillna("")

# Prepare JSON payloads
json_payloads = [
    {
        "study_name": env_vars["study_name"],
        "reopen": True,
        "submit": True,
        "change_reason": "Updated by the integration",
        "externally_owned": True,
        "form": {
            "study_country": env_vars["study_country"],
            "site": env_vars["site"],
            "subject": row['subject'],
            "eventgroup_name": "eg_TREAT_CO",
            "event_name": "ev_v02",
            "form_name": "SAMP_TPT_01_v001",
            "itemgroups": [
                {
                    "itemgroup_name": "ig_SAMP_TPT_01_A",
                    "itemgroup_sequence": idx + 1,
                    "items": [
                        {"item_name": "TPT_TPT", "value": row['TPT_TPT']},
                        {"item_name": "PERF_TPT", "value": row['PERF_TPT']},
                        {"item_name": "DAT_TPT", "value": row['DAT_TPT']},
                        {"item_name": "TIM_TPT", "value": row['TIM_TPT']},
                        {"item_name": "COM_TPT", "value": row['COM_TPT']}
                    ]
                }
            ]
        }
    }
    for idx, row in df.iterrows()
]

# Define the API endpoint
api_endpoint = f"{env_vars['BASE_URL']}/api/{env_vars['API_VERSION']}/app/cdm/forms/actions/setdata"

# Send JSON payloads to the API
with requests.Session() as session:
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {env_vars['SESSION_ID']}"
    })
    for payload in json_payloads:
        response = session.post(api_endpoint, json=payload)
        response_json = response.json()
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
# # Environment variables
# env_vars = {
#     "API_VERSION": os.getenv("API_VERSION"),
#     "BASE_URL": os.getenv("BASE_URL"),
#     "SESSION_ID": os.getenv("SESSION_ID"),
#     "study_name": os.getenv("Study_name"),
#     "study_country": os.getenv("Study_country"),
#     "site": os.getenv("site")
# }
#
# # Read a comma-delimited .txt file
# current_dir = os.path.dirname(os.path.abspath(__file__))
# parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
# csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_SAMP_TPT_PK_FULL_2022FEB171150.txt')
#
# df = (pd.read_csv(csv_file_path, delimiter='|', dtype=str)
#         .rename(columns={
#             'Subject Number': 'subject',
#             'Time Point': 'TPT_TPT',
#             'Sample Date': 'DAT_TPT',
#             'Sample Time': 'TIM_TPT',
#             'Comment': 'COM_TPT'
#         }))
# df['PERF_TPT'] = df['DAT_TPT'].notnull().map({True: 'Y', False: 'N'})
#
# # Date conversion
# df['DAT_TPT'] = pd.to_datetime(df['DAT_TPT'], format='%d %b %Y', errors='coerce').dt.strftime('%Y-%m-%d')
# df = df.fillna("")
#
# # Define the API endpoint
# api_endpoint = f"{env_vars['BASE_URL']}/api/{env_vars['API_VERSION']}/app/cdm/forms/actions/setdata"
#
# # Send JSON payloads to the API
# with requests.Session() as session:
#     session.headers.update({
#         "Content-Type": "application/json",
#         "Accept": "application/json",
#         "Authorization": f"Bearer {env_vars['SESSION_ID']}"
#     })
#     for idx, row in df.iterrows():
#         payload = {
#             "study_name": env_vars["study_name"],
#             "reopen": True,
#             "submit": True,
#             "change_reason": "Updated by the integration",
#             "externally_owned": True,
#             "form": {
#                 "study_country": env_vars["study_country"],
#                 "site": env_vars["site"],
#                 "subject": row['subject'],
#                 "eventgroup_name": "eg_TREAT_CO",
#                 "event_name": "ev_v02",
#                 "form_name": "SAMP_TPT_01_v001",
#                 "itemgroups": [
#                     {
#                         "itemgroup_name": "ig_SAMP_TPT_01_A",
#                         "itemgroup_sequence": idx + 1,
#                         "items": [
#                             {"item_name": "TPT_TPT", "value": row['TPT_TPT']},
#                             {"item_name": "PERF_TPT", "value": row['PERF_TPT']},
#                             {"item_name": "DAT_TPT", "value": row['DAT_TPT']},
#                             {"item_name": "TIM_TPT", "value": row['TIM_TPT']},
#                             {"item_name": "COM_TPT", "value": row['COM_TPT']}
#                         ]
#                     }
#                 ]
#             }
#         }
#         response = session.post(api_endpoint, json=payload)
#         response_json = response.json()
#         print(json.dumps(response_json, indent=4))