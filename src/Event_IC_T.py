import json
import os
from config import API_VERSION, BASE_URL, study_name, study_country, site, headers, convert_date_format
import pandas as pd
from error_log import  log_error, read_dataframe
from importer import import_form

# Read a comma-delimited .txt file
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_IC_FULL_2024JUL101011.txt')

df = read_dataframe(csv_file_path)

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

api_endpoint = f"{BASE_URL}/api/{API_VERSION}/app/cdm/forms/actions/setdata"

for payload in json_payloads:
    try:
        import_form(payload, api_endpoint, headers)
    except Exception as e:
        log_error(e)
