import json
from utils.error_log_utils import (
    FormDataError, APIError, InvalidSessionIDError, log_error, check_file_exists
)
from utils.api_utils import import_form
from utils.s3_utils import read_s3_csv
from utils.config_utils import (
    API_VERSION, BASE_URL, STUDY_NAME, STUDY_COUNTRY, SITE,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME,
    FILE_NAMES, SESSION_FILE
)
from utils.form_config_utils import FORM_CONFIGS
from utils.data_utils import (
    validate_columns, rename_columns, preprocess_dataframe, apply_value_mappings
)
import pandas as pd

check_file_exists(SESSION_FILE)
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()

form_config = FORM_CONFIGS["Inc_Ex"]

# Read data from S3
df = read_s3_csv(
    bucket=BUCKET_NAME,
    key=FILE_NAMES["ie"],
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    delimiter='|'
)

# Validate and rename columns
validate_columns(df, form_config["required_columns"])
df = rename_columns(df, form_config["rename_map"])

# Apply value mappings
df = apply_value_mappings(df, form_config)
df = df.fillna("")

# Preprocess if needed
df = preprocess_dataframe(df)

# Group by subject and CTPNUMG
grouped = df.groupby(['subject', 'CTPNUMG'])

json_payloads = []

for (subject, CTPNUMG), group in grouped:
    first_row = group.iloc[0]
    IEYN = first_row['IEYN']

    itemgroups = [
        {
            "itemgroup_name": form_config["itemgroup_name_A"],
            "itemgroup_sequence": 1,
            "items": [
                {"item_name": "CTPNUMG", "value": CTPNUMG},
                {"item_name": "IEYN", "value": IEYN}
            ]
        }
    ]

    # Add repeating ig_IE_01_B itemgroups for each row with IECAT and IENUM
    for idx, row in group.iterrows():
        if pd.notna(row['IECAT']) and pd.notna(row['IENUM']):
            itemgroups.append({
                "itemgroup_name": form_config["itemgroup_name_B"],
                "itemgroup_sequence": int(idx - group.index[0] + 1),
                "items": [
                    {"item_name": "IECAT", "value": row['IECAT']},
                    {"item_name": "IENUM", "value": row['IENUM']}
                ]
            })

    json_body = {
        "study_name": STUDY_NAME,
        "reopen": True,
        "submit": True,
        "change_reason": "Updated by the integration",
        "externally_owned": True,
        "form": {
            "study_country": STUDY_COUNTRY,
            "site": SITE,
            "subject": subject,
            "eventgroup_name": form_config["eventgroup_name"],
            "event_name": form_config["event_name"],
            "form_name": form_config["form_name"],
            "itemgroups": itemgroups
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
            msg = f"Validation error: {field} is missing in form for subject {form.get('subject')}"
            print(msg)
            log_error(msg)
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