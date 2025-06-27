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
from utils.dateConv_utils import convert_date_format

check_file_exists(SESSION_FILE)
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()

form_config = FORM_CONFIGS["IC"]

# Read data from S3
df = read_s3_csv(
    bucket=BUCKET_NAME,
    key=FILE_NAMES["ic"],
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

# Preprocess (date conversion)
def my_preprocess(df):
    df["DSSTDAT_IC"] = df["DSSTDAT_IC"].apply(convert_date_format)
    return df

df = preprocess_dataframe(df, preprocess_funcs=[my_preprocess])

# Build payloads: one form per consent record, with form_sequence per subject
json_payloads = []
subject_form_counter = {}

for idx, row in df.iterrows():
    subject = row['subject']
    # Increment form sequence for each subject
    if subject not in subject_form_counter:
        subject_form_counter[subject] = 1
    else:
        subject_form_counter[subject] += 1
    form_sequence = subject_form_counter[subject]

    itemgroup = {
        "itemgroup_name": form_config["itemgroup_name"],
        "itemgroup_sequence": 1,
        "items": [
            {"item_name": "DSSCAT_IC", "value": row['DSSCAT_IC']},
            {"item_name": "DSREFID_IC", "value": row['DSREFID_IC']},
            {"item_name": "IC", "value": row['IC']},
            {"item_name": "DSSTDAT_IC", "value": row['DSSTDAT_IC']}
        ]
    }

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
            "form_sequence": form_sequence,
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
            log_error(f"Validation error: {field} is missing in form for subject {form.get('subject')}")
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