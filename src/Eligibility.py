import json
from utils.error_log_utils import (
    FormDataError, APIError, CustomFileNotFoundError, InvalidSessionIDError,
    log_error, check_file_exists
)
from utils.api_utils import import_forms_bulk
from utils.s3_utils import read_s3_csv
from utils.config_utils import (
    API_VERSION, BASE_URL, STUDY_NAME, STUDY_COUNTRY, SITE,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME,
    FILE_NAMES, SESSION_FILE
)
from utils.form_config_utils import FORM_CONFIGS
from utils.data_utils import validate_columns, rename_columns, preprocess_dataframe, build_json_payloads, apply_value_mappings
from utils.dateConv_utils import convert_date_format

check_file_exists(SESSION_FILE)
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()

form_config = FORM_CONFIGS["Eligibility"]

# Read data
df = read_s3_csv(
    bucket=BUCKET_NAME,
    key=FILE_NAMES["eligibility"],
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    delimiter='|'
)

# Validate and rename columns
validate_columns(df, form_config["required_columns"])
df = rename_columns(df, form_config["rename_map"])

# Map values
df = apply_value_mappings(df, form_config)

# Preprocess (example: date conversion)
def my_preprocess(df):
    df["DSSTDAT_ELIG"] = df["DSSTDAT_ELIG"].apply(convert_date_format)
    return df

df = preprocess_dataframe(df, preprocess_funcs=[my_preprocess])
df = df.fillna("")

# Build payloads
json_payloads = build_json_payloads(df, form_config, STUDY_NAME, STUDY_COUNTRY, SITE)

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
try:
    import_forms_bulk(json_payloads, api_endpoint, headers, validate_form_data)
except Exception as e:
    print(f"Bulk import error: {e}")
    log_error(e)