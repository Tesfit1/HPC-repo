import os
from dotenv import load_dotenv

# Load environment variables from .env file (only once)
load_dotenv()



# API and Study Config
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
STUDY_NAME = os.getenv("Study_name")
STUDY_COUNTRY = os.getenv("Study_country")
SITE = os.getenv("site")

# AWS S3 Config
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("bucket_name")

# File names (add more as needed)
FILE_NAMES = {
    "casebook": os.getenv("Casebook_file_name"),
    "subject_ev_date": os.getenv("Subject_Ev_Date"),
    "ic": os.getenv("Event_IC"),
    "event_dm": os.getenv("Event_DM"),
    "visitzero": os.getenv("visitzero"),
    "vital_sign_screening": os.getenv("VitalSignScreening"),
    "vital_sign_treatment": os.getenv("VitalSignTreatment"),
    "vital_sign_eos": os.getenv("VitalSignEOS"),
    "event_date": os.getenv("EventDate"),
    "substance_use": os.getenv("SubstanceUse"),
    "eligibility": os.getenv("Eligibility"),
    "inc_exc": os.getenv("incExc"),
    "drug_admin": os.getenv("drugAdmin"),
    "blood_sampling_for_pk": os.getenv("BloodSamplingForPK"),
    "eos": os.getenv("EOS"),
    "event_date_tst": os.getenv("EventDateTST"),
    "treatment_summary": os.getenv("TreatmentSummary"),
    "ie": os.getenv("IE"),
    "interval_sampling": os.getenv("IntervalSampling"),
    "wic": os.getenv("wic"),
}

# Session file path
SESSION_FILE = os.getenv("SESSION_FILE", "session_id.txt")

# Python path (if needed)
PYTHON_PATH = os.getenv("python_path")

# Add any other centralized config here as needed