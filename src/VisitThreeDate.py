import json
from dotenv import load_dotenv
import requests
import os
from datetime import datetime
from dateConv import convert_date_format
import pandas as pd
import boto3
from error_log import FormDataError, APIError, FileNotFoundError, InvalidSessionIDError, log_error, check_file_exists
from io import StringIO 

load_dotenv()
#from CreateCasebookT import subjects

# Step 1: Load the CSV file into a DataFrame
# a: define varialbes and parameters 
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
SESSION_FILE = "session_id.txt"
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")

eventgroup_name = 'eg_TREAT_SD'
eventgroup_sequence = 1
event_name = 'ev_V03'

aws_access = os.getenv("aws_access_key_id")
aws_secret = os.getenv("aws_secret_access_key")
file_name = os.getenv("EventDate")
bucket_name = os.getenv("bucket_name")



# b: define the headers
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {SESSION_ID}",
}

# Read a comma-delimited .txt file
s3 = boto3.client('s3', aws_access_key_id= aws_access, aws_secret_access_key=aws_secret)

try:
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)
except FileNotFoundError as e:
    log_error(e)
    raise

# Rename columns while keeping original data intact

df = df.rename(columns={
    'Subject Number': 'subject',
    'Informed Consent Date':'DSSTDAT_IC'
})

def preprocess_dataframe(df):
    df["DSSTDAT_IC"] = df["DSSTDAT_IC"].apply(convert_date_format)
    return df
df = preprocess_dataframe(df)
# set the event date
#event_date = df['DSSTDAT_IC']
#event_date = str(df['DSSTDAT_IC'].iloc[0])
event_date = df['DSSTDAT_IC'].tolist()

#print(type(event_date))
#print(event_date)
#subjects = subjects.tolist() if isinstance(subjects, pd.Series) else subjects
data = {
	"study_name": study_name,
    "events": [
        {
            "study_country": study_country,
            "site": site,
            "subject": row['subject'],
            "eventgroup_name": eventgroup_name,
            "eventgroup_sequence": eventgroup_sequence,
            "event_name": event_name,
            "date": row['DSSTDAT_IC'],
            "change_reason": 'Subject creation',
            "method": "on_site_visit__v",
            "allow_planneddate_override": False,
            "externally_owned_date": True,            
            "externally_owned_method": False,
            "change_reason": "Action performed via the API"
        }
        for _, row in df.iterrows()
    ]
}

print(json.dumps(data, indent=4))



#Send the POST request
# this post request is not setdata but update since there is method time defined 

url =  f"{BASE_URL}/api/{API_VERSION}/app/cdm/events/actions/update"

post_setdate = requests.post(url, headers=headers, data=json.dumps(data))
response_json = post_setdate.json()  # This directly parses the response if it's JSON
print(json.dumps(response_json, indent=4))