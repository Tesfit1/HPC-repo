import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd
from datetime import datetime

load_dotenv()
#from CreateCasebookT import subjects

# Step 1: Load the CSV file into a DataFrame
# a: define varialbes and parameters
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
SESSION_ID = os.getenv("SESSION_ID")
study_name = os.getenv("Study_name")
study_country = os.getenv("Study_country")
site = os.getenv("site")




# b: define the headers
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {SESSION_ID}",
}

current_dir = os.path.dirname(os.path.abspath(__file__))

# Move one directory level up
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

# Path to the CSV file
csv_file_path = os.path.join(parent_dir, '1234-5678_TEST_HPC_SV_FULL_2024JUL101030.txt')

 # Update with  CSV file path
df = pd.read_csv(csv_file_path, delimiter='|',dtype=str)

# Rename columns while keeping original data intact

df = df.rename(columns={
    'Subject Number': 'subject',

})

def preprocess_dataframe(df):
    # Convert date format
    def convert_date_format(date_str):
        try:
            return datetime.strptime(date_str, "%d %b %Y").strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None  # Handle missing or invalid dates

    df["Visit Date"] = df["Visit Date"].apply(convert_date_format)
    return df
df = preprocess_dataframe(df)

event_date = df['Visit Date'].tolist()


data = {
	"study_name": study_name,
    "events": [
        {
            "study_country": study_country,
            "site": site,
            "subject": row['subject'],
            "eventgroup_name": (
                'eg_SCREEN' if row['Folder'] == 'V01' else
                # 'eg_TREAT_CO' if row['Folder'] == 'V02' else
                # 'eg_TREAT_CO' if row['Folder'] == 'V03' else
                'eg_TREAT_CO'
            ),
            "eventgroup_sequence": 1,
            "event_name": (
                'ev_V01' if row['Folder'] == 'V01' else
                'ev_v02' if row['Folder'] == 'V02' else
                'ev_V03' if row['Folder'] == 'V03' else
                'ev_V04'
            ),
            "date": row['Visit Date'],
            "change_reason": 'Subject creation'

        }
        for _, row in df.iterrows()
    ]
}

print(json.dumps(data, indent=4))



#Send the POST request

url =  f"{BASE_URL}/api/{API_VERSION}/app/cdm/events/actions/setdate"

post_setdate = requests.post(url, headers=headers, data=json.dumps(data))
response_json = post_setdate.json()  # This directly parses the response if it's JSON
print(json.dumps(response_json, indent=4))