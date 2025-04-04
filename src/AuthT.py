import os
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
import requests


# Load environment variables from .env file
load_dotenv()

# Read environment variables
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")

# Auth

# Define the URL and headers
url =  f"{BASE_URL}/api/{API_VERSION}/auth"
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': 'application/json',
}

# Data definition
data = {
    'username': CLIENT_ID,
    'password': CLIENT_SECRET
}

# POST request
response = requests.post(url, headers=headers, data=data)

# response
print(f"Authentication response: " + str(response.json()))

# session ID
session_id = response.json()['sessionId']  

# .env file read
env_file_path = ".env"  

if os.path.exists(env_file_path):
    with open(env_file_path, "r") as env_file:
        lines = env_file.readlines()

# Remove any existing SESSION_ID 
updated_lines = [line for line in lines if not line.startswith("SESSION_ID=")]

# Add the new SESSION_ID
updated_lines.append(f"SESSION_ID={session_id}\n")

#  .env file overwrite
with open(env_file_path, "w") as env_file:
    env_file.writelines(updated_lines)

print(f"Updated .env file with new SESSION_ID.")
