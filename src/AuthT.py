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

#Auth

url =  f"{BASE_URL}/api/{API_VERSION}/auth"
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': 'application/json',
}

# Define the data
data = {
    'username': CLIENT_ID,
    'password': CLIENT_SECRET
}

# Send the POST request
response = requests.post(url, headers=headers, data=data)

# Print the response
print(f"Authentication response: " + str(response.json()))
# Access the cookies
response_json = response.json()

# Store the session ID
session_id = response_json['sessionId']  # replace 'session_id' with the actual cookie name
load_dotenv()

# Step 2: Write the session_id to the .env file
env_file_path = ".env"  # Path to your .env file

# Open the .env file in append mode, and write the new session_id (if it's not already there)
with open(env_file_path, "a") as env_file:
    env_file.write(f"SESSION_ID={session_id}\n")



