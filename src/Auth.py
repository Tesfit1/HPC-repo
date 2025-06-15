# import os
# from dotenv import load_dotenv
# from requests.auth import HTTPBasicAuth
# import requests

# load_dotenv()

# # Read environment variables
# CLIENT_ID = os.getenv("CLIENT_ID")
# CLIENT_SECRET = os.getenv("CLIENT_SECRET")
# API_VERSION = os.getenv("API_VERSION")
# BASE_URL = os.getenv("BASE_URL")

# # Auth

# # Define the URL and headers
# url =  f"{BASE_URL}/api/{API_VERSION}/auth"
# headers = {
#     'Content-Type': 'application/x-www-form-urlencoded',
#     'Accept': 'application/json',
# }

# # Define the data
# data = {
#     'username': CLIENT_ID,
#     'password': CLIENT_SECRET
# }

# # Send the POST request
# response = requests.post(url, headers=headers, data=data)

# # Print the response
# print(f"Authentication response: " + str(response.json()))
# response_json = response.json()

# # Store the session ID
# if 'session_id' in response_json:
#     session_id = response_json['session_id']

#     # Use absolute path for shared session directory
#     # session_dir = "."
#     session_dir = '/opt/airflow/scripts/'
#     try:
#         os.makedirs(session_dir, exist_ok=True)
#         session_file_path = os.path.join(session_dir, "session_id.txt")
#         with open(session_file_path, "w") as f:
#             f.write(session_id)
#         print(f"Session ID saved successfully at {session_file_path}.")
#     except Exception as e:
#         print(f"Error saving session ID: {e}")
# else:
#     print("Error: 'sessionId' not found in the response.")



import os
from dotenv import load_dotenv
import requests

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")

url =  f"{BASE_URL}/api/{API_VERSION}/auth"
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': 'application/json',
}
data = {
    'username': CLIENT_ID,
    'password': CLIENT_SECRET
}

response = requests.post(url, headers=headers, data=data)
print(f"Authentication response: {response.json()}")
response_json = response.json()

# Use the correct key as per your API response
session_key = 'session_id' if 'session_id' in response_json else 'sessionId'
if session_key in response_json:
    session_id = response_json[session_key]
    print("Session ID to be written:", session_id)
    # session_file_path = '/opt/airflow/scripts/sessi session_file_path = '../session_id.txt'on_id.txt'
    session_file_path = "./session_id.txt"
    try:
        with open(session_file_path, "w") as f:
            f.write(session_id)
        print(f"Session ID saved successfully at {session_file_path}.")
    except Exception as e:
        print(f"Error saving session ID: {e}")
else:
    print(f"Error: '{session_key}' not found in the response.")