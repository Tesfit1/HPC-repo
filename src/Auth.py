import os
from dotenv import load_dotenv
import requests
from utils.config_utils import (
    API_VERSION, BASE_URL, CLIENT_ID, CLIENT_SECRET
)

SESSION_FILE = "./session_id.txt"

def get_session_id():
    url = f"{BASE_URL}/api/{API_VERSION}/auth"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
    }
    data = {
        'username': CLIENT_ID,
        'password': CLIENT_SECRET
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        response_json = response.json()
        print(f"Authentication response: {response_json}")
        session_id = response_json.get('session_id') or response_json.get('sessionId')
        if session_id:
            return session_id
        else:
            print("Error: 'session_id' or 'sessionId' not found in the response.")
            return None
    except Exception as e:
        print(f"Error during authentication: {e}")
        return None

def save_session_id(session_id, session_file_path=SESSION_FILE):
    try:
        with open(session_file_path, "w") as f:
            f.write(session_id)
        print(f"Session ID saved successfully at {session_file_path}.")
    except Exception as e:
        print(f"Error saving session ID: {e}")

if __name__ == "__main__":
    session_id = get_session_id()
    if session_id:
        save_session_id(session_id)