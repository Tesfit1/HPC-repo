import json
import requests
from error_log import APIError, InvalidSessionIDError, log_error

def import_form(payload, api_endpoint, headers, validate_form_data=None):
    try:
        if validate_form_data and not validate_form_data(payload):
            raise APIError(f"Invalid data in form {payload['form']['subject']}")

        response = requests.post(api_endpoint, headers=headers, data=json.dumps(payload))
        response_json = response.json()

        # Check for itemgroup/item-specific errors
        if response_json.get("responseStatus") == "FAILURE":
            form = response_json.get("form", {})
            for itemgroup in form.get("itemgroups", []):
                if itemgroup.get("responseStatus") == "FAILURE":
                    for item in itemgroup.get("items", []):
                        if item.get("responseStatus") == "FAILURE":
                            error_msg = (
                                f"Subject: {form.get('subject')}, "
                                f"ItemGroup: {itemgroup.get('itemgroup_name')}, "
                                f"Item: {item.get('item_name')}, "
                                f"Error: {item.get('errorMessage')}"
                            )
                            print(error_msg)
                            log_error(error_msg)
            raise APIError(f"Form import failed for subject {payload['form']['subject']}")

        if response.status_code == 200:
            if any(error.get('type') == 'INVALID_SESSION_ID' for error in response_json.get('errors', [])):
                raise InvalidSessionIDError("Invalid or expired session ID.")

        if response.status_code != 200:
            raise APIError(f"API call failed for form {payload['form']['subject']} with status code {response.status_code}")

        print(json.dumps(response_json, indent=4))

    except Exception as e:
        log_error(f"Unexpected error importing form {payload['form']['subject']}: {e}")
        raise e
    