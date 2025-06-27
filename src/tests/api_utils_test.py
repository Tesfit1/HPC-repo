import json
import requests
from utils.error_log_utils import log_error, APIError, InvalidSessionIDError

def import_form(payload, api_endpoint, headers, validate_form_data=None):
    try:
        if validate_form_data and not validate_form_data(payload):
            raise APIError(f"Invalid data in form {payload['form']['subject']}")

        response = requests.post(api_endpoint, headers=headers, data=json.dumps(payload))
        response_json = response.json()

        # Print the full response for debugging
        print(json.dumps(response_json, indent=4))

        # Check for invalid session ID error first
        if any(error.get('type') == 'INVALID_SESSION_ID' for error in response_json.get('errors', [])):
            raise InvalidSessionIDError("Invalid or expired session ID.")

        # Handle known subject-not-found error gracefully
        if response_json.get("responseStatus") == "FAILURE":
            errors = response_json.get("errors", [])
            error_message = response_json.get("errorMessage", "")
            subject = payload['form']['subject']

            # Check for subject not found
            if "[Subject] with name" in error_message and "not found" in error_message:
                msg = f"Subject not found in ClinData: {subject} -- skipping."
                print(msg)
                log_error(msg)
                return  # Do not raise, just skip

            # Handle other itemgroup/item-specific errors as before
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
            raise APIError(f"Form import failed for subject {subject}")

        if response.status_code != 200:
            raise APIError(f"API call failed for form {payload['form']['subject']} with status code {response.status_code}")

    except Exception as e:
        log_error(f"Unexpected error importing form {payload['form']['subject']}: {e}")
        raise e

def import_forms_bulk(payloads, api_endpoint, headers, validate_form_data=None):
    """
    Send multiple form payloads to the API endpoint.
    """
    for payload in payloads:
        import_form(payload, api_endpoint, headers, validate_form_data)