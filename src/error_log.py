import logging
import os

# Configure logging
logging.basicConfig(filename='import_errors.log', level=logging.ERROR)

# Custom exceptions

class FormDataError(Exception):
    """Exception raised for errors in the form data."""
    pass

class APIError(Exception):
    """Exception raised for errors in the API call."""
    pass

class FileNotFoundError(Exception):
    """Exception raised when a file is not found."""
    pass
class InvalidSessionIDError(Exception):
    """Exception raised for invalid or expired session ID."""
    pass


def log_error(exception):
    """Log the provided exception."""
    logging.error(exception)

def check_file_exists(file_path):
    """Check if the file exists and raise an exception if it does not."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
