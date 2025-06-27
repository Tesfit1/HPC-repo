import logging
import os

# Configure logging with timestamp, level, and message
logging.basicConfig(
    filename='import_errors.log',
    level=logging.ERROR,
    format='%(asctime)s %(levelname)s %(message)s'
)

# Optional: Also log to console
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# Custom exceptions
class FormDataError(Exception):
    """Exception raised for errors in the form data."""
    pass

class APIError(Exception):
    """Exception raised for errors in the API call."""
    pass

class CustomFileNotFoundError(Exception):
    """Exception raised when a file is not found."""
    pass

class InvalidSessionIDError(Exception):
    """Exception raised for invalid or expired session ID."""
    pass

def log_error(exception):
    """Log the provided exception with traceback info."""
    logging.error(str(exception), exc_info=True)

def check_file_exists(file_path):
    """Check if the file exists and raise an exception if it does not."""
    if not os.path.isfile(file_path):
        raise CustomFileNotFoundError(f"File not found: {file_path}")