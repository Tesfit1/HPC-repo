import os
import logging
import pytest
from utils import error_log_utils

def test_form_data_error():
    with pytest.raises(error_log_utils.FormDataError):
        raise error_log_utils.FormDataError("Form data error")

def test_api_error():
    with pytest.raises(error_log_utils.APIError):
        raise error_log_utils.APIError("API error")

def test_custom_file_not_found_error():
    with pytest.raises(error_log_utils.CustomFileNotFoundError):
        raise error_log_utils.CustomFileNotFoundError("File not found")

def test_invalid_session_id_error():
    with pytest.raises(error_log_utils.InvalidSessionIDError):
        raise error_log_utils.InvalidSessionIDError("Invalid session")

def test_log_error(caplog):
    with caplog.at_level(logging.ERROR):
        error_log_utils.log_error(Exception("Test error"))
        assert "Test error" in caplog.text

def test_check_file_exists(tmp_path):
    # Should not raise for existing file
    file = tmp_path / "exists.txt"
    file.write_text("data")
    error_log_utils.check_file_exists(str(file))
    # Should raise for missing file
    with pytest.raises(error_log_utils.CustomFileNotFoundError):
        error_log_utils.check_file_exists(str(tmp_path / "missing.txt"))