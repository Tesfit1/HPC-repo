import os
import importlib

def test_env_variables_loaded(monkeypatch):
    # Set dummy environment variables
    monkeypatch.setenv("CLIENT_ID", "dummy_client_id")
    monkeypatch.setenv("CLIENT_SECRET", "dummy_secret")
    monkeypatch.setenv("API_VERSION", "v1")
    monkeypatch.setenv("BASE_URL", "http://localhost")
    monkeypatch.setenv("Study_name", "StudyX")
    monkeypatch.setenv("Study_country", "CountryX")
    monkeypatch.setenv("site", "SiteX")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "dummy_aws_key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "dummy_aws_secret")
    monkeypatch.setenv("bucket_name", "dummy_bucket")
    monkeypatch.setenv("Casebook_file_name", "casebook.csv")
    monkeypatch.setenv("SESSION_FILE", "session_id.txt")

    # Reload config_utils to pick up monkeypatched env vars
    config_utils = importlib.reload(importlib.import_module("utils.config_utils"))

    # Check that config values are loaded and not None
    assert config_utils.CLIENT_ID == "dummy_client_id"
    assert config_utils.CLIENT_SECRET == "dummy_secret"
    assert config_utils.API_VERSION == "v1"
    assert config_utils.BASE_URL == "http://localhost"
    assert config_utils.STUDY_NAME == "StudyX"
    assert config_utils.STUDY_COUNTRY == "CountryX"
    assert config_utils.SITE == "SiteX"
    assert config_utils.AWS_ACCESS_KEY_ID == "dummy_aws_key"
    assert config_utils.AWS_SECRET_ACCESS_KEY == "dummy_aws_secret"
    assert config_utils.BUCKET_NAME == "dummy_bucket"
    assert config_utils.FILE_NAMES["casebook"] == "casebook.csv"
    assert config_utils.SESSION_FILE == "session_id.txt"