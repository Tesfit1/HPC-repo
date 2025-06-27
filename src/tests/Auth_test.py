import pytest
from unittest.mock import patch, MagicMock
from src import Auth

def test_get_session_id_success(monkeypatch):
    mock_response = MagicMock()
    mock_response.json.return_value = {'session_id': 'abc123'}
    mock_response.raise_for_status.return_value = None

    with patch("src.Auth.requests.post", return_value=mock_response):
        session_id = Auth.get_session_id()
        assert session_id == 'abc123'

def test_get_session_id_no_session(monkeypatch):
    mock_response = MagicMock()
    mock_response.json.return_value = {}
    mock_response.raise_for_status.return_value = None

    with patch("src.Auth.requests.post", return_value=mock_response):
        session_id = Auth.get_session_id()
        assert session_id is None

def test_get_session_id_exception(monkeypatch):
    with patch("src.Auth.requests.post", side_effect=Exception("fail")):
        session_id = Auth.get_session_id()
        assert session_id is None

def test_save_session_id(tmp_path):
    session_file = tmp_path / "session.txt"
    Auth.save_session_id("test-session", session_file)
    with open(session_file) as f:
        assert f.read() == "test-session"