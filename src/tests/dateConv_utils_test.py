from utils.dateConv_utils import convert_date_format

def test_convert_date_format_valid():
    assert convert_date_format("25 Jun 2024") == "2024-06-25"

def test_convert_date_format_invalid():
    assert convert_date_format("invalid-date") is None

def test_convert_date_format_none():
    assert convert_date_format(None) is None

def test_convert_date_format_empty():
    assert convert_date_format("") is None