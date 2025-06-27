import pandas as pd
from unittest.mock import patch, MagicMock
from utils import s3_utils

def test_read_s3_csv_success():
    # Mock boto3 client and response
    mock_s3 = MagicMock()
    mock_body = MagicMock()
    mock_body.read.return_value = b"col1|col2\nval1|val2"
    mock_s3.get_object.return_value = {"Body": mock_body}
    with patch("utils.s3_utils.boto3.client", return_value=mock_s3):
        df = s3_utils.read_s3_csv(
            bucket="bucket",
            key="key",
            aws_access_key_id="id",
            aws_secret_access_key="secret"
        )
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["col1", "col2"]
        assert df.iloc[0]["col1"] == "val1"
        assert df.iloc[0]["col2"] == "val2"

def test_read_s3_csv_file_not_found():
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {}
    with patch("utils.s3_utils.boto3.client", return_value=mock_s3):
        try:
            s3_utils.read_s3_csv(
                bucket="bucket",
                key="key",
                aws_access_key_id="id",
                aws_secret_access_key="secret"
            )
        except FileNotFoundError as e:
            assert "not found in bucket" in str(e)
        else:
            assert False, "FileNotFoundError was not raised"