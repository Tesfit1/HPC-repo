import importlib
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

SCRIPTS_TO_TEST = [
    "BloodSamplingForPK",
    "CreateSubject",
    "DrugAdmin",
    "Eligibility",
    "EOS",
    "Inc_Ex",
    "InformedConsent",
    "IntervalSampling",
    "SetEventDateEndOfStudy",
    "SubstanceUse",
    "TreatmentSummaryV2",
    "VisitFourDate",
    "VisitOneDate",
    "VisitPlaceDate",
    "VisitThreeDate",
    "VisitTwoDate",
    "VitalSignScreening",
    "VitalSignTreatment",
    "vitalSignEOS",
    "WithdrawalIC",
]

SCRIPT_CONFIG_MAP = {
    "BloodSamplingForPK": "BloodSamplingForPK",
    "CreateSubject": "CreateSubject",
    "DrugAdmin": "DrugAdmin",
    "Eligibility": "Eligibility",
    "EOS": "EOS",
    "Inc_Ex": "Inc_Ex",
    "InformedConsent": "InformedConsent",
    "IntervalSampling": "IntervalSampling",
    "SetEventDateEndOfStudy": "SetEventDateEndOfStudy",
    "SubstanceUse": "SubstanceUse",
    "TreatmentSummaryV2": "TreatmentSummaryV2",
    "VisitFourDate": "VisitFourDate",
    "VisitOneDate": "VisitOneDate",
    "VisitPlaceDate": "VisitPlaceDate",
    "VisitThreeDate": "VisitThreeDate",
    "VisitTwoDate": "VisitTwoDate",
    "VitalSignScreening": "VitalSignScreening",
    "VitalSignTreatment": "VitalSignTreatment",
    "vitalSignEOS": "VitalSignEOS",
    "WithdrawalIC": "WithdrawalIC",
}

SCRIPT_EXTRA_COLUMNS = {
    "VisitOneDate": ["DSSTDAT_IC"],
    "VisitPlaceDate": ["Subject Number", "Folder", "Visit Date"],
    "VisitThreeDate": ["Subject Number", "Informed Consent Date"],
    "VisitTwoDate": ["Subject Number", "Informed Consent Date"],
    # Add more as needed for other scripts
}

# For scripts that access columns not in required_columns, add them here
SCRIPT_FORCE_COLUMNS = {
    "VitalSignTreatment": [
        "Folder", "VSTPT_1", "VSORRES_1", "VSORRESU_1", "VSDAT_1", "VSTIM_1", "SYSBP_1"
    ],
    "EOS": [
        "Subject Number", "EOS Date", "Completed", "Reason Non-Completion", "Death date", "Lost to FUP date", "DSNCOMP_EOS"
    ],
    "InformedConsent": [
        "Subject Number", "Informed Consent Type", "Informed Consent Version ID", "Informed Consent Obtained", "Informed Consent Date"
    ],
    "SetEventDateEndOfStudy": [
        "Subject Number", "Informed Consent Date"
    ],
    "TreatmentSummaryV2": [
        "Subject Number", "Subject completed", "Treatment discontinuation decision", "Primary trt discontinuation reason", "Reason Subject Not Treated"
    ],
    "VisitFourDate": [
        "Subject Number", "Informed Consent Date"
    ],
    "withdrawal_IC": [
        "Subject Number", "Informed Consent Type", "Informed Consent Version ID", "Informed Consent Obtained", "Informed Consent Date"
    ],
    # Add more as needed
}

@pytest.mark.parametrize("script_name", SCRIPTS_TO_TEST)
def test_script_import_and_run(script_name, monkeypatch, tmp_path):
    try:
        form_config_utils = importlib.import_module("utils.form_config_utils")
        config_key = SCRIPT_CONFIG_MAP.get(script_name, script_name)
        form_config = form_config_utils.FORM_CONFIGS.get(config_key)
        if form_config and "required_columns" in form_config:
            required_columns = form_config["required_columns"]
        else:
            required_columns = []
    except Exception:
        required_columns = []

    extra_columns = SCRIPT_EXTRA_COLUMNS.get(script_name, [])
    force_columns = SCRIPT_FORCE_COLUMNS.get(script_name, [])

    # Only use original/raw column names (not renamed ones)
    # Remove any renamed columns from force_columns
    if form_config and "rename_map" in form_config:
        renamed = set(form_config["rename_map"].values())
        force_columns = [col for col in force_columns if col not in renamed]

    # Fallback to ["subject", "site"] if no required columns
    base_columns = required_columns if required_columns else ["subject", "site"]
    all_columns = list(dict.fromkeys(base_columns + extra_columns + force_columns))

    # If both are present, keep only the one needed by the script
    if "Subject Number" in all_columns and "subject" in all_columns:
        all_columns.remove("subject")

    # Remove any remaining duplicates (including DSNCOMP_EOS)
    seen = set()
    all_columns = [x for x in all_columns if not (x in seen or seen.add(x))]

    # Assert no duplicates remain
    assert len(all_columns) == len(set(all_columns)), f"Duplicate columns found: {all_columns}"

    dummy_data = {col: ["dummy"] for col in all_columns}
    df = pd.DataFrame(dummy_data)

    # Patch boto3.client for scripts that use it directly
    boto3_client_patch = patch("boto3.client", return_value=MagicMock())

    with patch("utils.s3_utils.read_s3_csv", return_value=df), \
         patch("utils.api_utils.import_form", return_value=None), \
         patch("utils.api_utils.import_forms_bulk", return_value=None), \
         patch("requests.post", return_value=MagicMock(json=lambda: {})), \
         patch("requests.get", return_value=MagicMock(json=lambda: {})), \
         patch("builtins.open", new_callable=MagicMock), \
         patch("utils.error_log_utils.check_file_exists", return_value=True), \
         boto3_client_patch:
        monkeypatch.setenv("SESSION_FILE", str(tmp_path / "session_id.txt"))
        try:
            importlib.import_module(f"src.{script_name}")
        except Exception as e:
            print(f"{script_name} df.columns: {df.columns.tolist()}")
            pytest.fail(f"Script {script_name} raised an exception: {e}")