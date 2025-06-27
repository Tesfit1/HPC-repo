import pytest
from utils import form_config_utils

def test_form_configs_keys_exist():
    # Check that expected form configs are present
    expected_keys = [
        "BloodSamplingForPK", "Demographics", "DrugAdmin", "Eligibility", "EOS",
        "Inc_Ex", "IC", "IntervalSampling", "SubstanceUse", "TreatmentSummary",
        "VitalSignEOS", "VitalSignScreening", "VitalSignTreatment", "WithdrawalIC"
    ]
    for key in expected_keys:
        assert key in form_config_utils.FORM_CONFIGS

def test_required_columns_and_rename_map():
    # Check that each config has required_columns and rename_map
    for config in form_config_utils.FORM_CONFIGS.values():
        assert "required_columns" in config
        assert isinstance(config["required_columns"], list)
        assert "rename_map" in config
        assert isinstance(config["rename_map"], dict)

def test_item_mappings_and_form_name():
    # Check that each config has item_mappings and form_name
    for config in form_config_utils.FORM_CONFIGS.values():
        assert "item_mappings" in config
        assert isinstance(config["item_mappings"], dict)
        assert "form_name" in config
        assert isinstance(config["form_name"], str)

def test_event_configs_keys_exist():
    # Check that expected event configs are present
    expected_event_keys = [
        "VisitTwoDate", "VisitThreeDate", "VisitFourDate", "VisitPlaceDate", "SetEventDateEndOfStudy"
    ]
    for key in expected_event_keys:
        assert key in form_config_utils.EVENT_CONFIGS

def test_event_configs_required_fields():
    # Check that each event config has required_columns, rename_map, eventgroup_name, event_name, date_column
    for config in form_config_utils.EVENT_CONFIGS.values():
        assert "required_columns" in config
        assert isinstance(config["required_columns"], list)
        assert "rename_map" in config
        assert isinstance(config["rename_map"], dict)
        assert "eventgroup_name" in config
        assert "event_name" in config
        assert "date_column" in config