import pandas as pd
import pytest
from utils import data_utils

def test_validate_columns_pass():
    df = pd.DataFrame({'a': [1], 'b': [2]})
    data_utils.validate_columns(df, ['a', 'b'])  # Should not raise

def test_validate_columns_fail():
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(KeyError):
        data_utils.validate_columns(df, ['a', 'b'])

def test_rename_columns():
    df = pd.DataFrame({'old': [1]})
    df2 = data_utils.rename_columns(df, {'old': 'new'})
    assert 'new' in df2.columns

def test_apply_value_mappings():
    df = pd.DataFrame({'col': ['Yes', 'No']})
    config = {'value_mappings': {'col': {'Yes': 1, 'No': 0}}}
    df2 = data_utils.apply_value_mappings(df, config)
    assert set(df2['col']) == {1, 0}

def test_preprocess_dataframe():
    df = pd.DataFrame({'a': [1]})
    def add_col(df): df['b'] = 2; return df
    df2 = data_utils.preprocess_dataframe(df, preprocess_funcs=[add_col])
    assert 'b' in df2.columns

def test_build_json_payloads_basic():
    df = pd.DataFrame({'subject': ['s1'], 'DSSCAT_IC': ['MAIN'], 'DSREFID_IC': ['v1'], 'IC': ['Y'], 'DSSTDAT_IC': ['2024-01-01']})
    config = {
        'item_mappings': {'DSSCAT_IC': 'DSSCAT_IC', 'DSREFID_IC': 'DSREFID_IC', 'IC': 'IC', 'DSSTDAT_IC': 'DSSTDAT_IC'},
        'form_name': 'IC_01_v002',
        'itemgroup_name': 'ig_IC_01_A',
        'eventgroup_name': 'eg_COMMON',
        'event_name': 'ev_COMMON'
    }
    payloads = data_utils.build_json_payloads(df, config, 'study', 'country', 'site')
    assert isinstance(payloads, list)
    assert payloads[0]['form']['form_name'] == 'IC_01_v002'
    assert payloads[0]['form']['subject'] == 's1'

def test_build_event_payloads_basic():
    df = pd.DataFrame({'subject': ['s1'], 'date': ['2024-01-01']})
    config = {
        'eventgroup_name': 'eg_COMMON',
        'event_name': 'ev_COMMON',
        'date_column': 'date'
    }
    result = data_utils.build_event_payloads(df, config, 'study', 'country', 'site')
    assert 'events' in result
    assert result['events'][0]['subject'] == 's1'
    assert result['events'][0]['date'] == '2024-01-01'