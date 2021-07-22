"""Tests for the io.py module."""
from cprices.utils.iofuns import *


def test_remove_str_from_list():
    """Test the string is removed as expected."""
    input_data = ['col_1', 'nuts1_name', 'nuts2_name', 'nuts3_name']
    expected = ['col_1']

    actual = remove_str_from_list(input_data, 'nuts')

    assert actual == expected


def test_remove_nuts():
    """Test that nuts regions are removed as expected from dev_config."""
    dev_config = {
        'groupby_cols': ['col_123', 'col_321', 'nuts1_name'],
        'preprocess_cols': {
            'scanner': ['col_1', 'col_2', 'nuts1_name'],
            'web_scraped': ['col_3', 'nuts2_name', 'nuts3_name']
        },
    }

    expected = {
        'groupby_cols': ['col_123', 'col_321'],
        'preprocess_cols': {
            'scanner': ['col_1', 'col_2'],
            'web_scraped': ['col_3']
        },
    }

    actual = remove_nuts(dev_config, 'nuts')

    assert actual == expected
