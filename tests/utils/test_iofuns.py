"""Tests for the io.py module."""
from tests.conftest import (
    Case,
    parametrize_cases,
)

from cprices.utils.iofuns import *


@parametrize_cases(
    Case(
        label="remove_nuts",
        dev_config={
            'groupby_cols': ['col_1', 'col_2', 'nuts1_name'],
            'preprocess_cols': {
                'scanner': ['col_3', 'nuts1_name', 'nuts2_name'],
                'web_scraped': ['col_4', 'nuts1_name', 'nuts2_name', 'nuts3_name']
            }
        },
        expected={
            'groupby_cols': ['col_1', 'col_2'],
            'preprocess_cols': {
                'scanner': ['col_3'],
                'web_scraped': ['col_4']
            }
        },
        col_to_remove='nuts',
    ),
    Case(
        label="remove_store_type",
        dev_config={
            'groupby_cols': ['col_1', 'col_2', 'store_type'],
            'preprocess_cols': {
                'scanner': ['col_3', 'store_type'],
                'web_scraped': ['col_4', 'store_type']
            }
        },
        expected={
            'groupby_cols': ['col_1', 'col_2'],
            'preprocess_cols': {
                'scanner': ['col_3'],
                'web_scraped': ['col_4']
            }
        },
        col_to_remove='store',
    ),
)
def test_remove_scenario(dev_config, expected, col_to_remove):
    """Test nuts col and store type col removed as expected."""
    assert remove_scenario(dev_config, col_to_remove) == expected
