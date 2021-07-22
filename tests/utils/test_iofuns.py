"""Tests for the io.py module."""
from cprices.utils.iofuns import *


def test_remove_nuts():
    """Test the nuts1_name is removed as expected."""
    input_data = {
        'groupby_cols': ['col_1', 'col_2', 'col_3', 'nuts1_name'],
        'preprocess_cols': {
            'scanner': ['col_1', 'col_2', 'col_3', 'nuts1_name'],
            'web_scraped': ['col_1', 'col_2', 'col_3', 'nuts1_name'],
        },
    }

    expected = {
        'groupby_cols': ['col_1', 'col_2', 'col_3'],
        'preprocess_cols': {
            'scanner': ['col_1', 'col_2', 'col_3'],
            'web_scraped': ['col_1', 'col_2', 'col_3'],
        },
    }

    actual = remove_nuts(input_data)

    assert actual == expected
