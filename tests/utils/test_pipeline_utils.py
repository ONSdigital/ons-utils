"""Tests for pipeline utils."""
import pytest

from tests.conftest import (
    Case,
    parametrize_cases,
)

from cprices.utils.pipeline_utils import *


@pytest.mark.skip(reason="test shell")
def test_timer_args():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_combine_scenario_df_outputs():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_create_run_id():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_get_run_times_as_df():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_plot_run_times():
    """Test for this."""
    pass


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
def test_remove_config_param(dev_config, expected, col_to_remove):
    """Test nuts col and store type col removed as expected."""
    assert remove_config_param(dev_config, col_to_remove) == expected
