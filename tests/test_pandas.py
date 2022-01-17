"""Tests for the pandas helpers in the pd_helpers.py module."""
import pytest

from pandas.testing import assert_frame_equal

from tests.conftest import create_dataframe
from ons_utils.pandas import *


def test_nested_dict_to_df():
    """Test for nested_dict_to_df."""
    input_d = {
        'bones': {
            'femur': {'tendons': 24},
            'humerus': {'tendons': 14},
        },
        'muscles': {
            'gluteus_maximus': {'tendons': 18},
        },
        'cars': 7,
    }

    actual = nested_dict_to_df(
        input_d,
        columns=['number'],
        level_names=('a', 'b', 'c'),
    )

    expected = create_dataframe([
        ('a', 'b', 'c', 'number'),
        ('bones', 'femur', 'tendons', 24),
        ('bones', 'humerus', 'tendons', 14),
        ('cars', None, None, 7),
        ('muscles', 'gluteus_maximus', 'tendons', 18),
    ])

    assert_frame_equal(
        # Sort values as dict order not preserved.
        actual.sort_values(['a', 'b']),
        # Set index because function returns a MultiIndex.
        expected.set_index(['a', 'b', 'c'])
    )


class TestStacker:
    """Group of tests for Stacker."""

    @pytest.mark.skip(reason="test shell")
    def test_Stacker(self):
        """Test for Stacker."""
        pass


@pytest.mark.skip(reason="test shell")
def test_convert_level_to_datetime():
    """Test for this."""
    pass


class TestMultiIndexSlicer:
    """Group of tests for MultiIndexSlicer."""

    @pytest.mark.skip(reason="test shell")
    def test_MultiIndexSlicer(self):
        """Test for MultiIndexSlicer."""
        pass


@pytest.mark.skip(reason="test shell")
def test_get_index_level_values():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_shifted_within_year_apply():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_shifted_within_year_ffill():
    """Test for this."""
    pass
