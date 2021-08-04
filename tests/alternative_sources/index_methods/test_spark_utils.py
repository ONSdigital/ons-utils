"""Tests for the _spark_utils.py module."""
import math

import pytest

from cprices.alternative_sources.index_methods._spark_utils import *


class TestConvertToSparkCol:
    """Tests for convert_to_spark_col."""

    @pytest.mark.parametrize(
        's',
        ['price', 'chicken_wings', '£$%^&*(!', 'the quick brown fox'],
    )
    def test_positive_cases(self, s, spark_session):
        """Test positive case for convert_to_spark_col."""
        assert isinstance(convert_to_spark_col(s), SparkCol)

    @pytest.mark.parametrize(
        'obj',
        [
            None,
            67,
            True,
            7.68574,
            ['car', 'van'],
            (10, 'green', 'bottles'),
            {'ace': 'spades', 'queen': 'hearts'},
        ]
    )
    def test_negative_cases(self, obj, spark_session):
        """Test negative case for convert_to_spark_col."""
        result = convert_to_spark_col(obj)
        assert isinstance(result, type(obj))
        assert result == obj

    def test_returns_nan_if_passed_nan(self, spark_session):
        """Test negative case for convert_to_spark_col."""
        result = convert_to_spark_col(float('nan'))
        assert isinstance(result, type(float('nan')))
        assert math.isnan(result)
