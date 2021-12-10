"""Tests for functions in the _decorators.py module."""
from pyspark.sql import Column as SparkCol

from ons_utils.decorators import *


@to_spark_col
def dummy_func(s1: str, s2: str):
    """Dummy function for test."""
    return s1, s2


@to_spark_col(exclude='s2')
def dummy_func_with_one_excluded(s1: str, s2: str):
    """Dummy function for test."""
    return s1, s2


@to_spark_col(exclude=['s1', 's2'])
def dummy_func_with_both_excluded(s1: str, s2: str):
    """Dummy function for test."""
    return s1, s2


class TestToSparkCol:
    """Test the decorator func to_spark_col and it's helper _convert_to_spark_col."""

    def test_converts_str_args_to_col(self, spark_session):
        """Test that using a function decorated with to_spark_col converts
        str args to spark.sql.Column type.
        """
        s1, s2 = dummy_func('cheese', 'biscuits')
        assert isinstance(s1, SparkCol)
        assert isinstance(s2, SparkCol)

    def test_converts_str_args_and_str_kwargs_to_col(self, spark_session):
        """Test that using a function decorated with to_spark_col converts
        str args and kwargs to pyspark.sql.Column.
        """
        s1, s2 = dummy_func('cheese', s2='biscuits')
        assert isinstance(s1, SparkCol)
        assert isinstance(s2, SparkCol)

        s3, s4 = dummy_func(s1='cheese', s2='biscuits')
        assert isinstance(s3, SparkCol)
        assert isinstance(s4, SparkCol)

    def test_doesnt_convert_exclusions(self, spark_session):
        """Test that using a function decorated with to_spark_col, and passing
        function parameters to exclude, then those parameters aren't converted
        to pyspark.sql.Column.
        """
        s1, s2 = dummy_func_with_one_excluded('cheese', 'biscuits')
        assert isinstance(s1, SparkCol)
        # Returns str instead because it's excluded from converting to SparkCol.
        assert isinstance(s2, str)

        s3, s4 = dummy_func_with_one_excluded(s1='cheese', s2='biscuits')
        assert isinstance(s3, SparkCol)
        assert isinstance(s4, str)

        # Both excluded case.
        s5, s6 = dummy_func_with_both_excluded('cheese', s2='biscuits')
        assert isinstance(s5, str)
        assert isinstance(s6, str)
