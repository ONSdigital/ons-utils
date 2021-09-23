"""Tests for pipeline utils."""
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import *
import pytest

from tests.conftest import (
    Case,
    create_dataframe,
    parametrize_cases,
)
from cprices.utils.pipeline_utils import *


@pytest.mark.skip(reason="test shell")
def test_combine_scenario_df_outputs():
    """Test for this."""
    pass


@pytest.mark.skip(reason="requires mocking of env variables")
def test_create_run_id():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_timer_args():
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


def test_check_empty(spark_session):
    """Test the check_empty function raises DataFrameEmptyError."""
    # Create an empty DataFrame.
    empty_RDD = spark_session.sparkContext.emptyRDD()
    df = spark_session.createDataFrame(
        data=empty_RDD,
        schema=StructType([]),
    )

    with pytest.raises(DataFrameEmptyError):
        check_empty(df)


class TestApplyMapper:
    """Tests for the apply mapper function."""

    @pytest.fixture
    def df_one_key(self):
        """Create a dataaframe that will map to one column."""
        return create_dataframe([
                ('default_values', ),
                ('item_a', ),
                ('item_b', ),
                ('item_c', ),
        ])

    @pytest.fixture
    def mapper_one_key(self):
        """Create a mapper with column that exists in input dataframe."""
        return create_dataframe([
                ('default_values', 'new_values'),
                ('item_a', 'item_b'),
                ('item_c', 'item_d'),
        ])

    @parametrize_cases(
        Case(
            label="join_one_key",
            df=pytest.lazy_fixture('df_one_key'),
            mapper=pytest.lazy_fixture('mapper_one_key'),
            keys=['default_values'],
            mapped_col_name='new_values',
            column_to_fill='new_values',
            fill_values='default_values',
            expected=create_dataframe([
                ('default_values', 'new_values'),
                ('item_a', 'item_b'),
                ('item_b', 'item_b'),
                ('item_c', 'item_d'),
            ]),
        ),
        Case(
            label="join_two_keys",
            df=create_dataframe([
                ('default_values', 'retailer'),
                ('item_a', 'retailer_a'),
                ('item_b', 'retailer_a'),
                ('item_c', 'retailer_a'),
                ('item_a', 'retailer_b'),
                ('item_b', 'retailer_b'),
                ('item_c', 'retailer_b'),
            ]),
            mapper=create_dataframe([
                ('default_values', 'retailer', 'new_values'),
                ('item_a', 'retailer_a', 'item_b'),
                ('item_c', 'retailer_a', 'item_d'),
            ]),
            keys=['default_values', 'retailer'],
            mapped_col_name='new_values',
            column_to_fill='new_values',
            fill_values='default_values',
            expected=create_dataframe([
                ('default_values', 'retailer', 'new_values'),
                ('item_a', 'retailer_a', 'item_b'),
                ('item_b', 'retailer_a', 'item_b'),
                ('item_c', 'retailer_a', 'item_d'),
                ('item_a', 'retailer_b', 'item_a'),
                ('item_b', 'retailer_b', 'item_b'),
                ('item_c', 'retailer_b', 'item_c'),
            ]),
        ),
    )
    def test_apply_mapper(
        self,
        to_spark,
        df,
        mapper,
        keys,
        mapped_col_name,
        column_to_fill,
        fill_values,
        expected,
    ):
        """Test mapper joins and null values are updated in defined column."""
        actual = apply_mapper(
            to_spark(df),
            to_spark(mapper),
            keys,
            mapped_col_name,
            column_to_fill,
            fill_values,
        )

        assert_df_equality(
            actual,
            to_spark(expected),
            ignore_row_order=True,
        )

    def test_raises_error_with_string_key(
        self,
        to_spark,
        df_one_key,
        mapper_one_key,
    ):
        """Test error message is returned if key is string type."""
        with pytest.raises(TypeError):
            apply_mapper(
                df=to_spark(df_one_key),
                mapper=to_spark(mapper_one_key),
                keys='default_values',
                mapped_col_name='new_values',
                column_to_fill='new_values',
                fill_values='default_values',
            )

    def test_raises_error_with_incorrect_key(
        self,
        to_spark,
        df_one_key,
        mapper_one_key,
    ):
        """Test error message is returned if key exists on one side only."""
        with pytest.raises(ValueError):
            apply_mapper(
                df=to_spark(df_one_key),
                mapper=to_spark(mapper_one_key),
                keys=['new_values'],
                mapped_col_name='new_values',
                column_to_fill='new_values',
                fill_values='default_values',
            )
