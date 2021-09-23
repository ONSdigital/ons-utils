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


class TestApplyRemapMapper:
    """Tests for the apply_remap_mapper function."""

    @pytest.fixture
    def input_df(self):
        """Create a dataframe that will map to one column."""
        return create_dataframe([
                ('default_values', ),
                ('item_a', ),
                ('item_b', ),
                ('item_c', ),
        ])

    @pytest.fixture
    def mapper_df(self):
        """Create a mapper with column that exists in input dataframe."""
        return create_dataframe([
                ('default_values', 'new_values'),
                ('item_a', 'item_b'),
                ('item_c', 'item_d'),
        ])

    @parametrize_cases(
        Case(
            label="keys_works_as_string",
            df=pytest.lazy_fixture('input_df'),
            mapper=pytest.lazy_fixture('mapper_df'),
            expected=create_dataframe([
                ('default_values', 'new_values'),
                ('item_a', 'item_b'),
                ('item_b', 'item_b'),
                ('item_c', 'item_d'),
            ]),
            keys='default_values',
            mapped_col_name='new_values',
            column_to_fill='new_values',
            fill_values='default_values',
        ),
        Case(
            label="overwrite_null_new_values_with_default_values",
            df=pytest.lazy_fixture('input_df'),
            mapper=pytest.lazy_fixture('mapper_df'),
            expected=create_dataframe([
                ('default_values', 'new_values'),
                ('item_a', 'item_b'),
                ('item_b', 'item_b'),
                ('item_c', 'item_d'),
            ]),
            keys=['default_values'],
            mapped_col_name='new_values',
            column_to_fill='new_values',
            fill_values='default_values',
        ),
        Case(
            label="overwrite_new_values_with_default_values",
            df=pytest.lazy_fixture('input_df'),
            mapper=pytest.lazy_fixture('mapper_df'),
            expected=create_dataframe([
                ('default_values', 'new_values'),
                ('item_a', 'item_a'),
                ('item_b', 'item_b'),
                ('item_c', 'item_c'),
            ]),
            keys=['default_values'],
            mapped_col_name='new_values',
            column_to_fill='default_values',
            fill_values='new_values',
        ),
        Case(
            label="overwrite_default_values_with_new_values",
            df=pytest.lazy_fixture('input_df'),
            mapper=pytest.lazy_fixture('mapper_df'),
            expected=create_dataframe([
                ('default_values', 'new_values'),
                ('item_b', 'item_b'),
                ('item_b', None),
                ('item_d', 'item_d'),
            ]),
            keys=['default_values'],
            mapped_col_name='default_values',
            column_to_fill='new_values',
            fill_values='default_values',
        ),
        Case(
            label="overwrite_null_default_values_with_new_values",
            df=pytest.lazy_fixture('input_df'),
            mapper=pytest.lazy_fixture('mapper_df'),
            expected=create_dataframe([
                ('default_values', 'new_values'),
                ('item_a', 'item_b'),
                ('item_b', None),
                ('item_c', 'item_d'),
            ]),
            keys=['default_values'],
            mapped_col_name='default_values',
            column_to_fill='default_values',
            fill_values='new_values',
        ),
        Case(
            label="join_relaunch_mapper",
            df=create_dataframe([
                ('sku', 'retailer'),
                ('item_a', 'retailer_a'),
                ('item_b', 'retailer_a'),
                ('item_c', 'retailer_a'),
                ('item_a', 'retailer_b'),
                ('item_b', 'retailer_b'),
                ('item_c', 'retailer_b'),
            ]),
            mapper=create_dataframe([
                ('sku', 'retailer', 'relaunch_sku'),
                ('item_a', 'retailer_a', 'item_b'),
                ('item_c', 'retailer_a', 'item_d'),
            ]),
            keys=['sku', 'retailer'],
            mapped_col_name='relaunch_sku',
            column_to_fill='relaunch_sku',
            fill_values='sku',
            expected=create_dataframe([
                ('sku', 'retailer', 'relaunch_sku'),
                ('item_a', 'retailer_a', 'item_b'),
                ('item_b', 'retailer_a', 'item_b'),
                ('item_c', 'retailer_a', 'item_d'),
                ('item_a', 'retailer_b', 'item_a'),
                ('item_b', 'retailer_b', 'item_b'),
                ('item_c', 'retailer_b', 'item_c'),
            ]),
        ),
        Case(
            label="join_grouping_mapper",
            df=create_dataframe([
                ('product_id', 'supplier'),
                ('item_a', 'supplier_a'),
                ('item_b', 'supplier_a'),
                ('item_c', 'supplier_a'),
                ('item_a', 'supplier_b'),
                ('item_b', 'supplier_b'),
                ('item_c', 'supplier_b'),
            ]),
            mapper=create_dataframe([
                ('product_id', 'supplier', 'group_id'),
                ('item_a', 'supplier_a', 'group_z'),
                ('item_c', 'supplier_a', 'group_y'),
            ]),
            keys=['product_id', 'supplier'],
            mapped_col_name='product_id',
            column_to_fill='group_id',
            fill_values='product_id',
            expected=create_dataframe([
                ('product_id', 'supplier', 'group_id'),
                ('group_z', 'supplier_a', 'group_z'),
                ('item_b', 'supplier_a', None),
                ('group_y', 'supplier_a', 'group_y'),
                ('item_a', 'supplier_b', None),
                ('item_b', 'supplier_b', None),
                ('item_c', 'supplier_b', None),
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
        actual = apply_remap_mapper(
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
