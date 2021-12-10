"""Tests for the general Spark utilities."""
from chispa import assert_df_equality
import pytest

from ons_utils.pyspark import *
from tests.conftest import create_dataframe

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
            float('nan'),
        ]
    )
    def test_negative_cases(self, obj, spark_session):
        """Test negative case for convert_to_spark_col."""
        with pytest.raises(ValueError):
            convert_to_spark_col(obj)


class TestGetWindowSpec:
    """Tests for get_window_spec function."""

    @pytest.fixture
    def input_df_get_window(self, to_spark):
        """Input dataframe for get_window_spec tests."""
        return to_spark(
            pd.DataFrame({
                'group': ['A', 'A', 'B', 'B', 'B'],
                'val': [5, 2, 1, 9, 6],
            })
        )

    def test_with_levels_passed(self, input_df_get_window, to_spark):
        """Test that get_window_spec results in the expected output when
        levels are passed.
        """
        actual_df = input_df_get_window.withColumn(
            'sum(val)',
            F.sum('val').over(get_window_spec(levels='group'))
        )

        expected_df_sum_groups = to_spark(
            pd.DataFrame({
                'group': ['A', 'A', 'B', 'B', 'B'],
                'val': [5, 2, 1, 9, 6],
                'sum(val)': [7, 7, 16, 16, 16],
            })
        )

        assert_df_equality(actual_df, expected_df_sum_groups)

    def test_with_no_levels_passed(self, input_df_get_window, to_spark):
        """Test that get_window_spec results in the expected output when
        no levels are passed.
        """
        actual_df = input_df_get_window.withColumn(
            'sum(val)', F.sum('val').over(get_window_spec())
        )

        expected_df_sum_all = to_spark(
            pd.DataFrame({
                'group': ['A', 'A', 'B', 'B', 'B'],
                'val': [5, 2, 1, 9, 6],
                'sum(val)': [23, 23, 23, 23, 23],
            })
        )

        assert_df_equality(actual_df, expected_df_sum_all)


class TestMapCol:
    """Tests for map_col pyspark helper."""

    def test_maps_simple_python_dict(self, to_spark):
        """Simple test for map_col working with a Python native dict."""
        df = to_spark(pd.DataFrame([1, 2, 3, 4], columns=['position']))
        mapping = {1: 'first', 2: 'second', 3: 'third'}

        actual = df.withColumn('ranking', map_col('position', mapping))

        expected = to_spark(
            pd.DataFrame({
                'position': [1, 2, 3, 4],
                'ranking': ['first', 'second', 'third', None]
            })
        )

        assert_df_equality(actual, expected)

    def test_maps_python_dict_with_list(self, to_spark):
        """Test that map_col can create an array column if the dict maps to a list."""
        df = to_spark(pd.DataFrame(['tiger', 'lion'], columns=['animal']))
        mapping = {'tiger': ['orange', 'stripy'], 'lion': ['golden', 'king']}

        actual = df.withColumn('attribute', map_col('animal', mapping))

        expected = to_spark(
            pd.DataFrame({
                'animal': ['tiger', 'lion'],
                'attribute': [['orange', 'stripy'], ['golden', 'king']]
            })
        )

        assert_df_equality(actual, expected, ignore_nullable=True)


def test_map_column_names(to_spark):
    """Test column names are mapped to given values."""
    input_df = to_spark(create_dataframe([
        ('col_A', 'col_B', 'col_Y', 'col_D', 'col_Z'),
        ('aaa',   'bbb',   'ccc',   'ddd',   'eee'),
    ]))

    actual = map_column_names(
        input_df,
        {'col_Y': 'col_C', 'col_Z': 'col_E'},
    )

    expected = to_spark(create_dataframe([
        ('col_A', 'col_B', 'col_C', 'col_D', 'col_E'),
        ('aaa',   'bbb',   'ccc',   'ddd',   'eee'),
    ]))

    assert_df_equality(actual, expected)


@pytest.mark.skip(reason="test shell")
def test_to_list():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_list_convert():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_is_list_or_tuple():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_get_hive_table_columns():
    """Test for this."""
    pass
