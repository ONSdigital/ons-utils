"""A set of unit tests for ."""
import math

from chispa import assert_df_equality
import pandas as pd
from pyspark.sql import (
    Column as SparkCol,
    functions as F,
)
import pytest

from cprices.utils.spark_helpers import *
from cprices.utils.spark_helpers import _convert_to_spark_col
from tests.conftest import create_dataframe, Case, parametrize_cases


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
    """Test the decorator func to_spark_col and its helper _convert_to_spark_col."""

    @pytest.mark.parametrize(
        's',
        ['price', 'chicken_wings', '£$%^&*(!', 'the quick brown fox'],
    )
    def test_convert_to_spark_col_positive_case(self, s, spark_session):
        """Test positive case for convert_to_spark_col."""
        assert isinstance(_convert_to_spark_col(s), SparkCol)

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
    def test_convert_to_spark_col_negative_case(self, obj, spark_session):
        """Test negative case for convert_to_spark_col."""
        result = _convert_to_spark_col(obj)
        assert isinstance(result, type(obj))
        assert result == obj

    def test_convert_to_spark_col_returns_nan_if_passed_nan(self, spark_session):
        """Test negative case for convert_to_spark_col."""
        result = _convert_to_spark_col(float('nan'))
        assert isinstance(result, type(float('nan')))
        assert math.isnan(result)

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


class TestConcat:
    """Tests for the concat function."""

    @pytest.fixture()
    def french_cheese(self, to_spark):
        """Input Spark dataframe of French cheeses."""
        return to_spark(create_dataframe([
            ('name', 'crumbliness', 'maturity', 'tang', 'creaminess'),
            ('brie', 0, 2, 1, 4),
            ('camembert', 0, 2, 2, 4),
            ('roquefort', 3, 4, 5, 2),
        ]))

    @pytest.fixture
    def greek_cheese(self, to_spark):
        """Input Spark dataframe of Greek cheeses."""
        return to_spark(create_dataframe([
            ('name', 'crumbliness', 'maturity', 'tang', 'creaminess'),
            ('feta', 5, 1, 2, 1),
            ('halloumi', 1, 1, 1, 1),
        ]))

    @pytest.fixture
    def british_cheese(self, to_spark):
        """Input Spark dataframe of British cheeses."""
        return to_spark(create_dataframe([
            ('name', 'crumbliness', 'maturity', 'tang', 'creaminess'),
            ('cheddar', 3, 4, 4, 2),
            ('caerphilly', 3, 3, 2, 2),
        ]))

    @pytest.fixture
    def italian_cheese(self, to_spark):
        """Input Spark dataframe of Italian cheeses.

        Has different columns to the other input dataframes in this test class.
        """
        return to_spark(create_dataframe([
            ('name', 'creaminess', 'saltiness'),
            ('buffalo mozzarella', 4, 3),
            ('ricotta', 5, 1),
        ]))

    @pytest.fixture
    def cheese_list(self, french_cheese, greek_cheese, british_cheese):
        """The three cheese input dataframes with same columns as list."""
        return [french_cheese, greek_cheese, british_cheese]

    @pytest.fixture
    def cheese_list_diff_cols(self, british_cheese, italian_cheese):
        """The three cheese input dataframes with same columns as list."""
        return [british_cheese, italian_cheese]

    @pytest.fixture
    def cheese_dict(self, french_cheese, greek_cheese, british_cheese):
        """The three cheese input dataframes with same columns as a dict."""
        return {
            'french': french_cheese,
            'greek': greek_cheese,
            'british': british_cheese,
        }

    @pytest.fixture
    def solo_keys_expected(self):
        """Return the expected output with the country keys."""
        return create_dataframe([
            ('country', 'name', 'crumbliness', 'maturity', 'tang', 'creaminess'),
            ('french', 'brie', 0, 2, 1, 4),
            ('french', 'camembert', 0, 2, 2, 4),
            ('french', 'roquefort', 3, 4, 5, 2),
            ('greek', 'feta', 5, 1, 2, 1),
            ('greek', 'halloumi', 1, 1, 1, 1),
            ('british', 'cheddar', 3, 4, 4, 2),
            ('british', 'caerphilly', 3, 3, 2, 2),
        ])

    @parametrize_cases(
        Case(
            "with_no_additional_columns_when_only_frames_passed",
            input_data=pytest.lazy_fixture('cheese_list'),
            keys=None,
            names=None,
            expected=create_dataframe([
                ('name', 'crumbliness', 'maturity', 'tang', 'creaminess'),
                ('brie', 0, 2, 1, 4),
                ('camembert', 0, 2, 2, 4),
                ('roquefort', 3, 4, 5, 2),
                ('feta', 5, 1, 2, 1),
                ('halloumi', 1, 1, 1, 1),
                ('cheddar', 3, 4, 4, 2),
                ('caerphilly', 3, 3, 2, 2),
            ])
        ),
        Case(
            "with_additional_column_when_keys_and_names_passed",
            input_data=pytest.lazy_fixture('cheese_list'),
            keys=['french', 'greek', 'british'],
            names='country',
            expected=pytest.lazy_fixture('solo_keys_expected')
        ),
        Case(
            "with_two_additional_columns_with_tuple_keys",
            input_data=pytest.lazy_fixture('cheese_list'),
            keys=[('french', 'no'), ('greek', 'yes'), ('british', 'yes')],
            names=['country', 'tasted'],
            expected=create_dataframe([
                ('country', 'tasted', 'name', 'crumbliness', 'maturity', 'tang', 'creaminess'),
                ('french', 'no',  'brie', 0, 2, 1, 4),
                ('french', 'no',  'camembert', 0, 2, 2, 4),
                ('french', 'no',  'roquefort', 3, 4, 5, 2),
                ('greek', 'yes', 'feta', 5, 1, 2, 1),
                ('greek', 'yes', 'halloumi', 1, 1, 1, 1),
                ('british', 'yes', 'cheddar', 3, 4, 4, 2),
                ('british', 'yes', 'caerphilly', 3, 3, 2, 2),
            ])
        ),
        Case(
            "can_concatenate_two_dfs_with_different_columns",
            marks=pytest.mark.xfail(reason="requires pyspark v3.1.0 and code change"),
            input_data=pytest.lazy_fixture('cheese_list_diff_cols'),
            keys=['british', 'italian'],
            names=['country'],
            expected=create_dataframe([
                ('country', 'tasted', 'name', 'crumbliness', 'maturity', 'tang', 'creaminess', 'saltiness'),
                ('british', 'yes', 'cheddar', 3, 4, 4, 2, None),
                ('british', 'yes', 'caerphilly', 3, 3, 2, 2, None),
                ('italian', 'buffalo mozzarella', None, None, None, None, 4, 3),
                ('italian', 'ricotta', None, None, None, None, 5, 1),
            ])
        ),
    )
    def test_union_dataframes_sequence_input_cases(
        self, to_spark,
        input_data, keys, names, expected,
    ):
        """Test all the positive cases for unioning dataframes with concat."""
        actual = concat(input_data, names, keys)

        assert_df_equality(actual, to_spark(expected), ignore_nullable=True)

    def test_unions_all_dataframes_in_mapping_input_when_keys_is_None(
        self, to_spark, cheese_dict, solo_keys_expected
    ):
        """Test that the dataframes in a dict or unioned with names
        as new column name and the values given by dict keys.
        """
        actual = concat(cheese_dict, names='country')
        assert_df_equality(actual, to_spark(solo_keys_expected), ignore_nullable=True)

    def test_unions_selection_of_dataframes_in_mapping_input_when_keys_is_passed(
        self, to_spark, cheese_dict,
    ):
        """Test that only a selection of dataframes in the dict or
        unioned with names as new column name and the values given by
        dict keys.
        """
        actual = concat(
            cheese_dict,
            names='country',
            keys=['french', 'british'],
        )
        expected = to_spark(create_dataframe([
            ('country', 'name', 'crumbliness', 'maturity', 'tang', 'creaminess'),
            ('french', 'brie', 0, 2, 1, 4),
            ('french', 'camembert', 0, 2, 2, 4),
            ('french', 'roquefort', 3, 4, 5, 2),
            ('british', 'cheddar', 3, 4, 4, 2),
            ('british', 'caerphilly', 3, 3, 2, 2),
        ]))
        assert_df_equality(actual, expected, ignore_nullable=True)

    @parametrize_cases(
        Case("when_frames_is_sequence", frames=[]),
        Case("when_frames_is_dict", frames=dict()),
    )
    def test_raises_value_error_with_empty_frames_input(self, frames):
        """Test raises value error with empty frames input."""
        with pytest.raises(ValueError):
            concat(frames)

    def test_raises_value_error_when_keys_not_same_len_as_frames(self, cheese_list):
        """Tests ValueError raised correctly."""
        with pytest.raises(ValueError):
            # cheese_list is len(3)
            concat(cheese_list, names='country', keys=['french', 'british'])

    def test_raises_value_error_when_mapping_passed_but_no_names_param(self, cheese_dict):
        """Tests ValueError raised correctly."""
        with pytest.raises(ValueError):
            concat(cheese_dict)

    def test_raises_value_error_when_keys_are_not_of_equal_length(self, cheese_list):
        """Tests ValueError raised correctly."""
        with pytest.raises(ValueError):
            concat(
                cheese_list,
                names=['country', 'tasted'],
                # Middle key is only 1 item, rather than 2 needed.
                keys=[('french', 'no'), 'greek', ('british', 'yes')]
            )

    def test_raises_value_error_when_key_length_is_different_to_names_length(self, cheese_list):
        """Tests ValueError raised correctly."""
        with pytest.raises(ValueError):
            # Names len is 1 buy key len is 2.
            concat(
                cheese_list,
                names=['country'],
                keys=[('french', 'no'), ('greek', 'yes'), ('british', 'yes')]
            )
        with pytest.raises(ValueError):
            # Names len is 2 buy key len is 1.
            concat(
                cheese_list,
                names=['country', 'tasted'],
                keys=['french', 'greek', 'british'],
            )

    @parametrize_cases(
        Case("spark_dataframe", frames=pytest.lazy_fixture('french_cheese')),
        Case("str", frames='my_dataframe'),
    )
    def test_raises_type_error_if_incorrect_type_passed_to_frames(self, frames):
        """Test TypeError raised correctly."""
        with pytest.raises(TypeError):
            concat(frames)

    def test_raises_type_error_if_objs_in_frames_are_not_all_spark_dataframes(
        self, british_cheese
    ):
        """Test TypeError raised correctly."""
        with pytest.raises(TypeError):
            concat([pd.DataFrame(), pd.DataFrame()])
            concat([british_cheese, pd.DataFrame()])
            concat(['my_df', 7, True])
