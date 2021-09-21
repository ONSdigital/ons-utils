"""A set of unit tests for ."""
import math

from chispa import assert_df_equality
import pandas as pd

from pyspark.sql import (
    Column as SparkCol,
    functions as F,
)
import pytest
from pytest_lazyfixture import lazy_fixture

from cprices.utils.spark_helpers import *
from cprices.utils.spark_helpers import (
    _convert_to_spark_col,
    _get_largest_number_dtype,
    _are_all_number_types,
    _get_column_types,
)
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


@pytest.mark.spark
@pytest.mark.unit
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
            input_data=lazy_fixture('cheese_list'),
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
            input_data=lazy_fixture('cheese_list'),
            keys=['french', 'greek', 'british'],
            names='country',
            expected=lazy_fixture('solo_keys_expected')
        ),
        Case(
            "with_two_additional_columns_with_tuple_keys",
            input_data=lazy_fixture('cheese_list'),
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
            input_data=lazy_fixture('cheese_list_diff_cols'),
            keys=['british', 'italian'],
            names=['country'],
            expected=[
                ('british', 'cheddar', 3, 4, 4, 2, None),
                ('british', 'caerphilly', 3, 3, 2, 2, None),
                ('italian', 'buffalo mozzarella', None, None, None, 4, 3),
                ('italian', 'ricotta', None, None, None, 5, 1),
            ],
            schema='country string, name string, crumbliness long, maturity long, tang long, creaminess long, saltiness long',
        ),
    )
    def test_union_dataframes_sequence_input_cases(
        self, to_spark,
        input_data, keys, names, expected, schema
    ):
        """Test all the positive cases for unioning dataframes with concat."""
        actual = concat(input_data, names, keys)

        assert_df_equality(actual, to_spark(expected, schema), ignore_nullable=True)

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

    def test_casts_lower_number_type_before_concat_to_prevent_nulls(
        self, create_spark_df
    ):
        """Test that when one dataframe is float and other int, the
        concat works by casting the int column to float before
        concatenating."""
        df1 = create_spark_df([
            ('breed',       'weight'),
            ('schnauzer',    7      ),
            ('bull mastiff', 12     ),
            ('chihuahua',    2      ),
        ])

        # Weights are ints in first df, but floats in this df.
        df2 = create_spark_df([
            ('breed',       'weight'),
            ('jack russell', 3.2    ),
            ('puli',         13.4   ),
            ('doberman',     14.8   ),
        ])

        actual = concat([df1, df2])

        # All weights are floats in the expected output.
        expected = create_spark_df([
            ('breed',       'weight'),
            ('schnauzer',    7.0    ),
            ('bull mastiff', 12.0   ),
            ('chihuahua',    2.0    ),
            ('jack russell', 3.2    ),
            ('puli',         13.4   ),
            ('doberman',     14.8   ),
        ])

        assert_df_equality(actual, expected)
        # Assert that the output has the same dtypes as the dataframe
        # with the higher precedence number type i.e. float.
        assert actual.dtypes == df2.dtypes

    def test_casts_column_to_str_if_concatenating_str_column_with_non_str_column(
        self, create_spark_df,
    ):
        """Test that if the type in a column of one of the frames to
        be concatenated is string, then the types of that column in the
        other frames should be cast to str before concatenating."""
        df1 = create_spark_df([
            ('store_type', 'branch'),
            ('1', 'outlet'),
            ('2', 'high street'),
        ])

        df2 = create_spark_df([
            ('store_type', 'branch'),
            (3, 'outlet'),
            (4, 'high street'),
        ])

        df3 = create_spark_df([
            ('store_type', 'branch'),
            (5, 3.2),
        ])

        actual = concat([df1, df2, df3])

        # Should convert the column in each dataframe to string, if the
        # dtype is str for one of the dataframes.
        expected = create_spark_df([
            ('store_type', 'branch'),
            ('1', 'outlet'),
            ('2', 'high street'),
            ('3', 'outlet'),
            ('4', 'high street'),
            ('5', '3.2'),
        ])

        assert_df_equality(actual, expected)

    def test_fills_missing_column_with_Nones_before_concatenating(
        self, create_spark_df,
    ):
        """When a column is missing, it should be filled with Nones
        before concatenating."""
        df1 = create_spark_df([
            ('unit',        'speed', 'attack'),
            ('camel_rider',  11,      9      ),
            ('knight',       13,      12     ),
        ])
        df2 = create_spark_df([
            ('unit',        'attack'),
            ('villager',     1      ),
            ('archer',       5      ),
        ])

        actual = concat([df1, df2])

        expected = create_spark_df([
            ('unit',        'speed', 'attack'),
            ('camel_rider',  11,      9      ),
            ('knight',       13,      12     ),
            ('villager',     None,    1      ),
            ('archer',       None,    5      ),
        ])

        assert_df_equality(actual, expected)

    def test_can_handle_differing_types_and_missing_columns(
        self, create_spark_df,
    ):
        """Test that concat is capable of coercing to the right types
        and filling missing columns."""
        df1 = create_spark_df([
            ('unit',        'speed', 'attack'),
            ('camel_rider',  11,      '9'      ),
            ('knight',       13,      '12'     ),
        ])
        df2 = create_spark_df([
            ('unit',        'attack'),
            ('villager',     1      ),
            ('archer',       5      ),
        ])
        df3 = create_spark_df([
            ('unit',        'speed'),
            ('monk',         2.2   ),
            ('ballista',     2.4   ),
        ])

        actual = concat([df1, df2, df3])

        expected = create_spark_df([
            ('unit',        'speed', 'attack'),
            ('camel_rider',  11.0,    '9'    ),
            ('knight',       13.0,    '12'   ),
            ('villager',     None,    '1'    ),
            ('archer',       None,    '5'    ),
            ('monk',         2.2,     None   ),
            ('ballista',     2.4,     None   ),
        ])

        assert_df_equality(actual, expected)
        assert actual.dtypes == [
            ('unit', 'string'),
            ('speed', 'double'),
            ('attack', 'string'),
        ]

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
        Case("spark_dataframe", frames=lazy_fixture('french_cheese')),
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

    def test_raises_type_error_when_trying_to_concatenate_two_column_types_that_cant_be_converted(
        self, create_spark_df,
    ):
        """Test TypeError raised correctly."""
        df1 = create_spark_df([
            ('date', 'speed'),
            (pd.Timestamp('2020-01-01'), 11),
        ])
        df2 = create_spark_df([
            ('date', 'speed'),
            (True, 1),
        ])

        with pytest.raises(TypeError):
            concat([df1, df2])

    def test_warns_when_dtypes_for_same_column_are_different(
        self, create_spark_df,
    ):
        """Test UnequalSchemaWarning warned correctly."""
        df1 = create_spark_df([
            ('id', 'animal'),
            (1, 'dog'),
            (2, 'cat'),
        ])
        # id are strings in df2
        df2 = create_spark_df([
            ('id', 'animal'),
            ('1', 'dog'),
            ('2', 'cat'),
        ])

        with pytest.warns(UnequalSchemaWarning):
            concat([df1, df2])

    def test_warns_if_column_exists_in_one_frame_but_not_others(
        self, create_spark_df,
    ):
        """Test UnequalSchemaWarning warned correctly."""
        df1 = create_spark_df([
            ('id', 'animal', 'name'),
            (1, 'dog', 'fido'),
            (2, 'cat', 'jimmy'),
        ])
        # id are strings in df2
        df2 = create_spark_df([
            ('id', 'animal'),
            (1, 'dog'),
            (2, 'cat'),
        ])

        with pytest.warns(UnequalSchemaWarning):
            concat([df1, df2])


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


@parametrize_cases(
    Case(
        input_schema=[
            ('shoe', 'string'),
            ('hat', 'string')
        ],
        column_name='shoe',
        expected={'string'},
    ),
    Case(
        input_schema=[
            ('speed', 'string'),
            ('attack', 'int'),
            ('speed', 'int'),
        ],
        column_name='speed',
        expected={'string', 'int'},
    ),
    Case(
        input_schema=[
            ('is_date', 'datetime'),
            ('is_date', 'int'),
            ('is_date', 'bool'),
        ],
        column_name='is_date',
        expected={'datetime', 'int', 'bool'},
    ),
)
def test_get_column_types(input_schema, column_name, expected):
    assert _get_column_types(input_schema, column_name) == expected


def test_are_all_number_types_positive_case():
    assert _are_all_number_types(
        ['float', 'bigint', 'tinyint', 'decimal(10,0)']
    )


def test_are_all_number_types_negative_case():
    assert _are_all_number_types(['float', 'string', 'bool']) is False


@parametrize_cases(
    Case(
        input_types=['tinyint', 'tinyint', 'int'],
        expected='int',
    ),
    Case(
        input_types=['bigint', 'tinyint', 'int'],
        expected='bigint',
    ),
    Case(
        input_types=['int', 'bigint', 'int', 'float'],
        expected='float',
    ),
    Case(
        input_types=['double', 'bigint', 'int', 'float'],
        expected='double',
    ),
    Case(
        input_types=['double', 'double', 'decimal(10,0)'],
        expected='decimal(10,0)',
    ),
)
def test_get_largest_number_dtype(input_types, expected):
    assert _get_largest_number_dtype(input_types) == expected


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.master('local')
        .appName('cprices_test_context')
        .config('spark.sql.shuffle.partitions', 1)
        .config('spark.ui.showConsoleProgress', 'false')
        .getOrCreate()
    )

    df1 = spark.createDataFrame(
        [(1, 'dog'), (2, 'cat')],
        schema=['id', 'animal'],
    )
    df2 = spark.createDataFrame(
        [('1', 'dog'), ('2', 'cat')],
        schema=['id', 'animal'],
    )
    concat([df1, df2])
