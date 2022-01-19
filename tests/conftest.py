"""Plugin that creates a spark session fixture to be used by all tests."""
import logging
import os
from pathlib import Path
from typing import Optional, Any, Mapping
import sys
import warnings

import pandas as pd
from _pytest.mark.structures import MarkDecorator
import pytest
from pyspark.sql import SparkSession

from ons_utils.testing import (
    create_dataframe,
    create_df_with_multi_indices,
    create_multi_column_df,
    to_date,
)


def suppress_py4j_logging():
    """Suppress spark logging."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session():
    """Set up spark session fixture."""
    suppress_py4j_logging()

    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_context")
        .config('spark.ui.showConsoleProgress', 'false')
        # Compatibility for PyArrow with Spark 2.4-legacy IPC format.
        .config('spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT', '1')
        .getOrCreate()
    )


@pytest.fixture
def create_spark_df(spark_session):
    """Create Spark DataFrame from tuple data with first row as schema."""
    def _(data):
        return spark_session.createDataFrame(data[1:], schema=data[0])
    return _


@pytest.fixture(scope='class')
def to_spark(spark_session):
    """Convert pandas df to spark."""
    def _(df: pd.DataFrame, *args, **kwargs):
        return spark_session.createDataFrame(df, *args, **kwargs)
    return _


@pytest.fixture
def spark_column():
    """Convert a list of values to a single spark column.

    Can be used as a workaround for Null and NaNs in numeric column.
    """
    def _(vals):
        return [(v,) for v in vals]
    return _


class Case:
    """Container for a test case, with optional test ID.

    Attributes
    ----------
        label : str
            Optional test ID. Will be displayed for each test when
            running `pytest -v`.
        kwargs: Parameters used for the test cases.

    Examples
    --------
    >>> Case(label="some test name", foo=10, bar="some value")
    >>> Case(foo=99, bar="some other value")   # no name given

    See Also
    --------
    source: https://github.com/ckp95/pytest-parametrize-cases

    """

    def __init__(
        self,
        label: Optional[str] = None,
        marks: Optional[MarkDecorator] = None,
        **kwargs,
    ):
        """Initialise objects."""
        self.label = label
        self.kwargs = kwargs
        self.marks = marks
        # Makes kwargs accessible with dot notation.
        self.__dict__.update(kwargs)

    def __repr__(self) -> str:
        """Return string."""
        return f"Case({self.label!r}, **{self.kwargs!r})"


def parametrize_cases(*cases: Case, defaults: Mapping[str, Any] = None):
    """More user friendly parameterize cases testing.

    See: https://github.com/ckp95/pytest-parametrize-cases
    """
    if not defaults:
        defaults = {}

    all_args = set()
    for case in cases:
        if not isinstance(case, Case):
            raise TypeError(f"{case!r} is not an instance of Case")

        all_args.update(case.kwargs.keys())

    argument_string = ",".join(sorted(all_args))

    case_list = []
    ids_list = []
    for case in cases:
        case_kwargs = case.kwargs.copy()
        args = case.kwargs.keys()

        # Make sure all keys are in each case, otherwise initialise with None.
        diff = {k: defaults.get(k, None) for k in set(all_args) - set(args)}
        case_kwargs.update(diff)

        # The case_kwargs dict needs to be sorted by the keys to match
        # the order of the argument string.
        case_tuple = tuple(value for _, value in sorted(case_kwargs.items()))

        # If marks are given, wrap the case tuple.
        if case.marks:
            case_tuple = pytest.param(*case_tuple, marks=case.marks)

        case_list.append(case_tuple)
        ids_list.append(case.label)

    if len(all_args) == 1:
        # otherwise it gets passed to the test function as a singleton tuple
        case_list = [i[0] for i in case_list]

    return pytest.mark.parametrize(
        argnames=argument_string, argvalues=case_list, ids=ids_list
    )


@pytest.fixture
def all_in_output():
    """Checks that all values are in the output."""
    def _(output, values):
        return all([x in values for x in output])
    return _


@pytest.fixture(
    params=[
        lambda s: s,
        lambda s: Path(s),
    ],
    ids=['str', 'pathlib.Path'],
)
def make_path_like(request):
    """From a string, return two acceptable file path types."""
    def _(filepath: str):
        path_func = request.param
        return path_func(filepath)
    return _


@pytest.fixture(scope='class')
def filename_to_pandas():
    """Convert dataset at test data filename to pandas DataFrame."""
    def _(filename: str, dir: str, *args, **kwargs):
        path = Path(dir).joinpath(filename)
        return pd.read_csv(path, *args, **kwargs)
    return _


@pytest.fixture(scope='class')
def filename_to_spark(to_spark):
    """Convert dataset at test data filename to Spark DataFrame."""
    def _(filename: str, dir: str, *args, **kwargs):
        path = Path(dir).joinpath(filename)
        return to_spark(pd.read_csv(path, *args, **kwargs))
    return _


@pytest.fixture
def suppress_warnings():
    """Stops warnings from being printed out."""
    warnings.filterwarnings('ignore')
