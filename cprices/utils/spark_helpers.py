"""A selection of helper functions for building in pyspark."""
import functools
import itertools
from typing import (
    Any,
    Callable,
    List,
    Mapping,
    Sequence,
    Union,
)

from py4j.protocol import Py4JError
from pyspark.sql import (
    Column as SparkCol,
    DataFrame as SparkDF,
    functions as F,
    Window,
    WindowSpec,
)

from .spark_concat import concat    # noqa: F401


def to_spark_col(_func=None, *, exclude: Sequence[str] = None) -> Callable:
    """Convert str args to Spark Column if not already."""
    if not exclude:
        exclude = []

    def caller(func: Callable[[Union[str, SparkCol]], SparkCol]):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            varnames = func.__code__.co_varnames
            if args:
                args = [
                    _convert_to_spark_col(arg)
                    if varnames[i] not in exclude
                    else arg
                    for i, arg in enumerate(args)
                ]
            if kwargs:
                kwargs = {
                    k: _convert_to_spark_col(kwarg)
                    if k not in exclude
                    else kwarg
                    for k, kwarg in kwargs.items()
                }
            return func(*args, **kwargs)
        return wrapper

    if _func is None:
        return caller
    else:
        return caller(_func)


def _convert_to_spark_col(s: Any) -> Union[Any, SparkCol]:
    """Convert strings to Spark Columns, otherwise returns input."""
    try:
        return F.col(s)
    except (AttributeError, Py4JError):
        return s


def map_col(col_name: str, mapping: Mapping[Any, Any]) -> SparkCol:
    """Map PySpark column using Python mapping."""
    map_expr = F.create_map([
        F.lit(x)
        if not is_list_or_tuple(x)
        # To handle when the value is a list or tuple.
        else F.array([F.lit(i) for i in x])
        # Convert mapping to list.
        for x in itertools.chain(*mapping.items())
    ])
    return map_expr[F.col(col_name)]


def is_list_or_tuple(x):
    """Return True if list or tuple."""
    return isinstance(x, tuple) or isinstance(x, list)


def get_window_spec(levels: Sequence[str] = None) -> WindowSpec:
    """Return WindowSpec partitioned by levels, defaulting to whole df."""
    if not levels:
        return whole_frame_window()
    else:
        return Window.partitionBy(levels)


def whole_frame_window() -> WindowSpec:
    """Return WindowSpec for whole DataFrame."""
    return Window.rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing,
    )


def to_list(df: SparkDF) -> List[Union[Any, List[Any]]]:
    """Convert Spark DF to a list.

    Returns
    -------
    list or list of lists
        If the input DataFrame has a single column then a list of column
        values will be returned. If the DataFrame has multiple columns
        then a list of row data as lists will be returned.
    """
    if len(df.columns) == 1:
        return df.toPandas().squeeze().tolist()
    else:
        return df.toPandas().values.tolist()


def map_column_names(df: SparkDF, mapper: Mapping[str, str]) -> SparkDF:
    """Map column names to the given values in the mapper.

    If the column name is not in the mapper the name doesn't change.
    """
    cols = [
        F.col(col_name).alias(mapper.get(col_name, col_name))
        for col_name in df.columns
    ]
    return df.select(*cols)


def get_hive_table_columns(spark, table_path) -> List[str]:
    """Return the column names for the given Hive table."""
    return to_list(spark.sql(f'SHOW columns in {table_path}'))


def transform(self, f, *args, **kwargs):
    """Chain Pyspark function."""
    return f(self, *args, **kwargs)


def get_first_group(df: SparkDF, groups: Sequence[str]) -> SparkDF:
    """Return a sample pyspark dataframe filtered to the first group."""
    row = df.select(groups).head(1)[0]
    query = " AND ".join([f"{group}=='{row[group]}'" for group in groups])
    return df.filter(query)
