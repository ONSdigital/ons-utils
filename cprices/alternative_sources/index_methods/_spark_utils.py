"""Spark utility functions."""
import functools
from typing import (
    Any,
    Callable,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pandas as pd
from pyspark.sql import (
    Column as SparkCol,
    DataFrame as SparkDF,
    functions as F,
)
from pyspark.sql.types import StructType
from pyspark.sql.functions import pandas_udf, PandasUDFType


def convert_to_spark_col(s: Union[str, SparkCol]) -> SparkCol:
    """Convert strings to Spark Columns, otherwise returns input."""
    if isinstance(s, str):
        return F.col(s)
    elif isinstance(s, SparkCol):
        return s
    else:
        raise ValueError(
            "expecting a string or pyspark column but received obj"
            f" of type {type(s)}"
        )


def convert_to_pandas_udf(
    func: Callable[..., pd.DataFrame],
    schema: Union[StructType, str],
    groups: Sequence[str],
    keep_index: bool = False,
    args: Optional[Sequence[Any]] = None,
    kwargs: Optional[Mapping[str, Any]] = None,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Convert the given function to a pyspark pandas_udf.

    Parameters
    ----------
    func : callable
        The function to convert to a pandas_udf. Must take a pandas
        dataframe as it's first argument.
    schema : StructType or str
        The schema for the output of the pandas_udf as either a
        StructType or a str schema in the DDL format i.e. ("col1 string,
        col2 integer, col3 timestamp").
    groups : sequence of str
        The column keys that define the groupings in the data. Needed
        here so it is available in the scope of the pandas_udf which can
        only accept a single pandas dataframe as an input.
    keep_index : bool, default False
        Set to True if the output from ``func`` returns a pandas
        dataframe with columns in the index that should be kept. Keep
        False if ``func`` already returns a pandas dataframe with no
        values in the index.
    args : sequence, optional
        The positional arguments to be unpacked into ``func``.
    kwargs : mapping, optional
        The keyword arguments to be unpacked into ``func``.

    Returns
    -------
    Callable
        A GROUPED_MAP pandas_udf that accepts a single pandas dataframe
        and returns a pandas dataframe.
    """
    args = [] if not args else args
    kwargs = {} if not kwargs else kwargs

    # As pandas_udfs with function type GROUPED_MAP must take either one
    # argument (data) or two arguments (key, data), it needs to be
    # defined as a nested function so that it has access to the
    # parameters in the enclosing scope.
    @functools.wraps(func)
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def wrapped_udf(df):
        result = func(df, *args, **kwargs)

        # Reset the index before returning.
        if keep_index:
            result = result.reset_index()

        # Because df is a single group we cast the first value for each
        # group key across the whole column in the output.
        groups_df = (
            pd.DataFrame(index=result.index, columns=groups)
            .fillna(df.loc[0, groups])
        )

        return pd.concat([groups_df, result], axis=1)

    return wrapped_udf


def get_ddl_schema(fields: Sequence[Tuple[str, str]]) -> str:
    """Get the ddl style schema from (name, dtype) fields.

    Parameters
    ----------
    dtypes : sequence of tuple
        List of (name, dtype) tuples, similar to the output of
        pyspark.sql.DataFrame.dtypes.

    Returns
    -------
    str
        The ddl schema.
    """
    ddl_schema = '\n'.join([f'{name} {dtype},' for name, dtype in fields])
    # Remove the last comma.
    return ddl_schema[:-1]


def get_fields(
    df: SparkDF,
    selection: Optional[Sequence[str]] = None,
) -> Sequence[Tuple[str, str]]:
    """Get the (name, dtype) fields for the dataframe.

    Parameters
    ----------
    selection : sequence of str, optional
        The selection of columns to return fields for.

    Returns
    -------
    sequence of tuple
        In the format (name, dtype) for each selected column.
    """
    fields = dict(df.dtypes)

    if selection:
        return [(col, fields.get(col)) for col in selection]
    else:
        return fields
