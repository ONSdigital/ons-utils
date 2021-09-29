"""Spark utility functions."""
from typing import Any, Union, Sequence, Tuple, Optional

from pyspark.sql import (
    Column as SparkCol,
    DataFrame as SparkDF,
    functions as F,
)
from py4j.protocol import Py4JError


def convert_to_spark_col(s: Any) -> Union[Any, SparkCol]:
    """Convert strings to Spark Columns, otherwise returns input."""
    try:
        return F.col(s)
    except (AttributeError, Py4JError):
        return s


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
    ddl_schema = '\n'.join([f'`{name}` {dtype},' for name, dtype in fields])
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
