"""Spark utility functions."""
from typing import Any, Union

from pyspark.sql import (
    Column as SparkCol,
    functions as F,
)
from py4j.protocol import Py4JError


def convert_to_spark_col(s: Any) -> Union[Any, SparkCol]:
    """Convert strings to Spark Columns, otherwise returns input."""
    try:
        return F.col(s)
    except (AttributeError, Py4JError):
        return s
