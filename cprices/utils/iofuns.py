"""Useful I/O functions for interacting with HDFS and Hive tables.

Includes:
* read_hive_table - Python friendly Hive table reader
"""
from typing import Optional, Sequence

from pyspark.sql import (
    DataFrame as SparkDF,
    SparkSession,
)


def read_hive_table(
    spark: SparkSession,
    table_path: str,
    columns: Optional[Sequence[str]] = None,
) -> SparkDF:
    """Read Hive table given table path and column selection.

    Parameters
    ----------
    table_path : str
        Hive table path in format "database_name.table_name".
    columns : list of str, optional
        The column selection. Selects all columns if None passed.
    """
    # Join columns to comma-separated string for the SQL query.
    selection = ','.join(columns) if columns else '*'
    return spark.sql(f"SELECT {selection} FROM {table_path}")
