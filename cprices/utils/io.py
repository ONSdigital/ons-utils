"""Useful I/O functions for interacting with HDFS and Hive tables.

Includes:
* read_hive_table - Python friendly Hive table reader
"""
from typing import Optional, Sequence
from pathlib import Path

from pyspark.sql import (
    DataFrame as SparkDF,
    SparkSession,
)

from cprices.config import DevConfig

dev_config = DevConfig('dev_config')


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


def read_output(spark: SparkSession, run_id: str, output: str) -> SparkDF:
    """Read the given output for the given run_id."""
    path = Path(dev_config.processed_dir).joinpath(run_id, output)
    return spark.read.parquet(path)
