"""Useful I/O functions for interacting with HDFS and Hive tables.

Includes:
* read_hive_table - Python friendly Hive table reader
* read_output - Reads the given output dataframe for a given run_id.
* get_recent_run_ids - Returns the last 20 run_ids for the pipeline.
"""
from typing import Optional, Sequence
from pathlib import Path

import pandas as pd
from pyspark.sql import (
    DataFrame as SparkDF,
    SparkSession,
)
from epds_utils import hdfs

from cprices.config import DevConfig

dev_config = DevConfig('dev_config', to_unpack=['directories'])


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
    """Read the given output for the given run_id.

    Parameters
    ----------
    run_id : str
        The unique identifying string for the run, of the form current
        date, time and Hadoop username (YYYYMMDD_HHMMSS_username).
    output : str
        The name of the output from the following selection:
        {'item_indices', 'low_level_indices', 'classified',
        'inliers_outliers', 'expenditure', 'filtered', 'configuration'}

    """
    path = (
        Path(dev_config.processed_dir)
        .joinpath(run_id, output)
        .as_posix()
    )
    return spark.read.parquet(path)


def get_recent_run_ids() -> pd.Series:
    """Return the last 20 cprices run IDs."""
    dirs = hdfs.read_dir(dev_config.processed_dir)
    # Date and time are at the 5 and 6 indices respectively.
    datetimes = [d[5] + ' ' + d[6] for d in dirs]
    # The full file path is in the last index position.
    run_ids = [Path(d[-1]).stem for d in dirs]

    return (
        pd.DataFrame({'time': pd.to_datetime(datetimes), 'run_id': run_ids})
        .sort_values('time', ascending=False)
        .run_id.head(20)
    )
