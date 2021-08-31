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


def get_recent_run_ids(
    n: int = 20,
    username: Optional[str] = None,
) -> pd.Series:
    """Return the recent cprices run IDs.

    Parameters
    ----------
    username : str, default None
        Filter for recent runs for a CDSW username.
    n : int, default 20
        The number of recent cprices run IDs to return.

    Returns
    -------
    pandas Series
        The ``n`` recent run IDs, filtered for ``username`` if given.
    """
    dirs = hdfs.read_dir(dev_config.processed_dir)
    # Date and time are at the 5 and 6 indices respectively.
    datetimes = [d[5] + ' ' + d[6] for d in dirs]
    # The full file path is in the last index position.
    run_ids = [Path(d[-1]).stem for d in dirs]

    run_ids = (
        pd.Series(run_ids, index=pd.to_datetime(datetimes), name='run_id')
        .sort_index(ascending=False)
    )

    if username:
        run_ids = run_ids.loc[run_ids.str.contains(username)]

    return run_ids.head(n)
