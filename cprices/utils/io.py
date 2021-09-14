"""Useful I/O functions for interacting with HDFS and Hive tables.

Includes:
* read_hive_table - Python friendly Hive table reader
* write_hive_table - Python friendly Hive table writer
* read_output - Reads the given output dataframe for a given run_id.
* get_recent_run_ids - Returns the last 20 run_ids for the pipeline.
"""
import os
from pathlib import Path
from typing import Callable, Optional, Sequence

import pandas as pd
from pyspark.sql import (
    DataFrame as SparkDF,
    functions as F,
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


def write_hive_table(
    spark: SparkSession,
    df: SparkDF,
    table_name: str,
    mode: str = 'overwrite',
    logger: Optional[Callable[[str], None]] = print,
 ) -> None:
    """
    Create a Hive table from a spark dataframe.

    Will also add two columns:

        * 'append_timestamp' - the timestamp for when the row was added
        * 'append_user' - the username of the person who added the row

    Note, to delete a table entirely in the Hive editor in HUE run the
    following SQL query (note this has no warning before executing)

        DROP TABLE <database name>.<table name>

    Parameters
    ----------
    spark
        Active spark session.
    df : SparkDF
        The spark dataframe to be saved ot Hive table.
    table_name : str
        The name to be used for the table, should be of form
            <database name>.<table name>
    mode : str, optional {'overwrite', 'append'}
        Whether to overwrite or append to the Hive table.
    logger : callable, default print
        Optional logger that accepts a string argument
    """
    logger(f'Will {mode.upper()} data to {table_name}')

    (
        df
        .withColumn('append_timestamp', F.current_timestamp())
        .withColumn('append_user', F.lit(os.environ['HADOOP_USER_NAME']))
        .write
        .saveAsTable(
            table_name,
            format='hive',
            mode=mode
        )
    )


def read_output(
    spark: SparkSession,
    run_id: str,
    output: str
) -> SparkDF:
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
