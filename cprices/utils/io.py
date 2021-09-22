"""Useful I/O functions for interacting with HDFS and Hive tables.

Includes:
* read_hive_table - Python friendly Hive table reader
* write_hive_table - Python friendly Hive table writer
* read_output - Reads the given output dataframe for a given run_id.
* get_recent_run_ids - Returns the last 20 run_ids for the pipeline.
"""
import os
from pathlib import Path
import textwrap
from typing import Callable, Dict, Optional, Sequence

import pandas as pd
from pyspark.sql import (
    DataFrame as SparkDF,
    functions as F,
    SparkSession,
)
from epds_utils import hdfs

from cprices.utils.pipeline_utils import pretty_wrap


def read_hive_table(
    spark: SparkSession,
    table_path: str,
    columns: Optional[Sequence[str]] = None,
    date_column: Optional[str] = None,
    date_range: Optional[Sequence[str]] = None,
    column_filter_dict: Optional[Dict[str, Sequence[str]]] = dict(),
) -> SparkDF:
    """Read Hive table given table path and column selection.

    Parameters
    ----------
    spark
        Spark session.
    table_path : str
        Hive table path in format "database_name.table_name".
    columns : list of str, optional
        The column selection. Selects all columns if None passed.
    date_column : Optional, str
        The name of the column to be used to filter the date range on.
    date_range : Optional, sequence (lower bound date, upper bound date)
        Sequence with two values, a lower and upper value for dates to load in.
    column_filter_dict : Optional, dict
        A dictionary containing column: [values] where the values correspond to
        terms in the column that are to be filtered by.

    Returns
    -------
    SparkDF
    """
    return spark.sql(
        build_sql_query(
            table_path=table_path,
            columns=columns,
            date_column=date_column,
            date_range=date_range,
            column_filter_dict=column_filter_dict
        )
    )


def build_sql_query(
    table_path: str,
    columns: Optional[Sequence[str]] = None,
    date_column: Optional[str] = None,
    date_range: Optional[Sequence[str]] = None,
    column_filter_dict: Optional[Dict[str, Sequence[str]]] = None,
) -> str:
    """
    Create the SQL query to load the data with the specified filter conditions.

    Parameters
    ----------
    spark
        Spark session.
    table_path : str
        Hive table path in format "database_name.table_name".
    columns : list of str, optional
        The column selection. Selects all columns if None passed.
    date_column : Optional, str
        The name of the column to be used to filter the date range on.
    date_range : Optional, sequence (lower bound date, upper bound date)
        Sequence with two values, a lower and upper value for dates to load in.
    column_filter_dict : Optional, dict
        A dictionary containing column: [values] where the values correspond to
        terms in the column that are to be filtered by.

    Returns
    -------
    str
        The string containing the SQL query.
    """
    # Create empty list to store all parts of query - combined at end.
    sql_query = []

    # Flag to check whether or not to use a WHERE or AND statement as only one
    # instance of WHERE is allowed in a query.
    first_filter_applied = False

    # Join columns to comma-separated string for the SQL query.
    selection = ', '.join(columns) if columns else '*'

    sql_query.append(f"SELECT {selection} FROM {table_path}")

    if date_column and date_range:
        sql_query.append("WHERE (")
        sql_query.append(f"{date_column} >= '{date_range[0]}'")
        sql_query.append(f"AND {date_column} < '{date_range[1]}'")
        sql_query.append(")")

        first_filter_applied = True

    # Add any column-value specific filters onto the query. Addtional queries
    # are of the form:
    # AND (column_A = 'value1' OR column_A = 'value2' OR ...)
    if column_filter_dict:
        for column in column_filter_dict.keys():
            # First query for column is different to subsequent ones. If date
            # has been filtered we use AND, if not we use WHERE for the first
            # instance.
            if first_filter_applied:
                sql_query.append(f"""
                    AND (\n{column} = '{column_filter_dict[column][0]}'
                """)
            else:
                sql_query.append(f"""
                    WHERE (\n{column} = '{column_filter_dict[column][0]}'
                """)

                first_filter_applied = True

            # Subsequent queries on column are the same form but use OR.
            if len(column_filter_dict[column]) > 1:
                for item in column_filter_dict[column][1:]:
                    sql_query.append(f"OR {column} = '{item}'\n")

            # close off the column filter query
            sql_query.append(')\n')

    # Join entries in list into one nicely formatted string for easier unit
    # testing. Use textwrap.dedent to remove leading whitespace from multiline
    # strings.
    return '\n'.join([textwrap.dedent(line.strip()) for line in sql_query])


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
    output: str,
    directory: Optional[str] = None,
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
    directory : str, optional
        Path to the HDFS processed directory. If not provided, will look
        for the CPRICES_PROCESSED_DIR env variable set during pipeline
        run.
    """
    if not directory:
        directory = get_directory_from_env_var('CPRICES_PROCESSED_DIR')

    path = Path(directory).joinpath(run_id, output).as_posix()
    return spark.read.parquet(path)


def get_recent_run_ids(
    n: int = 20,
    username: Optional[str] = None,
    directory: str = None,
) -> pd.Series:
    """Return the recent cprices run IDs.

    Parameters
    ----------
    username : str, default None
        Filter for recent runs for a CDSW username.
    n : int, default 20
        The number of recent cprices run IDs to return.
    directory : str, optional
        Path to the HDFS processed directory. If not provided, will look
        for the CPRICES_PROCESSED_DIR env variable set during pipeline
        run.

    Returns
    -------
    pandas Series
        The ``n`` recent run IDs, filtered for ``username`` if given.
    """
    if not directory:
        directory = get_directory_from_env_var('CPRICES_PROCESSED_DIR')

    dirs = hdfs.read_dir(directory)
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


def get_directory_from_env_var(env_var: str) -> Optional[str]:
    """Get directory from given environment variable."""
    directory = os.getenv(env_var)

    if not os.getenv(env_var):
        raise DirectoryError(env_var)

    return directory


class DirectoryError(Exception):

    def __init__(self, env_var) -> None:
        super().__init__(pretty_wrap(f"""
            No directory provided and no value available in {env_var}
            environment variable. Pass directory argument or set the env
            var before running again.
        """))
