"""Load and save functions for pipeline interaction with HDFS and HUE.

* Webscraped and scanner data is read in from Hive tables.
* Conventional data is read in from a parquet file in the staged data
  directory in HDFS.
* Outputs are saved in a sub-directory of the processed data directory
  in HDFS, named after the run_id which is a combination of current
  datetime and name of the user running the pipeline.

  - Analysis outputs are saved as CSVs.
  - All other outputs are saved as parquets.

"""
# Import Python libraries.
import logging
from pathlib import Path
from typing import Mapping,  Optional, Sequence

# Import PySpark libraries.
from epds_utils.hdfs import copy_local_to_hdfs
from pyspark.sql import (
    DataFrame as SparkDF,
    SparkSession,
)

LOGGER = logging.getLogger()


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


def save_output_hdfs(dfs: Mapping[str, SparkDF], output_dir: Path) -> str:
    """Store output dataframes (combined across all scenarios) in HDFS.

    Parameters
    ----------
    dfs : mapping of str to SparkDF
        The output dataframes from all scenarios to store in HDFS.
    output_dir : Path
        The path to the HDFS output directory where the dfs will be
        stored.

    Notes
    -----
    Run_id is the name of the folder that will be created inside the processed
    data folder in HDFS for this particular run and will contain all the
    output dataframes. The run_id is printed on the screen for the user to
    explore the output data.

    The configuration dataset is a two-column table where the first column
    shows the stage of the core pipeline and the second column shows (as a
    dictionary) all the config parameters for the corresponding stage. This
    can be used as a reference for the user in case they want to check the
    configuration of this run.
    """
    for name in dfs:
        LOGGER.info(f'{name}...')
        if name in ['analysis']:
            # store analysis output as csv
            path = output_dir.joinpath('analysis')
            dfs[name].repartition(1).write.csv(
                path,
                header=True,
                mode='overwrite'
            )

        else:
            path = output_dir.joinpath(name)
            dfs[name].write.parquet(path)


def copy_logs_to_hdfs(local_path: str, hdfs_path: str) -> None:
    """Copy the logs to HDFS."""
    copy_local_to_hdfs(local_path, hdfs_path)
