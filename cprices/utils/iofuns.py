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
from datetime import datetime
from functools import reduce
import logging
import os
import re
from typing import Mapping, Tuple, Optional, Sequence

# Import PySpark libraries.
from pyspark.sql import (
    DataFrame as SparkDF,
    functions as F,
    SparkSession,
)


LOGGER = logging.getLogger()


def load_web_scraped_data(
    spark: SparkSession,
    # NOTE: change type hint when weights removed from scenario file.
    selected_scenario: Mapping[Tuple[str, str, str], float],
    columns: Sequence[str],
    table_paths: Mapping[str, Mapping[str, str]],
) -> SparkDF:
    """Load webscraped data as specified in scenario config.

    Returns a single DataFrame with additional columns to identify the
    chosen suppliers and items. The suppliers and items chosen in the
    scenario file are used as the lookup keys for the Hive Tables,
    listed under "webscraped_input_tables" in the dev config file.

    Parameters
    ----------
    selected_scenario
        Mapping of (supplier, item, retailer) -> weight. From
        "input_data" in scenario file.

        The weights are unused and will soon be implemented differently
        and removed from the scenario file. Only the supplier and item
        keys are used by the function.
    columns
        Columns to load from Hive table.
    table_paths
        Nested mapping of path -> supplier -> Hive table path. Table
        paths are in the the format "database_name.table_name".

    Returns
    -------
    SparkDF
        Selected webscraped data with differentiating supplier and item
        columns.
    """
    dfs = []

    for supplier, item, _ in selected_scenario:
        # Grab the table path as specified by the user scenario.
        table_path = table_paths[supplier][item]
        df = read_hive_table(spark, table_path, columns)

        # Add columns to retain data origin after union step.
        df = (
            df
            .withColumn('supplier', F.lit(supplier))
            .withColumn('item', F.lit(item))
            .withColumn('data_source', F.lit('web_scraped'))
        )

        dfs.append(df)

    # DataFrames should have the same schema so union all in the list.
    # Because of the current setup, where we're using the weights
    # unnecessarily, it reads in duplicates of the tables since supplier
    # and item are duplicated for each retailer.
    # TODO: remove dropDuplicates() when weights are changed.

    return reduce(SparkDF.union, dfs).dropDuplicates()


def load_scanner_data(
    spark: SparkSession,
    # NOTE: change type hint when weights removed from scenario file.
    selected_scenario: Mapping[str, float],
    columns_to_load: Sequence[str],
    table_paths: Mapping[str, str],
) -> SparkDF:
    """Load scanner data as specified in scenario config.

    Returns a single DataFrame with an additional column to identify the
    chosen retailers. The retailers chosen in the scenario file are used
    as the lookup keys for the Hive Tables, listed under
    "scanner_input_tables" in the dev config file.

    Parameters
    ----------
    selected_scenario
        Mapping of retailer -> weight. From "input_data" in scenario
        file.

        The weights are unused and will soon be implemented differently
        and removed from the scenario file. Only the retailer key is
        used by the function.
    columns_to_load
        Columns to load from Hive table.
    table_paths
        Mapping of retailer -> Hive table path. Table paths are in the
        the format "database_name.table_name".

    Returns
    -------
    SparkDF
        Selected scanner data with differentiating retailer column.
    """
    dfs = []

    for retailer in selected_scenario:

        # Grab the table path as specified by the user scenario.
        table_path = table_paths[retailer]

        # As scanner retailers have a variable number of hierarchy level cols
        # we get the names from the table and use this for loading the data.
        table_columns = spark.sql(f"SELECT * FROM {table_path}").columns

        hierarchy_columns = [
            col for col in table_columns
            if re.match(r'(hierarchy_level_)\d(_code)', col)
        ]

        # Combine list of hierarchy columns to the predefined cols for reading
        read_columns = columns_to_load + hierarchy_columns

        df = read_hive_table(spark, table_path, read_columns)

        # Add columns to retain data origin after union step.
        df = (
            df
            .withColumn('retailer', F.lit(retailer))
            .withColumn('data_source', F.lit('scanner'))
        )

        dfs.append(df)

    # DataFrames should have the same schema so union all in the list.

    return reduce(SparkDF.union, dfs)


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


def load_conventional_data(
    spark: SparkSession,
    columns: Sequence[str],
    dir_path: str,
) -> SparkDF:
    """Load conventional price collection data.

    Parameters
    ----------
    columns
        Columns to load from Hive table.
    dir_path
        Path to the staged data directory on HDFS.

    """
    # Currently only single supplier (local_collection) and file
    # (historic) available for conventional data.
    path = os.path.join(
        dir_path,
        'conventional',
        'local_collection',
        'historic_201701_202001.parquet',
    )

    return spark.read.parquet(path).select(columns)


def save_output_hdfs(dfs: Mapping[str, SparkDF], processed_dir: str) -> str:
    """Store output dataframes (combined across all scenarios) in HDFS.

    Parameters
    ----------
    dfs
        The output dataframes from all scenarios to store in HDFS.
    processed_dir
        It has the path to the HDFS directory where the dfs will be stored.

    Returns
    -------
    str
        The unique identifying string for the run, of the form
        current date, time and username (YYYYMMDD_HHMMSS_username).

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
    # create run id using username and current time
    username = os.environ['HADOOP_USER_NAME']
    current_date_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = '_'.join([current_date_time, username])

    # create directory path to export processed data
    processed_dir = os.path.join(processed_dir, run_id)

    for name in dfs:
        LOGGER.info(f'{name}...')
        if name in ['analysis']:
            # store analysis output as csv
            path = os.path.join(processed_dir, 'analysis')
            dfs[name].repartition(1).write.csv(
                path,
                header=True,
                mode='overwrite'
            )

        else:
            path = os.path.join(processed_dir, name)
            dfs[name].write.parquet(path)

    return run_id
