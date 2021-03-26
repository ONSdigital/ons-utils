"""I/O functions for pipeline interaction with HDFS."""
# import python libraries
from datetime import datetime
from functools import reduce
import logging
import os
from typing import Dict, List, Mapping

# import pyspark libraries
from pyspark.sql import DataFrame as SparkDF
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


LOGGER = logging.getLogger()


def load_web_scraped_data(
    spark: SparkSession,
    config_data: Mapping[str, Mapping[str, Mapping[str, str]]],
    filtered_columns: List[str],
    config_table_path: Mapping[str, Mapping[str, str]],
) -> SparkDF:
    """Load web scraped data for processing as specified in scenario config.

    Parameters
    ----------
    spark
        Spark session.
    config_data
        Nested mapping of suppliers, items, and retailer weights.
    filtered_columns
        Columns to load from Hive table.
    config_table_path
        Nested mapping of path to supplier and item Hive table.

    Returns
    -------
    SparkDF
        Unionised web scraped data across all supplier and item combinations.
    """
    supplier_item_dfs = []

    filtered_data = {
        i: config_data[i] for i in config_data if i == 'web_scraped'
    }

    # Multiple Hive tables contain the web scraped data for the varying
    # supplier and item combinations. The paths for these tables are
    # specified in the dev config file.
    for supplier in filtered_data['web_scraped']:

        # Loop through all web scrapped supplier and item combinations.
        # Append data from Hive tables to a list then combine.
        for item in filtered_data['web_scraped'][supplier]:

            path = config_table_path[supplier][item]

            # Join columns to comma-separated string for the SQL query.
            variable = "','.join(filtered_columns)"
            staged_data = spark.sql(
                f"SELECT {eval(variable)} FROM {path}"  # noqa E501
            )

            staged_data = (
                staged_data
                .withColumn('supplier', F.lit(supplier))
                .withColumn('item', F.lit(item))
            )

            supplier_item_dfs.append(staged_data)

    # Use Spark DataFrame column names to union rows.
    web_scraped_data = reduce(SparkDF.unionByName, supplier_item_dfs)

    return web_scraped_data


def load_scanner_data(
    spark: SparkSession,
    config_data: Mapping[str, str],
    filtered_columns: List[str],
    config_table_path: Mapping[str, str],
) -> SparkDF:
    """Load scanner data for processing as specified in scenario config.

    Parameters
    ----------
    spark
        Spark session.
    config_data
        Nested mapping of retailer weights.
    filtered_columns
        Columns to load from Hive table.
    config_table_path
        Nested mapping of path to retailer Hive table.

    Returns
    -------
    SparkDF
        Unionised scanner data across all retailer combinations.
    """
    retailer_dfs = []

    filtered_data = {
        i: config_data[i] for i in config_data if i == 'scanner'
    }

    # Multiple Hive tables contain the scanner data for varying retailers.
    # The paths for these tables are specified in the dev config file.
    for retailer in filtered_data['scanner']:

        # Loop through all retailers for scanner data.
        # Append data from Hive tables to a list then combine.
        path = config_table_path.get(retailer)

        # Join columns to comma-separated string for the SQL query.
        variable = "','.join(filtered_columns)"
        staged_data = spark.sql(
            f"SELECT {eval(variable)} FROM {path}"  # noqa E501
        )

        staged_data = staged_data.withColumn('retailer', F.lit(retailer))

        retailer_dfs.append(staged_data)

    # Use Spark DataFrame column names to union rows.
    scanner_data = reduce(SparkDF.unionByName, retailer_dfs)

    return scanner_data


def load_conventional_data(
    spark: SparkSession,
    filtered_columns: List[str],
    config_dir_path: str,
) -> SparkDF:
    """Load conventional data for processing as specified in scenario config.

    Parameters
    ----------
    spark
        Spark session.
    filtered_columns
        Columns to load from Hive table.
    config_dir_path
        The path to the HDFS directory from where the conventional data
        is located.

    Returns
    -------
    SparkDF
        The conventional data as it was read from HDFS.
    """
    # Currently only single supplier (local_collection) and file
    # (historic) available for conventional data.
    path = os.path.join(
        config_dir_path,
        'conventional',
        'local_collection',
        'historic_201701_202001.parquet',
    )

    staged_data = (
        spark.read.parquet(path)
        .select(filtered_columns)
    )

    return staged_data


def save_output_hdfs(
    dfs: Dict[str, SparkDF],
    processed_dir: str,
) -> str:
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
