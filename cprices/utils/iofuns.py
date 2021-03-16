"""I/O functions for pipeline interaction with HDFS."""
# import python libraries
import copy
from datetime import datetime
import logging
import os
from typing import Dict, List, Mapping

# import pyspark libraries
from pyspark.sql import DataFrame as sparkDF
from pyspark.sql import SparkSession


LOGGER = logging.getLogger()


def filter_input_data(
    input_data: dict,
    data_source: str,
) -> dict:
    """Loop through dictionary and return specified data source."""
    filtered_data = {i: input_data[i] for i in input_data if i == data_source}

    return filtered_data


def load_web_scraped_data(
    spark: SparkSession,
    web_scraped_data: dict,
    web_scraped_data_columns: List[str],
    web_scraped_input_tables: Mapping[str, str],
) -> Dict[dict, sparkDF]:
    """Load web scraped data for processing as specified in scenario config.

    Parameters
    ----------
    spark
        Spark session.
    web_scraped_data
        Dictionary with all the web scraped suppliers and items.
        is located.
    web_scraped_data_columns
        List of columns to be loaded in for web-scraped data.
    web_scraped_input_tables
        Dictionary to map the supplier+item to a HIVE table path.

    Returns
    -------
    Dict[dict, sparkDF]
        Each path of keys leads to a value/spark dataframe as it was read
        from HDFS for the corresponding table.
    """
    # Create a full copy of the web_scraped_data dictionary
    web_scraped_staged_data = copy.deepcopy(web_scraped_data)

    # webscraped data has 3 levels: data_source, supplier, item
    for supplier in web_scraped_data['web_scraped']:
        for item in web_scraped_data['web_scraped'][supplier]:
            path = web_scraped_input_tables[supplier][item]

            web_scraped_staged_data['web_scraped'][supplier][item] = spark.sql(
                f"SELECT {','.join(web_scraped_data_columns)} FROM {path}"  # noqa E501
            )

    return web_scraped_staged_data


def load_scanner_data(
    spark: SparkSession,
    scanner_data: dict,
    scanner_data_columns: List[str],
    scanner_input_tables: Mapping[str, str],
) -> Dict[dict, sparkDF]:
    """Load data for processing as specified in the scenario config.

    Parameters
    ----------
    spark
        Spark session.
    scanner_data
        Dictionary with all the scanner suppliers and items.
    scanner_data_columns
        List of columns to be loaded in for scanner data.
    scanner_input_tables
        Dictionary to map the supplier to a HIVE table path.

    Returns
    -------
    Dict[dict, sparkDF]
        Each path of keys leads to a value/spark dataframe as it was read
        from HDFS for the corresponding table.
    """
    # Create a full copy of the scanner_data dictionary
    scanner_staged_data = copy.deepcopy(scanner_data)

    # conventional and scanner data have 2 levels: data_source, supplier
    for supplier in scanner_data['scanner']:
        path = scanner_input_tables.get(supplier)

        scanner_staged_data['scanner'][supplier] = spark.sql(
            f"SELECT {','.join(scanner_data_columns)} FROM {path}"
        )

    return scanner_staged_data


def load_conventional_data(
    spark: SparkSession,
    conventional_data: dict,
    staged_dir: str,
    conventional_data_columns: List[str],
) -> Dict[dict, sparkDF]:
    """Load data for processing as specified in the scenario config.

    Parameters
    ----------
    spark
        Spark session.
    conventional_data
        Dictionary with all the conventional suppliers and items.
    staged_dir
        The path to the HDFS directory from where the conventional data
        is located.
    conventional_data_columns
        List of columns to be loaded in for conventional data.

    Returns
    -------
    Dict[dict, sparkDF]
        Each path of keys leads to a value/spark dataframe as it was read
        from HDFS for the corresponding table.
    """
    # Create a full copy of the conventional_data dictionary
    conventional_staged_data = copy.deepcopy(conventional_data)

    # Currently only single supplier (local_collection) and file
    # (historic) available for conventional data
    path = os.path.join(
        staged_dir,
        'conventional',
        'local_collection',
        'historic_201701_202001.parquet',
    )

    conventional_staged_data['conventional'] = (
        spark.read.parquet(path)
        .select(conventional_data_columns)
    )

    return conventional_staged_data


def save_output_hdfs(
    dfs: Dict[str, sparkDF],
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
