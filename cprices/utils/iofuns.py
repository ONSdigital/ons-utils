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
import logging
import os
from typing import Any, List, Mapping

# Import PySpark libraries.
from pyspark.sql import (
    DataFrame as SparkDF,
)


LOGGER = logging.getLogger()


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


def remove_scenario(
    conf: Mapping[str, Any],
    col_name: str,
) -> Mapping[str, Any]:
    """Delete an entry from loaded config file using user defined string."""
    conf['preprocess_cols']['scanner'] = _remove_col(
        conf['preprocess_cols']['scanner'],
        col_name,
    )
    conf['preprocess_cols']['web_scraped'] = _remove_col(
        conf['preprocess_cols']['web_scraped'],
        col_name,
    )
    conf['groupby_cols'] = _remove_col(conf['groupby_cols'], col_name)

    return conf


def _remove_col(list_of_cols: List[str], to_remove: str) -> List[str]:
    """Remove item from list if starts with user defined str parameter."""
    return [item for item in list_of_cols if not item.startswith(to_remove)]
