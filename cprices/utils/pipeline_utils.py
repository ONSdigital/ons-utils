"""Contains pipeline utility functions.

Provides:

* :func:`combine_scenario_df_outputs` - Combines the dataframes for each
  scenario.
* :func:`create_run_id`
* :func:`plot_run_times`
* :func:`timer_args` - Reusable timer args, only requires name
  parameter.
* :func:`get_config_params` - Converts the config parameters into a
  Spark Dataframe.
"""
import datetime
import logging
import os
from typing import Any, Dict, Mapping, Sequence, Union

from humanfriendly import format_timespan
import matplotlib.pyplot as plt
import pandas as pd

from pyspark.sql import (
    DataFrame as SparkDF,
    SparkSession,
)

from cprices.config import Config
from cprices.utils.helpers import invert_nested_keys
from cprices.utils import spark_helpers

LOGGER = logging.getLogger()


def combine_scenario_df_outputs(
    dfs: Mapping[str, Mapping[str, SparkDF]],
) -> Dict[str, SparkDF]:
    """Combine the dataframes for each scenario.

    Parameters
    ----------
    dfs
        Mapping of scenario_name -> df_name -> df

    Returns
    -------
    dict
        Mapping of df_name -> combined_df, where the combined_df is the
        given dataframe for each scenario concatenated together, with a
        new column to distinguish which scenario the data is from.
    """
    # Inverts the dict nesting so scenario_name keys are in the inner
    # dict.
    dfs = invert_nested_keys(dfs)
    # Run concat on the new inner dict, which unions all the frames
    # together with a new column 'scenario' with dict keys as values.
    return {
        k: spark_helpers.concat(inner, names='scenario')
        for k, inner in dfs.items()
    }


def create_run_id() -> str:
    """Create run id using username and current datetime.

    Returns
    -------
    str
        The unique identifying string for the run, of the form current
        date, time and Hadoop username (YYYYMMDD_HHMMSS_username).
    """
    username = os.environ['HADOOP_USER_NAME']
    current_date_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    return '_'.join([current_date_time, username])


def _get_run_times_as_df(times: Mapping[str, float]) -> pd.DataFrame:
    """Convert dictionary of times to a pandas DataFrame."""
    return pd.DataFrame.from_dict(times, orient='index', columns=['Seconds'])


def plot_run_times(times: Mapping[str, float]) -> None:
    """Plot the run times for the whole pipeline."""
    # # Reverse the index.
    # run_times = run_times.reindex(run_times.index[::-1])
    run_times = _get_run_times_as_df(times)
    LOGGER.info(times)

    plt.figure()
    run_times.plot(kind='barh', stacked=True, title='Run time [seconds]')
    plt.show()


def timer_args(name):
    """Initialise timer args as workaround for 'text' arg."""
    return {
        'name': name,
        'text': lambda secs: name + f": {format_timespan(secs)}",
        'logger': LOGGER.info,
    }


def get_config_params(spark: SparkSession, config: Config) -> SparkDF:
    """Create table with configuration parameters for the scenario.

    The table contains two columns. The first column shows the stage of the
    pipeline, and the second column shows a dictionary containing all the
    config parameters for the corresponding stage. This can be used as a
    reference for the user to check the configuration of this run.
    """
    configuration = (
        pd.DataFrame.from_dict(vars(config), orient='index')
        .reset_index()
        .astype(str)
    )

    return spark.createDataFrame(configuration)


def remove_dev_config_col(
    dev_config: Mapping[str, Any],
    to_remove: Union[str, Sequence[str]],
) -> Mapping[str, Any]:
    """Delete an entry from dev config file using string or list."""
    col_to_drop = [
        col for col in dev_config['groupby_cols'] if col.startswith(to_remove)
    ]

    for col in col_to_drop:
        dev_config['groupby_cols'].remove(col)
        dev_config['web_scraped_preprocess_cols'].remove(col)
        dev_config['scanner_preprocess_cols'].remove(col)

    return dev_config
