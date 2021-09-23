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
from datetime import datetime
import logging
import os
import textwrap
from typing import Callable, Dict, List, Mapping, Optional

from humanfriendly import format_timespan
import matplotlib as mpl
import pandas as pd

from pyspark.sql import (
    DataFrame as SparkDF,
    functions as F,
    SparkSession,
)

from cprices.config import Config
from cprices.utils.helpers import invert_nested_keys
from cprices.utils import spark_helpers

LOGGER = logging.getLogger()

# So matplotlib works over SSH.
if os.environ.get('DISPLAY', '') == '':
    LOGGER.debug(
        'No display found. Using non-interactive Agg backend for matplotlib.'
    )
    mpl.use('Agg')

import matplotlib.pyplot as plt     # noqa: E402


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


def check_empty(df: SparkDF) -> None:
    """Raise a DataFrameEmptyError if DataFrame is empty."""
    if len(df.head(1)) == 0:
        raise DataFrameEmptyError


class DataFrameEmptyError(Exception):

    def __str__(self):
        return (
            "The DataFrame is empty."
            " Investigate the issue, fix and rerun the pipeline."
        )


def pretty_wrap(s: str, width: int = 100, border_char: str = None) -> str:
    """Apply dedent to triple quoted text and wrap to width.

    Option to add a border character that will top and tail the string.

    Parameters
    ----------
    s : str
        The triple quoted string to dedent and wrap.
    width : int, default 100
        Number of chars to wrap to.
    border_char : str, optional
        A single char to be the border marker. The border will top and
        tail the string and will be of len ``width``. Leave as ``None``
        for no border.

    Returns
    -------
    str
        The wrapped and dedented string with optional border.
    """
    # Splitting and joining on double line break preserves the
    # paragraphs.
    wrapped_str = (
        '\n\n'.join([
            textwrap.fill(paragraph, width)
            # Stops lists being wrapped.
            if not paragraph.startswith(('*', '-'))
            else paragraph
            for paragraph in textwrap.dedent(s).split('\n\n')
        ])
    )
    if wrapped_str[0] == ' ':
        # Remove the first char as it's whitespace. Occurs when doing a
        # line break straight after triple quotes.
        wrapped_str = wrapped_str[1:]

    if border_char:
        border_str = border_char*width
        wrapped_str = '\n'.join([border_str, wrapped_str, border_str])

    return wrapped_str


def to_title(s: str) -> str:
    """Prints a title with underline and newline."""
    return f"\n{s.replace('_', ' ').title()}\n{len(s) * '='}"


# Created this function to potentially use instead of just cache().count()
# as it provides some additional logging. Intention is that it would be
# used with an if statement to potentially stop the function in its tracks,
# resulting in no output for a given scenario. But then continuing on
# with the run to calculate further scenarios.
def count_rows_and_check_if_empty(
    df: SparkDF,
    stage: str,
    scenario_name: str,
    logger: Optional[Callable[[str], None]] = print,
) -> bool:
    """Count DataFrame rows and return True if empty.

    Caches the DataFrame before the count so the output from the Spark
    execution plan is saved in memory. Logs the number of rows in the
    DataFrame. Prints a warning if the DataFrame is empty.
    """
    n_rows = df.cache().count()
    if n_rows == 0:
        logger.warning(pretty_wrap(f"""
            DataFrame after stage {stage} is empty for scenario
            {scenario_name}. Contact emerging platforms to investigate
            why. There will be no results for this scenario. Continuing
            to the next.
        """))
        return True
    else:
        logger.info(f"DataFrame after {stage} stage has {n_rows} rows.")
        return False


def apply_mapper(
    df: SparkDF,
    mapper: SparkDF,
    keys: List[str],
    column_to_fill: str,
    new_values: str,
) -> SparkDF:
    """Apply mapper and replace column null values with specified column
    values.

    Parameters
    ----------
    df : spark dataframe
        The original dataframe containing key_cols.
    mapper : spark dataframe
        The imported mapper containing key_cols.
    keys : list of str
        The name of the join columns. The columns must exist on both
        sides.
    column_to_fill : str
        The name of the column containing null values to be updated.
    new_values : str
        The name of the column to update the null values with.

    Returns
    -------
    SparkDF
        The original dataframe joined to the mapper with updated column.
    """
    if isinstance(keys, str):
        raise TypeError("Keys must be in a list format.")

    for key in keys:
        if key not in df.columns:
            raise ValueError(
                "Join column only exists in mapper."
                " Update to a column on both sides."
            )

    return (
        df
        .join(mapper, on=[*keys], how='left')
        .withColumn(column_to_fill, F.coalesce(column_to_fill, new_values))
    )
