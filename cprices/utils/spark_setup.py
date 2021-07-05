"""Contains spark setup function: start_spark_session."""
import logging
import os
from pathlib import Path
import re
from typing import Union

from pyspark.sql import SparkSession

LOGGER = logging.getLogger('')


def start_spark_session(session_size: str = 'default') -> SparkSession:
    """Start the Spark Session.

    Parameters
    ----------
    session_size : {'default', 'large'}, str
        The Spark session size.

    Returns
    -------
    SparkSession
    """
    # Overrides preset PYSPARK_PYTHON if lower miscmods version than
    # specified.
    set_pyspark_python_env(miscmods_version=3.05)

    executor_memory = '8g' if session_size == 'large' else '2g'
    executor_cores = 3 if session_size == 'large' else 1
    return (
        SparkSession.builder.appName('cprices')
        .config('spark.executor.memory', executor_memory)
        .config('spark.yarn.executor.memoryOverhead', '1g')
        .config('spark.executor.cores', executor_cores)
        .config('spark.dynamicAllocation.maxExecutors', 3)
        .config('spark.dynamicAllocation.enabled', 'true')
        .config('spark.shuffle.service.enabled', 'true')
        # This stops progress bars appearing in the console whilst running
        .config('spark.ui.showConsoleProgress', 'false')
        .enableHiveSupport()
        .getOrCreate()
    )


def set_pyspark_python_env(miscmods_version: float) -> None:
    """Set PYSPARK_PYTHON environment variable if necessary.

    Checks current miscMods version in the PYSPARK_PYTHON environment
    variable and updates the variable if the the version is less than
    the miscmods version given. Also sets PYSPARK_PYTHON if not already
    set or the PYSPARK_PYTHON path is not in a miscMods dir.

    Parameters
    ----------
    miscmods_version: float
        The version number for miscMods that CDSW requires to run.

    Examples
    --------
    >>> # cprices pipeline needs > miscModsv3.05
    >>> set_pyspark_python_env(miscmods_version=3.05)
    """
    current_env = os.getenv('PYSPARK_PYTHON')
    # The PYSPARK_PYTHON variable is predefined so will try and check this.
    LOGGER.info(
        f'PYSPARK_PYTHON environment variable is preset to {current_env}'
    )

    if current_env:
        default_miscmods_version = find_miscmods_version(current_env)

    if (
        not current_env
        or not default_miscmods_version
        or (default_miscmods_version < miscmods_version)
    ):
        miscmods_path = Path(
            'opt', 'ons', 'virtualenv', f'miscMods_v{miscmods_version}',
            'bin', 'python3.6',
        )

        LOGGER.info(
            f'Setting PYSPARK_PYTHON environment variable to {miscmods_path}'
        )
        os.environ['PYSPARK_PYTHON'] = miscmods_path.as_posix()


def find_miscmods_version(s: str) -> Union[float, None]:
    """Find the miscmods version from environment variable string."""
    version_no = re.search(r'(?<=miscMods_v)\d+\.\d+', s)
    if version_no:
        return float(version_no.group())
    else:
        return None
