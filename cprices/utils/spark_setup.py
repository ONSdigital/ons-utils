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

    spark = (
        SparkSession.builder.appName('cprices')
        .config('spark.dynamicAllocation.enabled', 'true')
        .config('spark.shuffle.service.enabled', 'true')
        .config('spark.ui.showConsoleProgress', 'false')
        .enableHiveSupport()
        .getOrCreate()
    )
    # Configure Spark based on which cluster it's on.
    spark_config(spark, session_size)
    return spark


def spark_config(spark: SparkSession, session_size: str = 'default') -> None:
    """Set the Spark config based on CDSW node."""
    # Get the node ID from the environment variable.
    node_name = os.getenv('CDSW_NODE_NAME')
    node_id = re.search(r'([a-z])\d{2}', node_name).group(1)

    # 'd' for DevTest, 'u' for UAT, 'p' for Prod.
    if node_id == 'd':
        # For DevTest.
        executor_memory = '8g' if session_size == 'large' else '2g'
        executor_cores = 3 if session_size == 'large' else 1
        max_executors = 3
        memory_overhead = '1g'
    elif node_id == 'p':
        # For Prod.
        executor_memory = '20g'
        executor_cores = 5
        max_executors = 12
        memory_overhead = '2g'

        spark.conf.set('spark.driver.maxResultSize', '6g')
        spark.conf.set('spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT', 1)
        spark.conf.set('spark.wrokerEnv.ARROW_PREW_0_15_IPC_FORMAT', 1)

    spark.conf.set('spark.executor.memory', executor_memory)
    spark.conf.set('spark.executor.cores', executor_cores)
    spark.conf.set('spark.dynamicAllocation.maxExecutors', max_executors)
    spark.conf.set('spark.yarn.executor.memoryOverhead', memory_overhead)


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
