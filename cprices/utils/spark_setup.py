"""Contains spark setup function: start_spark_session."""
from contextlib import contextmanager
import logging
import os
from pathlib import Path
import re
from typing import Callable, Optional

# Don't import pydoop on Jenkins.
if not os.getenv('JENKINS_HOME'):
    import pydoop.hdfs as hdfs
from pyspark.sql import SparkSession
import IPython

from cprices._typing import FilePath

LOGGER = logging.getLogger('')


def start_spark_session(
    session_size: str = 'medium',
    miscmods_version: float = 3.05,
    appname: str = 'cprices',
    enable_arrow: bool = True,
    pyfiles_path: Optional[str] = None,
    logger:  Optional[Callable[[str], None]] = print,
) -> SparkSession:
    """Start the Spark Session.

    Provides the DAPCATs recommended (with some minor variation) session sizes:

        * small
        * medium
        * large (UAT or Prod only)
        * XL (UAT or Prod only)

    as detailed here:
    http://np2rvlapxx507/DAP_CATS/guidance/-/blob/master/spark_session_sizes.ipynb

    Further info on spark session configuration:
    http://np2rvlapxx507/DAP_CATS/guidance/-/blob/master/Spark%20session%20guidance.md
    http://np2rvlapxx507/DAP_CATS/troubleshooting/python-troubleshooting/blob/master/garbage_collection.md

    Parameters
    ----------
    session_size : {'small', 'medium', 'large', 'xl'}, str
        The Spark session size.
    miscmods_version : float, default 3.05
        The minimum miscmods version number to use.
    appname : str
        The spark session app name, which is post-pended by the session size
    enable_arrow : bool, default True
        Enable compatibility setting for PyArrow >= 0.15.0 and Spark
        2.3.x, 2.4.x
    pyfiles_path : str, optional
        The path for Python files to be uploaded to the executor.
    logger : callable, default print, optional
        To log the session size being created.

    Returns
    -------
    SparkSession
    """   # noqa: E501
    # Overrides PYSPARK_PYTHON if lower miscmods version than specified.
    set_pyspark_python_env(miscmods_version=miscmods_version)

    # XXL sizes are still under construction!
    settings = {
        "spark.executor.memory": {
            "small": "1g",
            "medium": "8g",
            "large": "10g",
            "xl": "20g",
            "xxl": "40g",
        },
        "spark.executor.cores": {
            "small": 1,
            "medium": 3,
            "large": 5,
            "xl": 5,
            "xxl": 5,
        },
        "spark.dynamicAllocation.maxExecutors": {
            "small": 3,
            "medium": 3,
            "large": 5,
            "xl": 12,
            "xxl": 6,
        },
        "spark.sql.shuffle.partitions": {
            "small": 12,
            "medium": 18,
            "large": 200,
            "xl": 240,
            "xxl": 240,
        },
        "spark.driver.maxResultSize": {
            "small": "1g",
            "medium": "1g",
            "large": "3g",
            "xl": "6g",
            "xxl": "6g",
        },
    }

    logger(f'Setting up a {session_size} spark session...')
    spark = (
        SparkSession.builder.appName(f'{appname}-{session_size}')
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .enableHiveSupport()
        .getOrCreate()
    )

    for setting, values in settings.items():
        spark.conf.set(setting, values.get(session_size))

    if enable_arrow:
        spark.conf.set('spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT', 1)
        spark.conf.set('spark.workerEnv.ARROW_PRE_0_15_IPC_FORMAT', 1)

    if pyfiles_path:
        spark.conf.set('spark.submit.pyFiles', pyfiles_path)

    return spark


def launch_spark_ui() -> None:
    """Displays a link to launch the Spark UI."""
    url = f"spark-{os.environ['CDSW_ENGINE_ID']}.{os.environ['CDSW_DOMAIN']}"
    return IPython.display.HTML(f"<a href=http://{url}>Spark UI</a>")


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
    LOGGER.debug(
        f'PYSPARK_PYTHON environment variable is preset to {current_env}'
    )

    if current_env:
        default_miscmods_version = find_miscmods_version(current_env)

    if (
        not current_env
        or not default_miscmods_version
        or (default_miscmods_version < miscmods_version)
    ):
        miscmods_path = (
            Path(
                '/opt/ons/virtualenv', f'miscMods_v{miscmods_version}',
                'bin/python3.6',
            )
            .as_posix()
        )

        LOGGER.debug(
            f'Setting PYSPARK_PYTHON environment variable to {miscmods_path}'
        )
        os.environ['PYSPARK_PYTHON'] = miscmods_path


def find_miscmods_version(s: str) -> Optional[float]:
    """Find the miscmods version from environment variable string."""
    version_no = re.search(r'(?<=miscMods_v)\d+\.\d+', s)
    return float(version_no.group()) if version_no else None


@contextmanager
def checkpoints(spark, checkpoint_dir: FilePath = None) -> None:
    """Context manager to set checkpoint directory and clear after use."""
    checkpoint_dir = set_checkpoint_dir(spark, checkpoint_dir)
    try:
        yield None
    finally:
        hdfs.rm(checkpoint_dir.as_posix())


def set_checkpoint_dir(spark, checkpoint_dir: FilePath = None) -> Path:
    """Set the checkpoint directory.

    If no checkpoint dir specified, then use the Hadoop username
    environment variable to create one. If the checkpoint dir already
    exists, a number suffix will be added until a dir is reached that
    doesn't exist.
    """
    if not checkpoint_dir:
        checkpoint_dir = get_default_checkpoint_dir()
    else:
        checkpoint_dir = Path(checkpoint_dir)

    # Add a number suffix if checkpoint dir already exists.
    checkpoint_dir = _get_unused_checkpoint_dir(checkpoint_dir)
    spark.sparkContext.setCheckpointDir(checkpoint_dir.as_posix())

    return checkpoint_dir


def get_default_checkpoint_dir() -> Path:
    """Return the default checkpoint directory."""
    username = os.getenv("HADOOP_USER_NAME")
    return Path('/', 'user', username, 'checkpoints')


def _get_unused_checkpoint_dir(checkpoint_dir: Path) -> Path:
    """Get a checkpoint_dir path that doesn't already exist.

    Keeps adding 1 to the checkpoint_dir path until it finds one that
    doesn't exist. This is to stop a checkpoint_dir being removed when
    parallel runs of the same system are taking place, as the context
    manager removes the checkpoint_dir. This also prevents accidentally
    removing a directory that has other files in it.
    """
    n = 1
    new_checkpoint_dir = checkpoint_dir.joinpath(str(n))
    while hdfs.path.isdir(new_checkpoint_dir.as_posix()):
        new_checkpoint_dir = checkpoint_dir.joinpath(str(n))
        n += 1

    return new_checkpoint_dir
