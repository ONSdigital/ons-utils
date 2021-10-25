"""Contains spark setup function: start_spark_session."""
from contextlib import contextmanager
import logging
import os
from pathlib import Path
import re
from typing import Callable, Optional, Sequence, Tuple, Union

# Don't import pydoop on Jenkins.
if not os.getenv('JENKINS_HOME'):
    import pydoop.hdfs as hdfs
from pyspark.sql import SparkSession
import IPython

from cprices._typing import FilePath


LOGGER = logging.getLogger('')

NODES = {
    'd': 'DEVTEST',
    'u': 'UAT',
    'p': 'PROD',
}


def start_spark(
    session_size: str = 'medium',
    app_name: Optional[str] = None,
    enable_arrow: bool = False,
    miscmods_version: Optional[float] = None,
    archives: Optional[FilePath] = None,
    config: Optional[Sequence[Tuple[str, Union[str, int]]]] = None,
    logger:  Optional[Callable[[str], None]] = print,
) -> SparkSession:
    """Start the Spark Session.

    Provides the `DAPCATs recommended session sizes
    <http://np2rvlapxx507/DAP_CATS/guidance/-/blob/master/spark_session_sizes.ipynb>`_
    (with some minor variation):

    * small
    * medium
    * large (UAT or Prod only)
    * XL (UAT or Prod only)

    More guidance on spark session configuration:

    * `Spark Session Guidance <http://np2rvlapxx507/DAP_CATS/guidance/-/blob/master/Spark%20session%20guidance.md>`_
    * `Garbage Collection <http://np2rvlapxx507/DAP_CATS/troubleshooting/python-troubleshooting/blob/master/garbage_collection.md>`_

    Parameters
    ----------
    session_size : {'small', 'medium', 'large', 'xl'}, str
        The Spark session size.
    app_name : str
        The spark session app name, which is post-pended by the session size
    enable_arrow : bool, default False
        Enable compatibility setting for PyArrow >= 0.15.0 and Spark
        2.3.x, 2.4.x
    miscmods_version : optional, str
        A miscmods version number to use if not using archives.
    archives : optional, str
        A file path to the virtual environment archives. These can be
        local or on HDFS.
    config : list of tuple, optional
        A list of additional spark config properties coupled (as a
        tuple) with the value to set them as. See `Spark Application
        Properties <https://spark.apache.org/docs/latest/configuration.html#application-properties>`_.
    logger : callable, default print, optional
        To log the session size being created.

    Returns
    -------
    SparkSession
    """   # noqa: E501
    session_size = session_size.lower()
    platform = get_node_platform()
    if session_size in {'large', 'xl', 'xxl'} and platform == 'DEVTEST':
        raise ValueError("Given session size only available on Prod or UAT.")

    if miscmods_version and archives:
        raise ValueError(
            "Only one of miscmods_version and archives parameters"
            " must be given."
        )
    if archives and len(archives.split('#')) != 1:
        raise ValueError(
            "Pass the archive file location without a tag. The function"
            " adds the tag '#environment' to given archive."
        )

    if miscmods_version:
        os.environ['PYSPARK_PYTHON'] = get_miscmods_path(miscmods_version)
    elif archives:
        # Point to the python interpreter in the archived virtual env.
        os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"

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

    if app_name:
        builder = SparkSession.builder.appName(f'{app_name}-{session_size}')
    else:
        builder = SparkSession.builder

    spark = (
        builder
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.shuffle.service.enabled", "true")
        .enableHiveSupport()
    )

    for setting, values in settings.items():
        spark = spark.config(setting, values.get(session_size))

    if enable_arrow:
        spark = (
            spark
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.sql.execution.arrow.fallback.enabled", "true")
            .config("spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT", 1)
            .config("spark.workerEnv.ARROW_PRE_0_15_IPC_FORMAT", 1)
        )

    if archives:
        spark = spark.config(
            "spark.yarn.dist.archives", f"{archives}#environment"
        )

    config = [] if not config else config
    for setting, value in config:
        spark = spark.config(setting, value)

    return spark.getOrCreate()


def launch_spark_ui() -> None:
    """Displays a link to launch the Spark UI."""
    url = f"spark-{os.environ['CDSW_ENGINE_ID']}.{os.environ['CDSW_DOMAIN']}"
    return IPython.display.HTML(f"<a href=http://{url}>Spark UI</a>")


def get_node_platform():
    """Get the node platform from the environment variable."""
    node_name = os.getenv('CDSW_NODE_NAME')
    node_id = re.search(r'([a-z])\d{2}', node_name).group(1)
    return NODES.get(node_id)


def get_miscmods_path(ver) -> str:
    """Return the full miscMods path for the given version."""
    return str(Path('/opt/ons/virtualenv/miscMods_v{ver}/bin/python3'))


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
