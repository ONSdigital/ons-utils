"""Helper functions for setting up the environment.

Contains:

* :func:`set_pyspark_python_env` - used in :mod:`cprices.main`.
* :func:`checkpoints` - a context manager for setting a checkpoint
  directory and clearing checkpoints after code has run.

"""
from contextlib import contextmanager
from copy import copy
import logging
import os
import re
import subprocess

from epds_utils.hdfs import hdfs_utils


LOGGER = logging.getLogger('')


def set_pyspark_python_env(pipeline_miscmods_version):
    """Ensure that PYSPARK_PYTHON environment variable is up to date.

    If the PYSPARK_PYTHON variable has been set in the CDSW project
    settings, we check what version this is set at. If it's higher than
    the value specified then it is kept, otherwise the variable is
    overwritten  with the specified value.

    Usage set_pyspark_python_env(pipeline_miscmods_version=3.05).

    Parameters
    ----------
    pipeline_miscmods_version: float
        This is the version number for miscMods that CDSW requires for
        running.
    """
    # This is the version of miscMods that will be set if it is higher than the
    # preset value for PYSPARK_PYTHON
    pipeline_environ_pyspark_python = os.path.join(
        '/opt',
        'ons',
        'virtualenv',
        f'miscMods_v{pipeline_miscmods_version}',
        'bin',
        'python3.6',
    )

    default_environ_pyspark_python = os.environ.get('PYSPARK_PYTHON')

    if default_environ_pyspark_python is not None:
        # The PYSPARK_PYTHON variable is predefined so will try and check this
        LOGGER.info(
            'PYSPARK_PYTHON environment variable is preset to'
            f' {default_environ_pyspark_python}'
        )
        try:
            default_miscmods_version = (
                float(
                    re.findall(
                        r'[v]\d+.\d+', default_environ_pyspark_python
                    )[0][1:]
                )
            )
            if default_miscmods_version < pipeline_miscmods_version:
                reset_pyspark_python_env(pipeline_environ_pyspark_python)
        except:
            reset_pyspark_python_env(pipeline_environ_pyspark_python)
    else:
        reset_pyspark_python_env(pipeline_environ_pyspark_python)


def reset_pyspark_python_env(pipeline_environ_pyspark_python):
    """Change the PYSPARK_PYTHON env variable.

    Parameters
    ----------
    pipeline_environ_pyspark_python : string
        The path to be set for PYSPARK_PYTHON.
    """
    LOGGER.info(
        'Setting PYSPARK_PYTHON environment variable to'
        f' {pipeline_environ_pyspark_python}'
    )
    os.environ['PYSPARK_PYTHON'] = pipeline_environ_pyspark_python


@contextmanager
def checkpoints(spark, checkpoint_dir):
    """Context manager to set checkpoint directory and clear after use."""
    checkpoint_dir = get_unused_checkpoint_dir(checkpoint_dir)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    try:
        yield None
    finally:
        cmd = ['hadoop',  'fs', '-rm', '-r', '-skipTrash', checkpoint_dir]
        subprocess.run(cmd)


def get_unused_checkpoint_dir(checkpoint_dir):
    """Get a checkpoint_dir path that doesn't already exist.

    Keeps adding 1 to the checkpoint_dir path until it finds one that
    doesn't exist. This is to stop a checkpoint_dir being removed when
    parallel runs of the same system are taking place, as the context
    manager removes the checkpoint_dir.
    """
    n = 1
    new_checkpoint_dir = copy(checkpoint_dir)
    while hdfs_utils.isdir(new_checkpoint_dir):
        new_checkpoint_dir = checkpoint_dir + str(n)
        n += 1

    return new_checkpoint_dir
