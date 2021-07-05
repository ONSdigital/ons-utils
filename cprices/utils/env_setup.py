"""Contains methods:

- set_pyspark_python_env USED IN MAIN.PY

"""
# import python libraries
import os
import re
import logging


LOGGER = logging.getLogger('')

def set_pyspark_python_env(pipeline_miscmods_version):
    """Ensure that the PYSPARK_PYTHON environment variable is up to date.

    If the PYSPARK_PYTHON variable has been set in the CDSW project settings,
    we check what version this is set at. If it's higher than the value specfied
    then it is kept, otherwise the variable is overwritten  with the specified
    value.

    Usage set_pyspark_python_env(pipeline_miscmods_version=3.05)

    Parameters
    ----------
    pipeline_miscmods_version: float
        This is the version number for miscMods that CDSW requires for running
    """
    def reset_pyspark_python_env(pipeline_environ_pyspark_python):
        """Change the PYSPARK_PYTHON env variable.

        Parameters
        ----------
        pipeline_environ_pyspark_python: string
            The path to be set for PYSPARK_PYTHON
        """
        LOGGER.info(f'Setting PYSPARK_PYTHON environment variable to {pipeline_environ_pyspark_python}')
        os.environ['PYSPARK_PYTHON'] = pipeline_environ_pyspark_python

    # This is the version of miscMods that will be set if it is higher than the
    # preset value for PYSPARK_PYTHON
    pipeline_environ_pyspark_python = f'/opt/ons/virtualenv/miscMods_v{pipeline_miscmods_version}/bin/python3.6'

    default_environ_pyspark_python = os.environ.get('PYSPARK_PYTHON')

    if default_environ_pyspark_python is not None:
        # The PYSPARK_PYTHON variable is predefined so will try and check this

        LOGGER.info(f'PYSPARK_PYTHON environment variable is preset to {default_environ_pyspark_python}')

        try:
            default_miscmods_version = (
              float(re.findall('[v]\d+.\d+', default_environ_pyspark_python)[0][1:])
              )

            if default_miscmods_version < pipeline_miscmods_version:
                reset_pyspark_python_env(pipeline_environ_pyspark_python)

        except:
            reset_pyspark_python_env(pipeline_environ_pyspark_python)

    else:
        reset_pyspark_python_env(pipeline_environ_pyspark_python)
