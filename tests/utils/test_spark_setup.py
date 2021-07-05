"""Tests for the env_setup.py module."""
import os

import pytest

from cprices.utils.spark_setup import *


@pytest.mark.parametrize(
    'env_var',
    [
        None,
        '/usr/local/bin/python3.6',
        'opt/ons/virtualenv/miscMods_v2.05/bin/python3.6',
    ],
    ids=['not_set', 'local_python', 'lower_miscmods_ver']
)
def test_set_pyspark_python_env_sets_right_path(monkeypatch, env_var):
    """Test that the PYSPARK_PYTHON is set given the input."""
    monkeypatch.setenv("PYSPARK_PYTHON", env_var)
    set_pyspark_python_env(3.05)

    assert (
        os.getenv('PYSPARK_PYTHON')
        == 'opt/ons/virtualenv/miscMods_v3.05/bin/python3.6'
    )


def test_doesnt_set_pyspark_python_env_when_already_later_version(monkeypatch):
    """Test that the PYSPARK_PYTHON is set given the input."""
    monkeypatch.setenv(
        "PYSPARK_PYTHON",
        'opt/ons/virtualenv/miscMods_v3.10/bin/python3.6',
    )
    set_pyspark_python_env(3.05)

    assert (
        os.getenv('PYSPARK_PYTHON')
        == 'opt/ons/virtualenv/miscMods_v3.10/bin/python3.6'
    )
