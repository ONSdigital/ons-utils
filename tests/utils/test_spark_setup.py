"""Tests for the env_setup.py module."""
import os

import pytest

from cprices.utils.spark_setup import *
from tests.conftest import parametrize_cases, Case


class TestSetPysparkPythonEnv:
    """Tests for set_pyspark_python_env."""

    @parametrize_cases(
        Case('not_set', env_var=None),
        Case('local_python', env_var='/usr/local/bin/python3.6'),
        Case(
            'lower_miscmods_ver',
            env_var='/opt/ons/virtualenv/miscMods_v2.05/bin/python3.6',
        ),
    )
    def test_set_pyspark_python_env_sets_right_path(self, monkeypatch, env_var):
        """Test that the PYSPARK_PYTHON is set given the input."""
        monkeypatch.setenv("PYSPARK_PYTHON", env_var)
        set_pyspark_python_env(3.05)

        assert os.getenv('PYSPARK_PYTHON') == '/opt/ons/virtualenv/miscMods_v3.05/bin/python3.6'

    def test_doesnt_set_pyspark_python_env_when_already_later_version(self, monkeypatch):
        """Test that the PYSPARK_PYTHON is set given the input."""
        monkeypatch.setenv(
            "PYSPARK_PYTHON",
            '/opt/ons/virtualenv/miscMods_v3.10/bin/python3.6',
        )
        set_pyspark_python_env(3.05)

        assert os.getenv('PYSPARK_PYTHON') == '/opt/ons/virtualenv/miscMods_v3.10/bin/python3.6'
