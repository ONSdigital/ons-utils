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
        set_pyspark_python_env('3.05')

        assert os.getenv('PYSPARK_PYTHON') == '/opt/ons/virtualenv/miscMods_v3.05/bin/python3.6'

    def test_doesnt_set_pyspark_python_env_when_already_later_version(self, monkeypatch):
        """Test that the PYSPARK_PYTHON is set given the input."""
        monkeypatch.setenv(
            "PYSPARK_PYTHON",
            '/opt/ons/virtualenv/miscMods_v3.10/bin/python3.6',
        )
        set_pyspark_python_env('3.05')

        assert os.getenv('PYSPARK_PYTHON') == '/opt/ons/virtualenv/miscMods_v3.10/bin/python3.6'


class TestStartSpark:

    @pytest.fixture(params=['large', 'xl', 'xxl'])
    def large_sizes(self, request):
        return request.param

    def test_raises_if_session_size_too_high(self, large_sizes, monkeypatch):
        """Large, xl and xxl can't be set on DEVTEST platform."""
        monkeypatch.setenv('CDSW_NODE_NAME', 'cdswwn-d01-02')
        with pytest.raises(ValueError):
            start_spark(session_size=large_sizes)

    @pytest.fixture
    def venv_path(self, tmpdir):
        return tmpdir.join('test_venv.tar.gz')

    @pytest.fixture
    def create_empty_file(self):
        def _(file_path):
            with open(file_path, 'w') as f:
                f.write('')
        return _

    def test_raises_if_archives_and_miscmods_version_both_passed(
        self, create_empty_file, venv_path,
    ):
        """Only one or the other must be provided."""
        create_empty_file(venv_path)
        with pytest.raises(ValueError):
            start_spark(
                miscmods_version='3.05',
                archives=venv_path,
            )

    def test_raises_if_archives_filepath_has_tag(
        self, create_empty_file, venv_path,
    ):
        """The archives filepath shouldn't have a # tag since its added by function."""
        create_empty_file(venv_path)
        venv_path_with_tag = f"{venv_path}#environment"
        with pytest.raises(ValueError):
            start_spark(
                miscmods_version='3.05',
                archives=venv_path_with_tag,
            )
