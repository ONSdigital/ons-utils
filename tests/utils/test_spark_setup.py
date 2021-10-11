"""Tests for the spark_setup.py module."""
import pytest

from cprices.utils.spark_setup import *


@pytest.fixture
def cdsw_node_name(monkeypatch):
    monkeypatch.setenv('CDSW_NODE_NAME', 'cdswwn-d01-02')


@pytest.mark.usefixtures('cdsw_node_name')
class TestStartSpark:

    @pytest.fixture(params=['large', 'xl', 'xxl'])
    def large_sizes(self, request):
        return request.param

    def test_raises_if_session_size_too_high(self, large_sizes):
        """Large, xl and xxl can't be set on DEVTEST platform."""
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
        self, create_empty_file, venv_path
    ):
        """The archives filepath shouldn't have a # tag since its added by function."""
        create_empty_file(venv_path)
        venv_path_with_tag = f"{venv_path}#environment"
        with pytest.raises(ValueError):
            start_spark(
                miscmods_version='3.05',
                archives=venv_path_with_tag,
            )
