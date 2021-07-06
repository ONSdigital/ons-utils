"""Tests for the env_setup.py module."""
import os

import pytest

from cprices.utils.spark_setup import *
from tests.conftest import parametrize_cases, Case


@parametrize_cases(
    Case('not_set', env_var=None),
    Case('local_python', env_var='/usr/local/bin/python3.6'),
    Case(
        'lower_miscmods_ver',
        env_var='opt/ons/virtualenv/miscMods_v2.05/bin/python3.6',
    ),
)
def test_set_pyspark_python_env_sets_right_path(monkeypatch, env_var):
    """Test that the PYSPARK_PYTHON is set given the input."""
    monkeypatch.setenv("PYSPARK_PYTHON", env_var)
    set_pyspark_python_env(3.05)

    assert os.getenv('PYSPARK_PYTHON') == 'opt/ons/virtualenv/miscMods_v3.05/bin/python3.6'


def test_doesnt_set_pyspark_python_env_when_already_later_version(monkeypatch):
    """Test that the PYSPARK_PYTHON is set given the input."""
    monkeypatch.setenv(
        "PYSPARK_PYTHON",
        'opt/ons/virtualenv/miscMods_v3.10/bin/python3.6',
    )
    set_pyspark_python_env(3.05)

    assert os.getenv('PYSPARK_PYTHON') == 'opt/ons/virtualenv/miscMods_v3.10/bin/python3.6'


@pytest.mark.spark
class TestSparkConf:
    """Tests for the spark_conf function."""

    @pytest.fixture(scope='function')
    def my_spark(self):
        """A Spark session for testing configuration."""
        return (
            SparkSession.builder.appName('cprices')
            .config('spark.dynamicAllocation.enabled', 'true')
            .config('spark.shuffle.service.enabled', 'true')
            .config('spark.ui.showConsoleProgress', 'false')
            .enableHiveSupport()
            .getOrCreate()
        )

    @parametrize_cases(
        Case(
            "default_session_size",
            session_size='default',
            memory='2g',
            cores='1',
        ),
        Case(
            "large_session_size",
            session_size='large',
            memory='8g',
            cores='3',
        ),
    )
    def test_sets_right_config_for_devtest(
        self, monkeypatch, my_spark,
        session_size, memory, cores,
    ):
        """The config should be as expected for given session size."""
        monkeypatch.setenv('CDSW_NODE_NAME', 'cdswwn-d01-01')
        spark_config(my_spark, session_size)
        assert my_spark.conf.get('spark.executor.memory') == memory
        assert my_spark.conf.get('spark.executor.cores') == cores
        assert my_spark.conf.get('spark.dynamicAllocation.maxExecutors') == '3'
        assert my_spark.conf.get('spark.yarn.executor.memoryOverhead') == '1g'

    def test_sets_right_config_for_prod(self, monkeypatch, my_spark):
        """The config should be as expected for when the Node is on Prod."""
        monkeypatch.setenv('CDSW_NODE_NAME', 'cdswwn-p01-01')
        spark_config(my_spark)
        assert my_spark.conf.get('spark.executor.memory') == '20g'
        assert my_spark.conf.get('spark.executor.cores') == '5'
        assert my_spark.conf.get('spark.dynamicAllocation.maxExecutors') == '12'
        assert my_spark.conf.get('spark.yarn.executor.memoryOverhead') == '2g'
        assert my_spark.conf.get('spark.driver.maxResultSize') == '6g'
        assert my_spark.conf.get('spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT') == '1'
        assert my_spark.conf.get('spark.wrokerEnv.ARROW_PREW_0_15_IPC_FORMAT') == '1'
