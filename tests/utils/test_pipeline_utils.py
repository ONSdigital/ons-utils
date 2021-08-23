"""Tests for pipeline utils."""
from pyspark.sql.types import *
import pytest

from cprices.utils.pipeline_utils import *


@pytest.mark.skip(reason="test shell")
def test_combine_scenario_df_outputs():
    """Test for this."""
    pass


@pytest.mark.skip(reason="requires mocking of env variables")
def test_create_run_id():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_timer_args():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_get_run_times_as_df():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_plot_run_times():
    """Test for this."""
    pass


def test_check_empty(spark_session):
    """Test the check_empty function raises DataFrameEmptyError."""
    # Create an empty DataFrame.
    empty_RDD = spark_session.sparkContext.emptyRDD()
    df = spark_session.createDataFrame(
        data=empty_RDD,
        schema=StructType([]),
    )

    with pytest.raises(DataFrameEmptyError):
        check_empty(df)
