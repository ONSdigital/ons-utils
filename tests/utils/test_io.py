"""Tests for the io.py module."""
import textwrap

import pytest

from tests.conftest import (
    Case,
    parametrize_cases,
)
from cprices.utils.io import *


@pytest.mark.skip(reason="test shell")
def test_read_hive_table():
    """Test for this."""
    pass


class TestBuildSqlQuery:
    """Tests for build_sql_query."""

    @parametrize_cases(
        Case(
            label='no additional filters specified',
            columns=None,
            date_column=None,
            date_range=None,
            column_filter_dict=None,
            expected="""
                SELECT * FROM database_name.table_name
            """,
        ),
        Case(
            label='columns specified',
            columns=['col1', 'col2'],
            date_column=None,
            date_range=None,
            column_filter_dict=None,
            expected="""
                SELECT col1, col2 FROM database_name.table_name
            """,
        ),
        Case(
            label='date range specified',
            columns=None,
            date_column='date',
            date_range=('2019-01-01', '2021-01-01'),
            column_filter_dict=None,
            expected="""
                SELECT * FROM database_name.table_name
                WHERE (
                date >= '2019-01-01'
                AND date < '2021-01-01'
                )
            """,
        ),
        Case(
            label='one filter column, one option specified',
            columns=None,
            date_column=None,
            date_range=None,
            column_filter_dict=dict(
                column_1=['value_1.1']
            ),
            expected="""
                SELECT * FROM database_name.table_name
                WHERE (
                column_1 = 'value_1.1'
                )
            """,
        ),
        Case(
            label='one filter column, two options specified',
            columns=None,
            date_column=None,
            date_range=None,
            column_filter_dict=dict(
                column_1=['value_1.1', 'value_1.2']
            ),
            expected="""
                SELECT * FROM database_name.table_name
                WHERE (
                column_1 = 'value_1.1'
                OR column_1 = 'value_1.2'
                )
            """,
        ),
        Case(
            label='two filter columns, one option specified',
            columns=None,
            date_column=None,
            date_range=None,
            column_filter_dict=dict(
                column_1=['value_1.1'],
                column_2=['value_2.1']
            ),
            expected="""
                SELECT * FROM database_name.table_name
                WHERE (
                column_1 = 'value_1.1'
                )
                AND (
                column_2 = 'value_2.1'
                )
            """,
        ),
        Case(
            label='two filter columns, 2 and 1 options specified',
            columns=None,
            date_column=None,
            date_range=None,
            column_filter_dict=dict(
                column_1=['value_1.1', 'value_1.2'],
                column_2=['value_2.1']
            ),
            expected="""
                SELECT * FROM database_name.table_name
                WHERE (
                column_1 = 'value_1.1'
                OR column_1 = 'value_1.2'
                )
                AND (
                column_2 = 'value_2.1'
                )
            """,
        ),
        Case(
            label='date_range and filter columns specified',
            columns=None,
            date_column='date',
            date_range=('2019-01-01', '2021-01-01'),
            column_filter_dict=dict(
                column_1=['value_1.1', 'value_1.2'],
                column_2=['value_2.1', 'value_2.2', 'value_2.3']
            ),
            expected="""
                SELECT * FROM database_name.table_name
                WHERE (
                date >= '2019-01-01'
                AND date < '2021-01-01'
                )
                AND (
                column_1 = 'value_1.1'
                OR column_1 = 'value_1.2'
                )
                AND (
                column_2 = 'value_2.1'
                OR column_2 = 'value_2.2'
                OR column_2 = 'value_2.3'
                )
            """,
        ),
    )
    def test_method(
        self,
        columns,
        date_column,
        date_range,
        column_filter_dict,
        expected
    ):
        """Test expected behaviour."""
        table_path = 'database_name.table_name'

        result = (
            build_sql_query(
                table_path,
                columns=columns,
                date_column=date_column,
                date_range=date_range,
                column_filter_dict=column_filter_dict,
            )
        )

        # Use textwrap.dedent to remove leading whitespace from the string for comparing
        expected = textwrap.dedent(expected)

        assert(result.strip('\n') == expected.strip('\n'))


@pytest.mark.skip(reason="test shell")
def test_write_hive_table():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_read_output():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_get_recent_run_ids():
    """Test for this."""
    pass


def test_get_directory_from_env_var(monkeypatch):
    """Tests returns env var value."""
    monkeypatch.setenv('CPRICES_TEST_DIR', '/test/dir')
    assert get_directory_from_env_var('CPRICES_TEST_DIR') == '/test/dir'


def test_get_directory_from_env_var_raises_when_no_var():
    with pytest.raises(DirectoryError):
        # Doesn't exist so raises error.
        get_directory_from_env_var('CPRICES_TEST_DIR')
