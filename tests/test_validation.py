"""Tests for the config validation functions in validation.py."""
from typing import Sequence, Union

import cerberus
import pytest
import toolz

from cprices.utils.helpers import tuple_convert
from cprices.validation import *
from tests.conftest import parametrize_cases, Case


@pytest.mark.skip(reason="test shell")
def test_validate_config_sections():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_validate_preprocessing():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_validate_classification():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_validate_mapper_settings():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_validate_outlier_detection():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_validate_averaging_and_grouping():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_validate_imputation():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_validate_filtering():
    """Test for this."""
    pass


@pytest.mark.skip(reason="test shell")
def test_validate_indices():
    """Test for this."""
    pass


@pytest.fixture(scope='module')
def config_section():
    return {
        'number_of_twists': 5,
        'burn': True,
        'parts': ['arm', 'leg'],
        'method': 'lightning',
    }


@pytest.fixture(scope='module')
def validation_schema():
    return {
        'number_of_twists': {
            'type': 'integer',
            'min': 0,
            'max': 4
        },
        'burn': {'type': 'boolean'},
        'parts': {
            'type': 'list',
            'allowed': ['arm', 'head', 'chest']
        },
        'method': {
            'type': 'string',
            'allowed': ['hamster', 'engine']
        },
        'vehicle': {
            'type': ['string', 'list'],
            'allowed': ['plane', 'truck']
        },
        'helper': {
            'type': 'dict',
            'schema': {
                'creature': {'type': 'string'},
                'name': {'type': 'string'},
            },
        },
    }


@pytest.fixture(scope='module')
def validator_errors(validation_schema):
    """"""
    config = {
        'number_of_twists': 5,
        'burn': 'yezzir',
        'parts': ['arm', 'leg'],
        'method': 'lightning',
        'vehicle': 1.56,
        'helper': {
            'creature': 'turtle',
            'name': 5.0,
        }
    }

    v = cerberus.Validator(validation_schema)
    v.validate(config)
    return v


@pytest.fixture(scope='module')
def validator_passing(validation_schema):
    """"""
    config = {
        'number_of_twists': 2,
        'burn': True,
        'parts': ['arm', 'chest'],
        'method': 'hamster',
        'vehicle': 'plane',
        'helper': {
            'creature': 'turtle',
            'name': 'frederick',
        }
    }

    v = cerberus.Validator(validation_schema)
    v.validate(config)
    return v


@pytest.fixture
def get_error(validator_errors):
    def _(parameter: Union[str, Sequence[str]]):
        return (
            toolz.get_in(
                tuple_convert(parameter),
                validator_errors.document_error_tree)
            .errors[0]
        )
    return _



@pytest.fixture
def outside_constraints_error(get_error):
    return get_error('number_of_twists')


@pytest.fixture
def type_error_boolean(get_error):
    return get_error('burn')

@pytest.fixture
def not_allowed_multiple_error(get_error):
    return get_error('parts')

@pytest.fixture
def not_allowed_single_error(get_error):
    return get_error('method')





class TestErrorMessageBuilder:

    @parametrize_cases(
        Case(
            label="returns_single_section",
            parameter=('helper', 'name'),
            expected=('helper',)
        ),
        Case(
            label="returns_empty_tuple_when_no_sections",
            parameter='burn',
            expected=(),
        ),
        # Case for multiple sections
    )
    def test_get_sections(
        self, get_error, validation_schema,
        parameter, expected,
    ):
        """ """
        err = get_error(parameter)
        msg_builder = ErrorMessageBuilder(err, validation_schema)
        assert msg_builder.get_sections() == expected


    @parametrize_cases(
        Case(
            label="returns_name_in_nested_section",
            parameter=('helper', 'name'),
            expected='name'
        ),
        Case(
            label="returns_name_when_no_sections",
            parameter='burn',
            expected='burn',
        ),
        # Case for multiple sections
    )
    def test_get_parameter_name(
        self, get_error, validation_schema,
        parameter, expected,
    ):
        err = get_error(parameter)
        msg_builder = ErrorMessageBuilder(err, validation_schema)
        assert msg_builder.get_parameter_name() == expected

    @parametrize_cases(
        Case(
            label="returns_name_in_nested_section",
            parameter=('helper', 'name'),
            expected='string'
        ),
        Case(
            label="returns_name_when_no_sections",
            parameter='burn',
            expected='boolean',
        ),
        # Case for multiple sections
    )
    def test_expected_type(
        self, get_error, validation_schema,
        parameter, expected,
    ):
        err = get_error(parameter)
        msg_builder = ErrorMessageBuilder(err, validation_schema)
        assert msg_builder.get_expected_type() == expected
