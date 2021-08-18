"""Validation functions for scenario config files.

Provides:

* :func:`validate_scan_scenario_config`
* :func:`validate_webscraped_scenario_config`

Both functions return an error message that can be used to raise an
exception. The intention is that the messages from all scenarios will be
combined before being raised.
"""
from typing import Dict, Sequence, Mapping, Union, Hashable

import cerberus
from flatten_dict import flatten
from epds_utils import hdfs

from cprices.validation_schemas import full_schema


def validate_scan_scenario_config(config) -> str:
    """Validate the config using required sections for scanner.

    Returns
    -------
    str
        An error message with all validation errors. Returns an empty
        string if no errors.
    """
    return get_all_errors(
        config,
        sections=[
            'input_data',
            'preprocessing',
            'outlier_detection',
            'averaging',
            'flag_low_expenditures',
            'indices',
        ],
        mapper_sections=[
            'consumption_segment_mappers'
        ],
    )


def validate_webscraped_scenario_config(config) -> str:
    """Validate the config using required sections for web scraped.

    Returns
    -------
    str
        An error message with all validation errors. Returns an empty
        string if no errors.
    """
    return get_all_errors(
        config,
        sections=[
            'input_data',
            'outlier_detection',
            'averaging',
            'grouping'
            'indices'
        ],
        mapper_sections=[
            'consumption_segment_mappers'
        ],
    )


def get_all_errors(
    config,
    sections: Sequence[str],
    mapper_sections: Sequence[str]
) -> str:
    """Combine cerberus and mapper error messages."""
    # Get section errors.
    schema = full_schema(sections)
    err_msgs = get_cerberus_errors(vars(config), schema)
    err_msgs += get_mapper_errors(config, mapper_sections)

    # Get header.
    if err_msgs:
        header = get_underlined_header(
            f"Validation errors for config {config.name}"
        )
        err_msgs = ['\n' + header] + err_msgs

    # Combine.
    return '\n'.join(err_msgs)


def get_cerberus_errors(config: Mapping, schema: Mapping) -> Sequence[str]:
    """Validate the config using the cerberus schema and output errors."""
    v = cerberus.Validator(schema, allow_unknown=True)

    err_msgs = []
    if not v.validate(config):
        # Get the errors in a suitable format.
        errs = flatten(remove_list_wrappers(v.errors))
        for param, msg in errs.items():
            err_msgs.append(f"parameter {param}: {msg}")

    return err_msgs


def get_mapper_errors(config, sections: Sequence[str]) -> Sequence[str]:
    """Validate that the mappers exist and output error messages."""
    # Get mapper errors.
    mapper_err_msgs = []
    for section in sections:
        err_msgs = validate_filepaths(getattr(config, section))
        if err_msgs:
            mapper_err_msgs.append(
                "\n".join(['\n' + section + ' errors:'] + err_msgs)
            )

    return mapper_err_msgs


def validate_filepaths(filepaths: Mapping[Hashable, str]) -> Sequence[str]:
    """Validate a dict of filepaths and output resulting errors."""
    err_msgs = []
    # Flatten so it can handle any nesting level.
    for key, path in filepaths.items():
        if not hdfs.test(path):
            err_msgs.append(
                f"{key}: file at {path} does not exist."
            )

    return err_msgs


def get_underlined_header(header: str, char: str = '-') -> str:
    """Underline the header with given char."""
    underline = char * len(header)
    return '\n'.join([header, underline])


def remove_list_wrappers(
    d: Mapping[str, Union[Sequence[str], Sequence[Mapping]]],
) -> Dict[str, Union[str, Dict]]:
    """Remove list wrappers from dict values recursively.

    The output from cerberus.errors is a dict with the
    values wrapped in a list. This outputs a nested dict.
    """
    new_d = {}
    for k, v in d.items():
        # Access first element in list.
        v = v[0]
        if isinstance(v, dict):
            # Remove list wrappers from the inner dicts too.
            new_d.update({k: remove_list_wrappers(v)})
        else:
            new_d.update({k: v})

    return new_d


if __name__ == "__main__":
    from cprices.config import ScanScenarioConfig, WebScrapedScenarioConfig
    sc_config = ScanScenarioConfig('scenario_scan', subdir='scanner')
    print(validate_scan_scenario_config(sc_config))

    sc_config = WebScrapedScenarioConfig('scenario_web', subdir='web_scraped')
    print(validate_webscraped_scenario_config(sc_config))
