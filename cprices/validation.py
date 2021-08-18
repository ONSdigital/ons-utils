"""Validation rules for config files."""
from collections import abc
from typing import Sequence, Mapping

import cerberus
from flatten_dict import flatten


from epds_utils import hdfs


class ScenarioSectionError(Exception):
    """Exception raised when sections are missing in the config.name file."""

    def __init__(self, config, section):
        """Init the ScenarioSectionError."""
        self.message = (
            f"{config.name}: {section} does not appear"
            " among the config parameters."
        )
        super().__init__(self.message)


def check_config_sections_exist(config, sections) -> None:
    """Validate that all sections are in the given config."""
    for key in sections:
        if key not in vars(config).keys():
            raise ScenarioSectionError(config.name, key)


# def validate_section_values(config, sections: Sequence[str]):
#     """Validate the given sections with the appropriate validator."""
#     validators = validators_lib()
#     for section in sections:
#         validator = validators.get(section)
#         validator(config)


def validate_conventional_scenario_sections(config) -> None:
    """Validate the sections in the conventional scenario config."""
    required_sections = ['input_data']
    check_config_sections_exist(config, required_sections)


def validate_scan_scenario_config(config) -> None:
    """Validate the config using required sections for scanner."""
    required_sections = [
        'input_data',
        'preprocessing',
        'consumption_segment_mappers',
        'outlier_detection',
        'averaging',
        'flag_low_expenditures',
        'indices'
    ]
    check_config_sections_exist(config, required_sections)
    # validate_section_values(config, required_sections)


def validate_webscraped_scenario_config(config) -> None:
    """Validate the config using required sections for web scraped."""
    required_sections = [
        'input_data',
        'consumption_segment_mappers',
        'outlier_detection',
        'averaging',
        'grouping'
        'indices'
    ]
    check_config_sections_exist(config, required_sections)
    # validate_section_values(config, required_sections)


def validate_filepaths(filepaths: Mapping) -> Sequence[str]:
    """Validate a dict of filepaths and output resulting errors."""
    err_msgs = []
    # Flatten so it can handle any nesting level.
    for key, path in flatten(filepaths).items():
        if not hdfs.test(path):
            err_msgs.append(
                f"{key}: file at {path} does not exist."
            )

    return err_msgs


# def remove_list_values(d):
#     """"""
#     new_d = {}
#     for k, v in d.items():
#         if isinstance(v, abc.Mapping):
#             new_d.update(remove_list_values(d))
#         else:
#             new_d.update({k: v[0]})

#     return new_d


# def recursive_items(dictionary):
#     for key, value in dictionary.items():
#         if type(value) is dict:
#             yield from recursive_items(value)
#         else:
#             yield key, value


if __name__ == "__main__":
    from cprices.config import ScenarioConfig
    sc_config = ScenarioConfig('scenario_scan', subdir='scanner')
