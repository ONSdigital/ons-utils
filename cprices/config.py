"""Configuration file loader and validation functions."""
from collections import abc
from datetime import datetime
from logging.config import dictConfig
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Union
import yaml

from flatten_dict import flatten

from cprices import validation
from cprices.utils.helpers import (
    fill_tuples,
    fill_tuple_keys,
    get_key_value_pairs,
    is_non_string_sequence,
    list_convert,
)


class ConfigFormatError(Exception):
    """Exception raised when given config YAML is not in mapping format."""

    def __init__(self):
        """Init the ConfigFormatError."""
        super().__init__("attributes or config yaml must be a mapping")


class Config:
    """Base class for config files."""

    def __init__(
        self,
        filename: str,
        subdir: Optional[str] = None,
        to_unpack: Optional[Sequence[str]] = None,
    ):
        """Initialise the Config class.

        Parameters
        ----------
        filename : str,
            The filename of the config file without the YAML extension.
        subdir : str, optional
            The subdirectory within the config directory that contains
            the config file.
        to_unpack : sequence of str
            A list of keys that contain mappings to unpack. The mappings
            at given keys will be set as new attributes directly.
        """
        self.name = filename
        self.config_path = self.get_config_path(subdir)
        self.set_attrs(self.load_config(), to_unpack)

    def get_config_dir(self) -> Path:
        """Get the config directory from possible locations.

        Looks first to see if the environment variable CPRICES_CONFIG
        is set. If not cycles through current working directory, home
        directory, and cprices directories until it finds a folder
        called config which it returns.

        Returns
        -------
        Path
            The config directory.
        """
        config_dir_path_env = os.getenv("CPRICES_CONFIG")
        if config_dir_path_env:
            return Path(config_dir_path_env)

        for loc in (
            # This location is where the config is stored currently.
            Path.home().joinpath('cprices', 'cprices'),
            Path.home().joinpath('cprices'),
            Path.home(),
            Path.cwd(),
        ):
            if loc.joinpath('config').exists():
                return loc.joinpath('config')

    def get_config_path(self, subdir: Optional[str] = None) -> Path:
        """Return the path to the config file.

        Parameters
        ----------
        subdir : str, optional
            The subdirectory within the config directory that contains
            the config file.
        """
        filename = self.name + '.yaml'
        to_join = [filename] if not subdir else [subdir, filename]
        return self.get_config_dir().joinpath(*to_join)

    def load_config(self):
        """Load the config file."""
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    def update(self, attrs: Mapping[str, Any]):
        """Update the attributes."""
        for key, value in attrs.items():
            setattr(self, key, value)

    def set_attrs(
        self,
        attrs: Mapping[str, Any],
        to_unpack: Optional[Sequence[str]] = None,
    ):
        """Set attributes from given mapping using update method.

        Parameters
        ----------
        to_unpack : sequence of str
            A list of keys that contain mappings to unpack. Instructs
            the method to unpack mappings at given keys by setting them
            as new attributes directly.
        """
        if not isinstance(attrs, abc.Mapping):
            raise ConfigFormatError

        # Initialise to_unpack as empty list if not given.
        for attr in to_unpack if to_unpack else []:
            nested_mapping = attrs.pop(attr)
            if not isinstance(nested_mapping, abc.Mapping):
                raise TypeError(
                    f"given attr {attr} to unpack must be a mapping"
                )
            self.update(nested_mapping)

        self.update(attrs)

    def flatten_nested_dicts(self, attrs: Sequence[str]) -> None:
        """Flatten the nested dict config for web_scraped."""
        self.update({k: flatten(getattr(self, k)) for k in attrs})

    def get_key_value_pairs(self, attrs: Sequence[str]) -> None:
        """Get the key value pairs from a dictionary as list of tuples."""
        self.update({k: get_key_value_pairs(getattr(self, k)) for k in attrs})

    def fill_tuples(
        self,
        attrs: Sequence[str],
        repeat: bool = True,
        length: int = None,
    ) -> None:
        """Fill tuples so they are all the same length."""
        self.update({
            k: fill_tuples(getattr(self, k), repeat=repeat, length=length)
            for k in attrs
        })

    def fill_tuple_keys(
        self,
        attrs: Sequence[str],
        repeat: bool = True,
        length: int = None,
    ) -> None:
        """Fill tuple keys so they are all the same length."""
        self.update({
            k: fill_tuple_keys(getattr(self, k), repeat=repeat, length=length)
            for k in attrs
        })

    def extend_attr(self, attr: str, extend_vals: Sequence[Any]) -> None:
        """Extend a list or tuple attr with the given values."""
        current_vals = getattr(self, attr)

        if not is_non_string_sequence(current_vals):
            raise AttributeError(f'attribute {attr} is not an extendable type')
        elif isinstance(current_vals, tuple):
            extend_vals = tuple(extend_vals)
        elif isinstance(current_vals, list):
            extend_vals = list(extend_vals)

        setattr(self, attr, getattr(self, attr) + extend_vals)


class SelectedScenarioConfig(Config):
    """Class to store the selected scenarios."""

    def __init__(self, *args, to_unpack=['selected_scenarios'], **kwargs):
        """Init like config, then run .combine_input_data()."""
        super().__init__(*args, to_unpack=to_unpack, **kwargs)
        self.nones_to_empty_iterator()

    def nones_to_empty_iterator(self):
        """Constrain None values to empty iterators.

        This allows them to still be iterated over in main.py.
        """
        for attr, value in vars(self).items():
            if not value:
                setattr(self, attr, [])


class ScanScenarioConfig(Config):
    """Class with methods for scanner scenario configs."""

    def __init__(self, *args, subdir='scanner', **kwargs):
        """Init like config, then run .combine_input_data()."""
        super().__init__(*args, subdir=subdir, **kwargs)
        self.combine_input_data()

    def validate(self) -> str:
        """Validate the scenario config against the schema.

        Returns
        -------
        str
            An error message with all validation errors. Returns an
            empty string if no errors.
        """
        return validation.validate_scan_scenario_config(self)

    def combine_input_data(self) -> None:
        """Combine with supplier dict and without supplier list."""
        # First get key value pairs from the with supplier section.
        # Since it is a dict.
        with_supplier_inputs = get_key_value_pairs(
            self.input_data.get('with_supplier', {})
        )

        # Add the list, and fill the tuples to the same length.
        self.input_data = (
            with_supplier_inputs
            + self.input_data.get('without_supplier', [])
        )
        self.fill_tuples(['input_data'], repeat=True, length=2)

        return self


class WebScrapedScenarioConfig(Config):
    """Class with methods for web scraped scenario configs."""

    def __init__(self, *args, subdir='web_scraped', **kwargs):
        """Init like config, then run .combine_input_data()."""
        super().__init__(*args, subdir=subdir, **kwargs)
        self.flatten_nested_dicts(['consumption_segment_mappers'])
        self.get_key_value_pairs(['input_data'])

    def validate(self) -> str:
        """Validate the scenario config against the schema.

        Returns
        -------
        str
            An error message with all validation errors. Returns an
            empty string if no errors.
        """
        return validation.validate_webscraped_scenario_config(self)


class DevConfig(Config):
    """Class to store the dev config settings."""

    def add_strata(
        self,
        extra_strata: Union[str, Sequence[str]],
    ) -> None:
        """Add extra strata columns to DevConfig column list attributes.

        Adds to the grouping columns, the columns to read in from the
        tables, and the columns taken through from preprocessing.
        """
        column_attrs = [
            'strata_cols',
            'preprocess_cols',
            'data_cols',
        ]
        for attr in column_attrs:
            # Ensures that there are no duplicates of the user-specified
            # strata cols in the result of the subsequent extension, by
            # checking if they are already in the given attributes
            # above.
            missing_strata = [
                c for c in list_convert(extra_strata)
                if c not in getattr(self, attr)
            ]
            self.extend_attr(attr, missing_strata)


class LoggingConfig:
    """Class to set logging config."""

    def __init__(self):
        """Init the logging config object."""
        self.log_id = self.create_log_id()
        self.log_dir = self.get_logs_dir()
        self.filename = f'{self.log_id}.log'
        self.full_path = self.log_dir.joinpath(self.filename)

    def create_log_id(self) -> str:
        """Create the unique log ID from the current timestamp."""
        return 'log_' + datetime.now().strftime('%y%m%d_%H%M%S')

    def get_logs_dir(self) -> Path:
        """Return the logs directory."""
        loc = Path.home().joinpath('cprices', 'cprices')
        if loc.exists():
            return loc.joinpath('run_logs')

        return Path.home().joinpath('cprices_run_logs')

    def create_logs_dir(self) -> None:
        """Create the log directory if not already created."""
        self.get_logs_dir().mkdir(exist_ok=True)

    def set_logging_config(
        self,
        console: str,
        text_log: str,
        disable_other_loggers: bool = False,
    ) -> None:
        """Set the config for the logging module.

        Parameters
        ----------
        console : str
            Formatter ID for the console handler. Can be any string value.
        text_log : str
            Formatter for the log file handler.
        disable_other_loggers : bool, default False
            If True, disables any existing non-root loggers unless they
            or their ancestors are explicitly named in the logging
            configuration.
        """
        logging_config = {
            'version': 1,
            'loggers': {
                '': {  # root logger
                    'handlers': ['console', 'file_log'],
                    'level': 'INFO',
                    'propagate': False,
                },
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': console,
                    'level': 'INFO',
                },
                'file_log': {
                    'class': 'logging.FileHandler',
                    'formatter': text_log,
                    'level': 'DEBUG',
                    'mode': 'w',
                    'filename': self.full_path,
                },
            },
            'formatters': {
                'basic': {
                    'format': '%(message)s',
                },
                'debug': {
                    'format': '[%(asctime)s %(levelname)s - file=%(filename)s:%(lineno)d] %(message)s',
                    'datefmt': '%y/%m/%d %H:%M:%S',
                },
            },
            'disable_existing_loggers': disable_other_loggers,
        }
        dictConfig(logging_config)
