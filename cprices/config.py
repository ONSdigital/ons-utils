"""Configuration file loader and validation functions."""
from collections import abc
from copy import copy, deepcopy
from datetime import datetime
from logging.config import dictConfig
import os
from pathlib import Path
from typing import Mapping, Any, Sequence, Optional
import yaml

from flatten_dict import flatten

from cprices import validation
from cprices.utils.helpers import (
    fill_tuples,
    fill_tuple_keys,
    get_key_value_pairs,
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
        to_unpack: Optional[Sequence[str]] = None,
    ):
        """Initialise the Config class.

        to_unpack : sequence of str
            A list of keys that contain mappings to unpack. The mappings
            at given keys will be set as new attributes directly.
        """
        self.name = filename
        self.config_path = self.get_config_path()
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

    def get_config_path(self) -> Path:
        """Return the path to the config file."""
        return self.get_config_dir().joinpath(self.name + '.yaml')

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
        self.update({k: flatten(vars(self)[k]) for k in attrs})

    def get_key_value_pairs(self, attrs: Sequence[str]) -> None:
        """Get the key value pairs from a dictionary as list of tuples."""
        self.update({k: get_key_value_pairs(vars(self)[k]) for k in attrs})

    def fill_tuples(
        self,
        attrs: Sequence[str],
        repeat: bool = True,
        length: int = None,
    ) -> None:
        """Fill tuples so they are all the same length."""
        self.update({
            k: fill_tuples(vars(self)[k], repeat=repeat, length=length)
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
            k: fill_tuple_keys(vars(self)[k], repeat=repeat, length=length)
            for k in attrs
        })


class SelectedScenarioConfig(Config):
    """Class to store the selected scenarios."""


class ScenarioConfig(Config):
    """Class to store the configuration settings for particular scenario."""

    def validate(self):
        """Validate the scenario config against the schema."""
        validation.validate_config(self)

    def pick_source(self, source: str) -> 'ScenarioConfig':
        """Select the config parameters for the given source.

        Parameters
        ----------
        source : {'web_scraped', 'scanner'}, str
            The data source to pick the config for.

        Returns
        -------
        Config
            An altered version of the main scenario config for the data
            source.
        """
        if source not in {'web_scraped', 'scanner'}:
            raise ValueError("source must be 'web_scraped' or 'scanner'")

        new_config = copy(self)

        for key, value in vars(new_config).items():
            if isinstance(value, dict):
                if value.get(source, None):
                    setattr(new_config, key, value.get(source))

        if source == 'web_scraped':
            # Flattens nested to (supplier, item) tuple.
            new_config.flatten_nested_dicts(['item_mappers'])
            # Converts dict to (suppler, item) tuple pairs.
            new_config.get_key_value_pairs(['input_data'])

        if source == 'scanner':
            new_config.combine_scanner_input_data()

        return new_config

    def combine_scanner_input_data(self) -> 'ScenarioConfig':
        """Combine with supplier dict and without supplier list."""
        scan_input_data = deepcopy(self.input_data)

        # First get key value pairs from the with supplier section.
        # Since it is a dict.
        self.input_data = scan_input_data.get('with_supplier', [])
        if self.input_data:
            self.get_key_value_pairs(['input_data'])

        # Add the list, and fill the tuples to the same length.
        self.input_data += scan_input_data.get('without_supplier', [])
        self.fill_tuples(['input_data'], repeat=True, length=2)

        return self


class DevConfig(Config):
    """Class to store the dev config settings."""


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


if __name__ == "__main__":
    sc_config = ScenarioConfig('scenario_scan')
    print(sc_config.pick_source('scanner').input_data)
