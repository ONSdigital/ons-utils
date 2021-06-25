"""Configuration file loader and validation functions."""
from datetime import datetime
from logging.config import dictConfig
from pathlib import Path
from typing import Mapping, Any
import yaml

from cprices import validation

SRC_DIR = Path(__file__).parent
ROOT_DIR = SRC_DIR.parent
CONFIG_DIR = ROOT_DIR.joinpath('config')


class Config:
    """Base class for config files."""

    def __init__(self, filename: str):
        """Initialise the Config class."""
        self.name = filename

    def get_config_path(self):
        """Return the path to the config file."""
        return CONFIG_DIR.joinpath(self.name + '.yaml')

    def load_config(self):
        """Load the config file."""
        with open(self.get_config_path(), 'r') as f:
            return yaml.safe_load(f)

    def update(self, attrs: Mapping[str, Any]):
        """Update the attributes."""
        for key, value in attrs.items():
            setattr(self, key, value)


class SelectedScenarioConfig(Config):
    """Class to store the selected scenarios."""

    def __init__(self, filename: str):
        """Init the selected scenarios config."""
        super().__init__(filename)
        self.selected_scenarios = self.config['selected_scenarios']


class ScenarioConfig(Config):
    """Class to store the configuration settings for particular scenario."""

    def __init__(self, scenario: str):
        """Init the scenario config."""
        super().__init__(scenario)
        self.update(self.load_config())

    def validate(self):
        """Validate the scenario config against the schema."""
        validation.validate_config(self)


class DevConfig(Config):
    """Class to store the dev config settings."""

    def __init__(self, filename: str):
        """Init the developer config."""
        super().__init__(filename)
        config = self.load_config()
        self.update(config.pop('directories'))
        self.analysis_params = (config.pop('analysis_params'))
        self.update(config)


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
        return ROOT_DIR.joinpath('run_logs')

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
