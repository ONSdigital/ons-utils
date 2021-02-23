"""Configuration file loader and validation functions."""
from datetime import datetime
from logging.config import dictConfig
import os
import yaml

from cerberus import Validator

from epds_utils import hdfs


class SelectedScenarioConfig:
    """Class to store the selected scenarios."""

    def __init__(self, root_dir):
        """Init the selected scenarios config."""
        with open(os.path.join(root_dir, 'config', 'scenarios.yaml'), 'r') as f:
            config = yaml.safe_load(f)

        self.selected_scenarios = config['selected_scenarios']


class ScenarioConfig:
    """Class to store the configuration settings for particular scenario."""

    def __init__(self, config_path):
        """Init the scenario config."""
        def iterdict(d):
            for k, v in d.items():
                if isinstance(v, dict):
                    iterdict(v)
                else:
                    if type(v) == str:
                        v = eval(v)
                    d.update({k: v})
            return d

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)


#        config['input_data']['web_scraped'] = iterdict(config['input_data']['web_scraped'])

        self.input_data = {}
        for input_data in config['input_data'].keys():
            if input_data == 'web_scraped':
                self.input_data[input_data] = iterdict(config['input_data'][input_data])
            else:
                self.input_data[input_data] = config['input_data'][input_data]

        self.preprocessing = {
            'start_date': str(config['preprocessing']['start_date']),
            'end_date': str(config['preprocessing']['end_date']),
            'drop_retailers': config['preprocessing']['drop_retailers'],
            'calc_p_and_q_using_size': config['preprocessing']['calc_p_and_q_using_size'],
            'scanner_expenditure_column': config['preprocessing']['scanner_expenditure_column'],
            'add_promo': config['preprocessing']['add_promo'],
        }

        self.classification = {
            'web_scraped_active': config['classification']['web_scraped_active'],
            'mapper_settings': config['classification']['mapper_settings']
        }

        self.outlier_detection = {
            'fences': config['outlier_detection']['fences'],
            'active': config['outlier_detection']['active'],
            'log_transform': config['outlier_detection']['log_transform'],
            'method': config['outlier_detection']['method'],
            'k': config['outlier_detection']['k'],
        }

        self.averaging = {
            'scanner': config['averaging']['scanner'],
            'web_scraped': config['averaging']['web_scraped'],
        }

        self.grouping = {
            'active': config['grouping']['active'],
            'scanner': config['grouping']['scanner'],
            'web_scraped': config['grouping']['web_scraped'],
        }

        self.imputation = {
            'active': config['imputation']['active'],
            'ffill_limit': config['imputation']['ffill_limit']
        }

        self.filtering = {
            'active': config['filtering']['active'],
            'max_cumsum_share': config['filtering']['max_cumsum_share']
        }

        self.indices = {
            'base_price_methods': config['indices']['base_price_methods'],
            'base_period': config['indices']['base_period'],
            'index_methods': config['indices']['index_methods'],
            'multilateral_methods': config['indices']['multilateral_methods'],
            'window': config['indices']['window_length'],
        }


class DevConfig:
    """Class to store the dev config settings."""

    def __init__(self, root_dir):
        """Init the developer config."""
        with open(os.path.join(root_dir, 'config', 'dev_config.yaml'), 'r') as f:
            config = yaml.safe_load(f)

        self.groupby_cols = config['groupby_cols']
        self.staged_dir = config['directories']['staged_dir']
        self.processed_dir = config['directories']['processed_dir']
        self.test_dir = config['directories']['test_dir']
        self.mappers_dir = config['directories']['mappers_dir']
        self.conventional_data_columns = config['conventional_data_columns']
        self.scanner_input_tables = config['scanner_input_tables']
        self.scanner_data_columns = config['scanner_data_columns']
        self.webscraped_data_columns = config['webscraped_data_columns']
        self.multi_item_datasets = config['multi_item_datasets']
        self.multi_item_sep_char = config['multi_item_sep_char']
        self.logging_config = config['logging_config']
        self.analysis_params = {}
        for analysis_param in config['analysis_params'].keys():
            self.analysis_params[analysis_param] = config['analysis_params'][analysis_param]


def logging_config(
    root_dir: str,
    disable_other_loggers: bool = False
):
    """Create dictionary to configure logging.

    Parameters
    ----------
    config_dir: string
        - path to dev conf

    disable_other_loggers: bool
        - setting this to True allows to overwrite the current
          logger within the same session.

    Returns
    -------
    log_id: string
        log_id string which is a timestamp of when the logging was created.
        this is used give the log file a unique name.

    """
    dev_config = DevConfig(root_dir)
    log_config = dev_config.logging_config

    # Set logging config
    log_id = 'log_' + datetime.now().strftime('%y%m%d_%H%M%S')

    # create dir to keeps log files
    logging_dir = os.path.join(root_dir, 'run_logs')
    if not os.path.exists(logging_dir):
        os.mkdir(logging_dir)

    logging_config = {
        'version': 1,
        'disable_existing_loggers': disable_other_loggers,
        'formatters': {
            'basic': {
                'format': '%(message)s',
            },
            'debug': {
                'format': '[%(asctime)s %(levelname)s - file=%(filename)s:%(lineno)d] %(message)s',
                'datefmt': '%y/%m/%d %H:%M:%S',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': log_config['console'],
                'level': 'INFO',
            },
            'file_log': {
                'class': 'logging.FileHandler',
                'formatter': log_config['text_log'],
                'level': 'DEBUG',
                'mode': 'w',
                'filename': os.path.join(logging_dir, f'{log_id}.log'),
            },
        },
        'loggers': {
            '': {  # root logger
                'handlers': ['console', 'file_log'],
                'level': 'INFO',
                'propagate': False,
            },
        },
    }
    dictConfig(logging_config)

    return log_id


def check_params(root_dir: str, selected_scenarios: list) -> None:
    """Validate scenario config parameters.

    This function runs at the very beginning and includes assert statements
    to ensure that the config parameters are valid, i.e. they have the right
    data type and values within the permitted range.
    """
    for scenario in selected_scenarios:
        # Import and reload config file for scenario.
        validating_config = ScenarioConfig(os.path.join(root_dir, 'config', f'{scenario}.yaml'))

        required_keys = [
            'input_data',
            'preprocessing',
            'classification',
            'outlier_detection',
            'averaging',
            'grouping',
            'imputation',
            'filtering',
            'indices'
        ]

        for key in required_keys:
            if key not in validating_config.__dict__.keys():
                msg = f'{scenario}: {key} does not appear among the config parameters.'
                raise Exception(msg)

        outlier_methods_list = [
            'tukey',
            'kimber',
            'ksigma',
            'udf_fences'
        ]

        averaging_methods_list = [
            'unweighted_arithmetic',
            'unweighted_geometric',
            'weighted_arithmetic',
            'weighted_geometric'
        ]

        base_price_methods_list = [
            'fixed_base',
            'chained',
            'bilateral',
            'fixed_base_with_rebase',
        ]

        index_methods_list = [
            'carli',
            'jevons',
            'dutot',
            'laspeyres',
            'paasche',
            'fisher',
            'tornqvist',
        ]

        multilateral_methods_list = [
            'geks',
            'rygeks',
        ]

        v = Validator()
        v.schema = {
            # Preprocessing
            'start_date': {'type': 'string', 'regex': r'([12]\d{3}-(0[1-9]|1[0-2])-01)'},
            'end_date': {'type': 'string', 'regex': r'([12]\d{3}-(0[1-9]|1[0-2])-01)'},
            'drop_retailers': {'type': 'boolean'},
            'add_promo': {'type': 'integer', 'min': 0, 'max': 2},
            # Classification
            'web_scraped_active': {'type': 'boolean'},
            'user_defined_mapper': {'type': 'boolean'},
            # Outlier detection/ Averaging/ Grouping/ Filtering/ Imputation
            'active': {'type': 'boolean'},
            # Outlier detection
            'log_transform': {'type': 'boolean'},
            'outlier_methods': {'type': 'string', 'allowed': outlier_methods_list},
            'k': {'type': 'float', 'min': 1, 'max': 4},
            'fence_value': {'type': 'float'},
            # Averaging/ Grouping
            'web_scraped': {'type': 'string', 'allowed': averaging_methods_list},
            'scanner': {'type': 'string', 'allowed': averaging_methods_list},
            # Imputation
            'ffill_limit': {'type': 'integer', 'min': 1},
            # Imputation
            'max_cumsum_share': {'type': 'float', 'min': 0, 'max': 1},
            # Indices
            'base_price_methods': {'type': 'list', 'allowed': base_price_methods_list, 'nullable': True},
            'index_methods': {'type': 'list', 'allowed': index_methods_list},
            'multilateral_methods': {'type': 'list', 'allowed': multilateral_methods_list, 'nullable': True},
            'base_period': {'type': 'integer', 'min': 1, 'max': 12},
            'window': {'type': 'integer', 'min': 0},
        }


        # Preprocessing
        if not v.validate({'start_date': validating_config.preprocessing['start_date']}):
            raise ValueError(
                f"{scenario}: parameter 'start_date' in preprocessing must be"
                " a string in the format YYYY-MM-01"
            )

        if not v.validate({'end_date': validating_config.preprocessing['end_date']}):
            raise ValueError(
                f"{scenario}: parameter 'end_date' in preprocessing must be"
                " a string in the format YYYY-MM-01"
            )

        if not v.validate({'drop_retailers': validating_config.preprocessing['drop_retailers']}):
            raise ValueError(
                f"{scenario}: parameter 'drop_retailers' in preprocessing must be a boolean"
            )


        # Classification
        mapper_settings = validating_config.classification['mapper_settings']
        if not v.validate({'web_scraped_active': validating_config.classification['web_scraped_active']}):
            raise ValueError(f"{scenario}: parameter 'active' in classification, web_scraped is not a boolean")

        for data_source in mapper_settings:
            for supplier in mapper_settings[data_source]:

                def classification_validation(classification_mapper_dict, mapper_source):
                    if not v.validate({'user_defined_mapper': classification_mapper_dict['user_defined_mapper']}):
                        raise ValueError(
                            f"{scenario}: parameter 'user_defined_mapper' in classification,"
                            " mapper_settings, {mapper_source} is not a boolean"
                        )

                    # convert to cerberus?
                    if classification_mapper_dict['mapper_path'] is None:
                        raise Exception(
                            f"{scenario}:\n The {mapper_source} config setting \"user_defined_mapper\" "
                            "is set to True but no path is provided.\n Please provide a path or set "
                            "\"user_defined_mapper\" to False"
                        )

                    if ('user_defined_mapper' in classification_mapper_dict) \
                            and (classification_mapper_dict['user_defined_mapper']):
                        # check user defined mapper exists
                        if not hdfs.test(classification_mapper_dict['mapper_path']):
                            raise Exception(
                                f"{scenario}: {mapper_source} user defined mapper"
                                f" ({classification_mapper_dict['mapper_path']}) does not exist"
                            )

                if data_source == 'scanner':
                    mapper_source = f'{data_source}, {supplier}'
                    classification_validation(mapper_settings[data_source][supplier], mapper_source)

                if data_source == 'web_scraped':
                    for item in mapper_settings[data_source][supplier]:
                        mapper_source = f'{data_source}, {supplier}, {item}'
                        classification_validation(mapper_settings[data_source][supplier][item], mapper_source)


        # Outlier detection
        if v.validate({'active': validating_config.outlier_detection['active']}):
            if not v.validate({'log_transform': validating_config.outlier_detection['log_transform']}):
                raise ValueError(
                    f"{scenario}: parameter 'log_transform' in outlier_detection must be a boolean"
                )

            if not v.validate({'outlier_methods': validating_config.outlier_detection['method']}):
                raise ValueError(
                    f"{scenario}: parameter 'method' for outlier_detection must be a"
                    f" string among {outlier_methods_list}")

            if not v.validate({'k': validating_config.outlier_detection['k']}):
                raise ValueError(
                    f"{scenario}: parameter 'k' for outlier detection must be a float between 1 and 4"
                )
        else:
            raise ValueError(f"{scenario}: parameter 'active' in outlier_detection is not a boolean")

        for fence_dictionary in [validating_config.outlier_detection['fences']]:
            for fence_list in fence_dictionary.values():
                for list_value in fence_list:
                    if not v.validate({'fence_value': list_value}):
                        raise ValueError(f"{scenario}: value {list_value} in fences in outlier detection must be a float")


        # Averaging
        for data_source in ['web_scraped', 'scanner']:
            if not v.validate({data_source: validating_config.averaging[data_source]}):
                raise ValueError(
                    f"{scenario}: parameter {data_source} in averaging must be"
                    f" a string among {averaging_methods_list}"
                )


        # Grouping
        if v.validate({'active': validating_config.grouping['active']}):
            if validating_config.grouping['active']:
                for data_source in ['web_scraped', 'scanner']:
                    if not v.validate({data_source: validating_config.grouping[data_source]}):
                        raise ValueError(
                            f"{scenario}: {data_source} for grouping must be a string"
                            f" among {averaging_methods_list}"
                        )
        else:
            raise ValueError(f"{scenario}: parameter 'active' in grouping is not a boolean")


        # Imputation
        if v.validate({'active': validating_config.imputation['active']}):
            if validating_config.imputation['active']:
                if not v.validate({'ffill_limit': validating_config.imputation['ffill_limit']}):
                    raise ValueError(
                        f"{scenario}: ffill_limit for imputation must be an integer greater than 0"
                    )
        else:
            raise ValueError(f"{scenario}: parameter 'active' in imputation is not a boolean")


        # Filtering
        if v.validate({'active': validating_config.filtering['active']}):
            if validating_config.filtering['active']:
                if not v.validate({'max_cumsum_share': validating_config.filtering['max_cumsum_share']}):
                    raise ValueError(
                        f"{scenario}: max_cumsum_share in filtering must be a float between 0 and 1"
                    )
        else:
            raise ValueError(f"{scenario}: parameter 'active' in filtering is not a boolean")


        # Indices
        if not v.validate({'base_price_methods': validating_config.indices['base_price_methods']}):
            raise ValueError(
                f"{scenario}: parameter 'base_price_methods' in indices must be a list,"
                f" only containing values among {base_price_methods_list}",
            )
        if not v.validate({'index_methods': validating_config.indices['index_methods']}):
            raise ValueError(
                f"{scenario}: parameter 'index_methods' in indices must be a list,"
                f" only containing values among {index_methods_list}",
            )
        if not v.validate({'multilateral_methods': validating_config.indices['multilateral_methods']}):
            raise ValueError(
                f"{scenario}: parameter 'multilateral_methods' in indices must be a list,"
                f" only containing values among {multilateral_methods_list}",
            )

        if not v.validate({'window': validating_config.indices['window']}):
            raise ValueError(f"{scenario}: parameter 'window' in indices must be a positive integer")

        if not v.validate({'base_period': validating_config.indices['base_period']}):
            raise ValueError(
                f"{scenario}: parameter 'base_period' in indices must be"
                " an integer representing a month between 1 and 12 inclusive."
            )

        if not (
            validating_config.indices['base_price_methods']
            or validating_config.indices['multilateral_methods']
        ):
            raise ValueError(
                "One of either 'base_price_methods' or 'multilateral_methods'"
                " must be provided. They can't both be None."
            )
