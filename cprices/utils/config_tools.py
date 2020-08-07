# import python libraries
import ast
from datetime import datetime
from logging.config import dictConfig
import os
import yaml

from epds_utils import hdfs


class SelectedScenarioConfig:
    """
    This class stores the selected scenarios
    """
    def __init__(self, root_dir):

        with open(os.path.join(root_dir, 'config', 'scenarios.yaml'), 'r') as f:
            config = yaml.safe_load(f)

        self.selected_scenarios = config['selected_scenarios']



class ScenarioConfig:
    """
    This class stores the configuration settings for a particular scenario
    """
    def __init__(self, config_path):

        def iterdict(d):
            for k, v in d.items():
                if isinstance(v, dict):
                    iterdict(v)
                else:
                    if type(v) == str:
                        v = eval(v)
                    d.update({k: v})
            return d

        with open(config_path,'r') as f:
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
            'drop_retailers': config['preprocessing']['drop_retailers']
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
            'index_methods': config['indices']['methods'],
            'splice_window_length': config['indices']['splice_window_length'],
        }


class DevConfig:
    """
    This class stores the dev config settings
    """
    def __init__(self, root_dir):

        with open(os.path.join(root_dir, 'config', 'dev_config.yaml'), 'r') as f:
            config = yaml.safe_load(f)

        self.groupby_cols = config['groupby_cols']
        self.staged_dir = config['directories']['staged_dir']
        self.processed_dir = config['directories']['processed_dir']
        self.test_dir = config['directories']['test_dir']
        self.mappers_dir = config['directories']['mappers_dir']
        self.multi_item_datasets = config['multi_item_datasets']
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
                'handlers': ['console','file_log'],
                'level': 'INFO',
                'propagate': False,
            },
        },
    }
    dictConfig(logging_config)

    return log_id


def check_params(root_dir, selected_scenarios):

    """
    This function runs at the very beginning and includes assert statements
    to ensure that the config parameters are valid, i.e. they have the right
    data type and values within the permitted range.
    """

    for scenario in selected_scenarios:

        # import and reload config file for scenario
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

        # Preprocessing
        try:
            datetime.strptime(validating_config.preprocessing['start_date'], "%Y-%m-%d")
        except:
            raise ValueError("{scenario}: Datestamp in incorrect format - must be 'YYYY-mm-dd'")

        try:
            datetime.strptime(validating_config.preprocessing['end_date'], "%Y-%m-%d")
        except:
            raise ValueError("{scenario}: Datestamp in incorrect format - must be 'YYYY-mm-dd'")


        # Classification
        mapper_settings = validating_config.classification['mapper_settings']
        msg = "{scenario}: parameter 'active' in classification, web_scraped is not a boolean"
        assert isinstance(validating_config.classification['web_scraped_active'], bool), msg
        for data_source in mapper_settings:
            for supplier in mapper_settings[data_source]:
                if data_source == 'scanner':
                        msg = f"{scenario}: parameter 'user_defined_mapper' in classification, mapper_settings, {data_source} {supplier} is not a boolean"
                        assert isinstance(mapper_settings[data_source][supplier]['user_defined_mapper'], bool), msg

                        if mapper_settings[data_source][supplier]['mapper_path'] == None:
                            msg = f"{scenario}:\n The {data_source} {supplier} config setting \"user_defined_mapper\" " \
                                "is set to True but no path is provided.\n Please provide a path or set " \
                                "\"user_defined_mapper\" to False"
                            raise Exception(msg)

                        if ('user_defined_mapper' in mapper_settings[data_source][supplier]) \
                        and (mapper_settings[data_source][supplier]['user_defined_mapper']):
                            # check user defined mapper exists
                            if not hdfs.test(mapper_settings[data_source][supplier]['mapper_path']):
                                raise Exception(f"{scenario}: {data_source} {supplier} user defined mapper ({mapper_settings[data_source][supplier]['mapper_path']}) does not exist")

                if data_source == 'web_scraped':
                    for item in mapper_settings[data_source][supplier]:
                        msg = f"{scenario}: parameter 'user_defined_mapper' in classification, mapper_settings, {data_source} {supplier} {item} is not a boolean"
                        assert isinstance(mapper_settings[data_source][supplier][item]['user_defined_mapper'], bool), msg

                        if mapper_settings[data_source][supplier][item]['mapper_path'] == None:
                            msg = f"{scenario}:\n The {data_source} {supplier} config setting \"user_defined_mapper\" " \
                                "is set to True but no path is provided.\n Please provide a path or set " \
                                "\"user_defined_mapper\" to False"
                            raise Exception(msg)

                        if ('user_defined_mapper' in mapper_settings[data_source][supplier][item]) \
                        and (mapper_settings[data_source][supplier][item]['user_defined_mapper']):
                            # check user defined mapper exists
                            if not hdfs.test(mapper_settings[data_source][supplier][item]['mapper_path']):
                                raise Exception(f"{scenario}: {data_source} {supplier} {item} user defined mapper ({mapper_settings[data_source][supplier][item]['mapper_path']}) does not exist.")

        # Outlier detection
        msg = "{scenario}: parameter 'active' in outlier_detection is not a boolean"
        isbool = isinstance(validating_config.outlier_detection['active'], bool)
        assert isbool, msg
        if isbool:
            if validating_config.outlier_detection['active']:
                msg = "{scenario}: parameter 'log_transform' in outlier_detection is not a boolean"
                assert isinstance(validating_config.outlier_detection['log_transform'], bool), msg

                method = validating_config.outlier_detection['method']
                msg = ("{scenario}: the outlier detection method must be a string " +
                       "among 'tukey', 'kimber', 'ksigma', 'udf_fences'")
                assert isinstance(method, str), msg
                assert method in ['tukey', 'kimber', 'ksigma', 'udf_fences'], msg

                k = validating_config.outlier_detection['k']
                msg = '{scenario}: k for outlier detection must be a float between 1 and 4'
                assert isinstance(k, float), msg
                assert (k>=1)&(k<=4), msg


        # Averaging
        methods = [
            'unweighted_arithmetic',
            'unweighted_geometric',
            'weighted_arithmetic',
            'weighted_geometric'
        ]
        for data_source in ['web_scraped', 'scanner']:
            method = validating_config.averaging[data_source]
            msg = (f"{scenario}: the averaging method for {data_source} must be a string among {methods}")
            assert isinstance(method, str), msg
            assert method in methods, msg


        # Grouping
        msg = "{scenario}: parameter 'active' in grouping is not a boolean"
        assert isinstance(validating_config.grouping['active'], bool), msg


        # Imputation
        msg = "{scenario}: parameter 'active' in imputation is not a boolean"
        isbool = isinstance(validating_config.imputation['active'], bool)
        assert isbool, msg
        if isbool:
            if validating_config.imputation['active']:
                ffill_limit = validating_config.imputation['ffill_limit']
                msg = '{scenario}: ffill_limit for imputation must be an integer greater than 0'
                assert isinstance(ffill_limit, int), msg
                assert (ffill_limit>0), msg


        # Filtering
        msg = "{scenario}: parameter 'active' in filtering is not a boolean"
        isbool = isinstance(validating_config.filtering['active'], bool)
        assert isbool, msg
        if isbool:
            if validating_config.filtering['active']:
                mcs = params['filtering']['max_cumsum_share']
                msg = '{scenario}: max_cumsum_share for filtering must be a float between 0 and 1'
                assert isinstance(mcs, float), msg
                assert (mcs>=0)&(mcs<=1), msg


        # Indices
        d = validating_config.indices['splice_window_length']
        msg = '{scenario}: splice window length for rygeks must be a positive integer'
        assert isinstance(d, int), msg
        assert d>0, msg
