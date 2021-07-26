"""Validation rules for config files."""
from typing import Mapping, Union
from cerberus import Validator

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


def validate_config_sections(config) -> None:
    """Validate that all required sections are in the config.name file."""
    required_sections = [
        'input_data',
        # 'preprocessing',
        # 'item_mappers',
        'outlier_detection',
        'averaging',
        'grouping',
        'imputation',
        'flag_low_expenditures',
        'indices'
    ]

    config_sections = vars(config).keys()

    for key in required_sections:
        if key not in config_sections:
            raise ScenarioSectionError(config.name, key)


def validate_config(config) -> None:
    """Validate config.name config parameters.

    This function runs at the very beginning and includes assert statements
    to ensure that the config parameters are valid, i.e. they have the right
    data type and values within the permitted range.
    """
    if config.preprocessing:
        validate_preprocessing(config)
    validate_classification(config)
    validate_outlier_detection(config)
    validate_averaging_and_grouping(config)
    validate_imputation(config)
    validate_flag_low_expenditures(config)
    validate_indices(config)


def validate_preprocessing(config) -> None:
    """Validate the preprocessing settings in the config."""
    expenditure_cols = {
        'sales_value_inc_discounts',
        'sales_value_excl_markdowns',
        'sales_value_vat',
        'sales_value_vat_excl_markdowns',
    }

    v = Validator()
    v.schema = {
        # Preprocessing
        'start_date': {
            'type': 'string',
            'regex': r'([12]\d{3}-(0[1-9]|1[0-2])-01)',
        },
        'end_date': {
            'type': 'string',
            'regex': r'([12]\d{3}-(0[1-9]|1[0-2])-01)',
        },
        # Scanner preprocessing
        'use_unit_prices': {'type': 'boolean'},
        'product_id_code_col': {
            'type': 'string',
            'allowed': ['gtin', 'retail_line_code'],
        },
        'calc_price_before_discount': {'type': 'boolean'},
        'promo_col': {
            'type': 'string',
            'allowed': ['price_promo_discount', 'multi_promo_discount'],
        },
        'sales_value_col': {
            'type': 'string',
            'allowed': expenditure_cols,
        },
        'align_daily_frequency': {
            'type': 'string',
            'allowed': ['weekly', 'monthly'],
        },
        'week_selection': {
            'type': 'list',
            'allowed': [1, 2, 3, 4],
            'nullable': True,
        },
    }

    if not v.validate({'start_date': config.preprocessing['start_date']}):
        raise ValueError(
            f"{config.name}: parameter 'start_date' in preprocessing"
            "must be a string in the format YYYY-MM-01."
        )

    if not v.validate({'end_date': config.preprocessing['end_date']}):
        raise ValueError(
            f"{config.name}: parameter 'end_date' in preprocessing"
            " must be a string in the format YYYY-MM-01."
        )

    # Scanner preprocessing
    to_validate = config.preprocessing['use_unit_prices']
    if not v.validate({'use_unit_prices': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'use_unit_prices' in"
            " preprocessing must a boolean."
            f" Instead got '{to_validate}'."
        )

    to_validate = config.preprocessing['product_id_code_col']
    if not v.validate({'product_id_code_col': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'product_id_code_col' in"
            " preprocessing must be one of: {'gtin', 'retail_line_code'}."
            f" Instead got '{to_validate}'."
        )

    to_validate = config.preprocessing['calc_price_before_discount']
    if not v.validate({'calc_price_before_discount': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'calc_price_before_discount' in"
            " preprocessing must be a boolean."
            f" Instead got '{to_validate}'."
        )

    to_validate = config.preprocessing['promo_col']
    if not v.validate({'promo_col': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'promo_col' in preprocessing"
            "must be one of: {'price_promo_discount', 'multi_promo_discount'}."
            f" Instead got '{to_validate}'."
        )

    to_validate = config.preprocessing['sales_value_col']
    if not v.validate({'sales_value_col': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'sales_value_col' in"
            f" preprocessing must be one of {expenditure_cols}."
            f" Instead got '{to_validate}'."
        )

    to_validate = config.preprocessing['align_daily_frequency']
    if not v.validate({'align_daily_frequency': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'align_daily_frequency' in"
            " preprocessing must be one of: {'weekly', 'monthly'}"
            f" Instead got '{to_validate}'."
        )

    to_validate = config.preprocessing['week_selection']
    if not v.validate({'week_selection': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'week_selection' in preprocessing"
            " should be a combination of integers [1, 2, 3, 4]."
            f" Instead got '{to_validate}'."
        )


def validate_classification(config) -> None:
    """Validate the classification settings in the config."""
    v = Validator()
    v.schema = {'web_scraped_active': {'type': 'boolean'}}

    # Classification
    to_validate = config.classification['web_scraped_active']
    if not v.validate({'web_scraped_active': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'web_scraped_active' in classification"
            " must be a boolean."
            f" Instead got '{to_validate}'."
        )

    mapper_settings = config.classification['mapper_settings']
    for data_source, d1 in mapper_settings.items():
        for supplier, d2 in d1.items():
            if data_source == 'scanner':
                settings = d2
                mapper_source = f'{data_source}, {supplier}'
                validate_mapper_settings(settings, config.name, mapper_source)

            # Webscraped needs to loop through items too.
            if data_source == 'web_scraped':
                for item, settings in d2.items():
                    mapper_source = f'{data_source}, {supplier}, {item}'
                    validate_mapper_settings(settings, config.name, mapper_source)


def validate_mapper_settings(
    mapper_settings: Mapping[str, Union[str, bool]],
    scenario_name: str,
    mapper_source: str,
) -> None:
    """Validate the mapper settings in the classification section."""
    v = Validator()
    v.schema = {'user_defined_mapper': {'type': 'boolean'}}

    mapper_switch = mapper_settings['user_defined_mapper']
    mapper_path = mapper_settings['mapper_path']

    if not v.validate({'user_defined_mapper': mapper_switch}):
        raise ValueError(
            f"{scenario_name}: parameter 'user_defined_mapper' in classification"
            " mapper settings, for {mapper_source} must be a boolean."
            f" Instead got '{mapper_switch}'."
        )

    if mapper_switch:
        # Raise exception if the path isn't supplied or doesn't exist.
        if not mapper_path:
            raise Exception(
                f"{scenario_name}:\n The {mapper_source} config setting"
                " 'user_defined_mapper' is set to True but no path is provided."
                " Please provide a path or set 'user_defined_mapper' to False."
            )
        else:
            if not hdfs.test(mapper_path):
                raise Exception(
                    f"{scenario_name}: {mapper_source} user defined mapper"
                    f" {mapper_path} does not exist."
                )


def validate_outlier_detection(config):
    """Validate the outlier detection settings in the config."""
    outlier_methods = {'tukey', 'kimber', 'ksigma'}

    v = Validator()
    v.schema = {
        # Outlier detection/ Averaging/ Grouping/ Filtering/ Imputation
        'active': {'type': 'boolean'},
        'log_transform': {'type': 'boolean'},
        'outlier_methods': {
            'type': 'string',
            'allowed': outlier_methods
        },
        'k': {
            'type': 'float',
            'min': 1,
            'max': 4,
        },
        'fence_value': {'type': 'float'},
        'stddev_method': {
            'type': 'string',
            'allowed': ['population', 'sample'],
        },
        'quartile_method': {
            'type': 'string',
            'allowed': ['exact', 'approx'],
        },
        'accuracy': {
            'type': 'float',
            'min': 1,
        },
    }

    active = config.outlier_detection['active']
    if not v.validate({'active': active}):
        raise ValueError(
            f"{config.name}: parameter 'active' in outlier_detection"
            " must be a boolean."
            f" Instead got '{active}'."
        )

    # If active is True, validate the rest.
    if active:
        to_validate = config.outlier_detection['log_transform']
        if not v.validate({'log_transform': to_validate}):
            raise ValueError(
                f"{config.name}: parameter 'log_transform' in"
                " outlier_detection must be a boolean."
                f" Instead got '{to_validate}'."
            )

        to_validate = config.outlier_detection['method']
        if not v.validate({'outlier_methods': to_validate}):
            raise ValueError(
                f"{config.name}: parameter 'method' for outlier detection"
                f" must be one of {outlier_methods}."
                f" Instead got '{to_validate}'."
            )

        to_validate = config.outlier_detection['k']
        if not v.validate({'k': to_validate}):
            raise ValueError(
                f"{config.name}: parameter 'k' for outlier detection"
                " must be a float between 1 and 4."
                f" Instead got '{to_validate}'."
            )

        to_validate = config.outlier_detection['stddev_method']
        if not v.validate({'stddev_method': to_validate}):
            raise ValueError(
                f"{config.name}: parameter 'stddev_method' for outlier"
                " detection must be one of {'population', 'sample'}."
                f" Instead got '{to_validate}'."
            )

        to_validate = config.outlier_detection['quartile_method']
        if not v.validate({'quartile_method': to_validate}):
            raise ValueError(
                f"{config.name}: parameter 'quartile_method' for outlier"
                " detection must be one of {'exact', 'approx'}."
                f" Instead got '{to_validate}'."
            )

        to_validate = config.outlier_detection['quartile_method']
        if not v.validate({'accuracy': to_validate}):
            raise ValueError(
                f"{config.name}: parameter 'accuracy' for outlier"
                " detection must be a positive numeric literal."
                f" Instead got '{to_validate}'."
            )


def validate_averaging_and_grouping(config):
    """Validate the averaging and grouping settings in the config."""
    averaging_methods = {
        'unweighted_arithmetic',
        'unweighted_geometric',
        'weighted_arithmetic',
        'weighted_geometric'
    }

    v = Validator()
    v.schema = {
        'active': {'type': 'boolean'},
        'web_scraped': {
            'type': 'string',
            'allowed': averaging_methods,
        },
        'scanner': {
            'type': 'string',
            'allowed': averaging_methods,
        },
    }

    active = config.grouping['active']
    if not v.validate({'active': active}):
        raise ValueError(
            f"{config.name}: parameter 'active' in grouping"
            " must be a boolean."
            f" Instead got '{active}'."
        )

    # Averaging
    for data_source in ['web_scraped', 'scanner']:
        to_validate = config.averaging[data_source]
        if not v.validate({data_source: to_validate}):
            raise ValueError(
                f"{config.name}: parameter {data_source} in averaging"
                f" must be one of {averaging_methods}."
                f" Instead got '{to_validate}'."
            )

    # Grouping
    if active:
        for data_source in ['web_scraped', 'scanner']:
            to_validate = config.grouping[data_source]
            if not v.validate({data_source: to_validate}):
                raise ValueError(
                    f"{config.name}: {data_source} for grouping must"
                    " be one of {averaging_methods}."
                    f" Instead got '{to_validate}'."
                )


def validate_imputation(config):
    """Validate the imputation settings in the config."""
    v = Validator()
    v.schema = {
        'ffill_limit': {
            'type': 'integer',
            'min': 1,
        },
    }

    active = config.imputation['active']
    if not v.validate({'active': active}):
        raise ValueError(
            f"{config.name}: parameter 'active' in imputation"
            " must be a boolean."
            f" Instead got '{active}'."
        )

    if active:
        to_validate = config.imputation['ffill_limit']
        if not v.validate({'ffill_limit': to_validate}):
            raise ValueError(
                f"{config.name}: ffill_limit for imputation must"
                " be an integer greater than 0."
                f" Instead got '{to_validate}'."
            )


def validate_flag_low_expenditures(config):
    """Validate the flag_low_expenditures settings in the config."""
    v = Validator()
    v.schema = {
        'threshold': {
            'type': 'float',
            'min': 0,
            'max': 1,
        },
    }

    active = config.flag_low_expenditures['active']
    if not v.validate({'active': active}):
        raise ValueError(
            f"{config.name}: parameter 'active' in flag_low_expenditures"
            " must be a boolean."
            f" Instead got '{active}'."
        )

    if active:
        to_validate = config.flag_low_expenditures['threshold']
        if not v.validate({'threshold': to_validate}):
            raise ValueError(
                f"{config.name}: threshold in flag_low_expenditures"
                " must be a float between 0 and 1."
                f" Instead got '{to_validate}'."
            )


def validate_indices(config):
    """Validate the indices settings in the config."""
    base_price_methods = {
        'fixed_base',
        'chained',
        'bilateral',
        'fixed_base_with_rebase',
    }

    index_methods = {
        'carli',
        'jevons',
        'dutot',
        'laspeyres',
        'paasche',
        'fisher',
        'tornqvist',
    }

    multilateral_methods = {
        'ewgeks',
        'rygeks',
        'geks_movement_splice',
        'geks_window_splice',
        'geks_half_window_splice',
        'geks_december_link_splice',
        'geks_mean_splice',
    }

    v = Validator()
    v.schema = {
        'base_price_methods': {
            'type': 'list',
            'allowed': base_price_methods,
            'nullable': True,
        },
        'index_methods': {
            'type': 'list',
            'allowed': index_methods,
        },
        'multilateral_methods': {
            'type': 'list',
            'allowed': multilateral_methods,
            'nullable': True,
        },
        'base_period': {
            'type': 'integer',
            'min': 1,
            'max': 12,
        },
        'window': {
            'type': 'integer',
            'min': 3,
        },
    }

    to_validate = config.indices['base_price_methods']
    if not v.validate({'base_price_methods': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'base_price_methods' in indices"
            f" must be a list containing values among {base_price_methods}."
            f" Instead got '{to_validate}'."
        )

    to_validate = config.indices['index_methods']
    if not v.validate({'index_methods': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'index_methods' in indices"
            " must be a list containing values among {index_methods}."
            f" Instead got '{to_validate}'."
        )

    to_validate = config.indices['multilateral_methods']
    if not v.validate({'multilateral_methods': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'multilateral_methods' in indices"
            " must be a list containing values among {multilateral_methods}."
            f" Instead got '{to_validate}'."
        )

    to_validate = config.indices['window']
    if not v.validate({'window': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'window' in indices"
            " must be a positive integer > 2."
            f" Instead got '{to_validate}'."
        )

    to_validate = config.indices['base_period']
    if not v.validate({'base_period': to_validate}):
        raise ValueError(
            f"{config.name}: parameter 'base_period' in indices"
            " must be an integer representing a month between 1"
            " and 12 inclusive."
            f" Instead got '{to_validate}'."
        )

    if not (
        config.indices['base_price_methods']
        or config.indices['multilateral_methods']
    ):
        raise ValueError(
            "One of either 'base_price_methods' or 'multilateral_methods'"
            " must be provided. They can't both be None."
        )
