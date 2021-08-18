"""The cerberus schemas for config validation."""
from typing import Dict, Mapping, Sequence


def full_schema(sections: Sequence[str]) -> Dict[str, Dict]:
    """Return the full schema for the given sections."""
    schema = non_section_schema()
    schema.update({k: v for k, v in schema_sections() if k in sections})
    return schema


def schema_sections() -> Dict[str, Dict]:
    """Return a schema with all the sections."""
    return {
        'preprocessing': preprocessing_schema(),
        'outlier_detection': outlier_detection_schema(),
        'averaging': averaging_schema(),
        'grouping': grouping_schema(),
        'flag_low_expenditures': flag_low_expenditures_schema(),
        'indices': validate_indices(),
    }


def non_section_schema() -> Dict:
    """Return schema for config options not indented in a section."""
    return {
        'extra_strata': {
            'type': ['list', 'string'],
        }
    }


def preprocessing_schema() -> Dict:
    """Return schema for preprocessing validation."""
    return {
        'start_date': {
            'type': 'date',
            'regex': r'([12]\d{3}-(0[1-9]|1[0-2])-01)',
        },
        'end_date': {
            'type': 'date',
            'regex': r'([12]\d{3}-(0[1-9]|1[0-2])-01)',
        },
        'use_unit_prices': {'type': 'boolean'},
        'product_id_code_col': {
            'type': 'string',
            'allowed': {'gtin', 'productid_ons', 'sku'},
        },
        'calc_price_before_discount': {'type': 'boolean'},
        'promo_col': {
            'type': 'string',
            'allowed': {'price_promo_discount', 'multi_promo_discount'},
        },
        'sales_value_col': {
            'type': 'string',
            'allowed': {
                'sales_value_inc_discounts',
                'sales_value_exc_discounts',
                'sales_value_vat',
                'sales_value_vat_exc_discounts',
            },
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


def outlier_detection_schema() -> Dict:
    """Return schema for outlier detection validation."""
    return {
        'active': {'type': 'boolean'},
        'options': {
            'log_transform': {'type': 'boolean'},
            'outlier_methods': {
                'type': 'string',
                'allowed': {'tukey', 'kimber', 'ksigma'}
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
        },
    }


def grouping_schema() -> Dict:
    """Return schema for grouping validation."""
    pass


def averaging_schema() -> Dict:
    """Return schema for averaging validation."""
    return {
        'active': {'type': 'boolean'},
        'method': {
            'type': 'string',
            'allowed': {
                'unweighted_arithmetic',
                'unweighted_geometric',
                'weighted_arithmetic',
                'weighted_geometric'
            },
        },
    }


def flag_low_expenditures_schema() -> Dict:
    """Return schema for flag low expenditures validation."""
    return {
        'active': {'type': 'boolean'},
        'threshold': {
            'type': 'float',
            'min': 0,
            'max': 1,
        },
    }


def validate_indices() -> Mapping:
    """Return schema for indices validation."""
    return {
        'base_price_methods': {
            'type': 'list',
            'allowed': {
                'fixed_base',
                'chained',
                'bilateral',
                'fixed_base_with_rebase',
            },
            'nullable': True,
        },
        'index_methods': {
            'type': 'list',
            'allowed': {
                'carli',
                'jevons',
                'dutot',
                'laspeyres',
                'paasche',
                'fisher',
                'tornqvist',
                'geary-khamis',
            },
        },
        'multilateral_methods': {
            'type': 'list',
            'allowed': {
                'ewgeks',
                'rygeks',
                'geks_movement_splice',
                'geks_window_splice',
                'geks_half_window_splice',
                'geks_december_link_splice',
                'geks_mean_splice',
            },
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
