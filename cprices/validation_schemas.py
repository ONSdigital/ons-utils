"""The cerberus schemas for config validation.

Provides:

* :func:`full_schema` - Returns the schema for the given sections.
"""
from typing import Dict, Mapping, Sequence


def full_schema(sections: Sequence[str]) -> Dict[str, Dict]:
    """Return the full schema for the given sections."""
    schema = non_section_schema()
    schema.update(consumption_segment_mappers_schema())
    schema.update({
        k: v for k, v in schema_sections().items()
        if k in sections
    })
    return schema


def schema_sections() -> Dict[str, Dict]:
    """Return a schema with all the sections."""
    section_schemas = {
        'preprocessing': preprocessing_schema(),
        'outlier_detection': outlier_detection_schema(),
        'averaging': averaging_schema(),
        'grouping': grouping_schema(),
        'flag_low_expenditures': flag_low_expenditures_schema(),
        'indices': indices_schema(),
    }
    # For nested schema, needs 'type' and 'schema'.
    return {
        section: {
            'type': 'dict',
            'required': True,
            'schema': schema,
        }
        for section, schema in section_schemas.items()
    }


def non_section_schema() -> Dict:
    """Return schema for config options not indented in a section."""
    return {
        'extra_strata': {
            'type': ['list', 'string'],
            'nullable': True,
        },
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
        'remove_discounts': {'type': 'boolean'},
        'discount_col': {
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
                'sales_value_inc_discriminatory_discounts',
            },
        },
        'align_daily_frequency': {
            'type': 'string',
            'allowed': {'weekly', 'monthly'},
        },
        'week_selection': {
            'type': 'list',
            'allowed': {1, 2, 3, 4},
            'nullable': True,
        },
    }


def consumption_segment_mappers_schema() -> Dict:
    """Return schema for consumption segment mappers validation."""
    return {
        'consumption_segment_mappers': {
            'type': 'dict',
            'keyschema': {'type': ['string', 'list']},
            'valueschema': {'type': 'string'},
        },
    }


def outlier_detection_schema() -> Dict:
    """Return schema for outlier detection validation."""
    return {
        'active': {'type': 'boolean'},
        'options': {
            'type': 'dict',
            'schema': {
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
                    'allowed': {'population', 'sample'},
                },
                'quartile_method': {
                    'type': 'string',
                    'allowed': {'exact', 'approx'},
                },
                'accuracy': {
                    'type': 'float',
                    'min': 1,
                },
            },
        },
    }


def grouping_schema() -> Dict:
    """Return schema for grouping validation."""
    return {
        'active': {'type': 'boolean'},
        'post_grouping_averaging_method': {
            # There are no weights in web_scraped yet.
            'allowed': {
                'unweighted_arithmetic',
                'unweighted_geometric',
            }
        },
        'mappers': {
            'type': 'dict',
            'keyschema': {'type': 'string'},
            'valueschema': {
                'type': 'dict',
                'keyschema': {'type': 'string'},
                'valueschema': {'type': 'string'},
            },
        },
    }


def averaging_schema() -> Dict:
    """Return schema for averaging validation."""
    return {
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


def indices_schema() -> Mapping:
    """Return schema for indices validation."""
    bilateral_index_methods = {
        'carli',
        'jevons',
        'dutot',
        'laspeyres',
        'paasche',
        'fisher',
        'tornqvist',
    }

    multilateral_method_options = {
        'initial_window_methods': {
            'type': 'list',
            'allowed': {
                'revised',
                'expanding'
            }
        },
        'extension_methods': {
            'type': 'list',
            'allowed': {
                'pure',
                'expanding_window',
                'movement_splice',
                'window_splice',
                'half_window_splice',
                'december_link_splice',
                'mean_splice',
            }
        },
        'window': {
            'type': 'integer',
            'min': 3,
        }
    }

    return {
        'bilateral_index_options': {
            'type': 'dict',
            'schema': {
                'index_methods': {
                    'type': 'list',
                    'allowed': bilateral_index_methods
                },
                'index_types': {
                    'type': 'list',
                    'allowed': {
                        'fixed_base',
                        'chained',
                        'fixed_base_with_rebase'
                    },
                },
                'base_period': {
                    'type': 'integer',
                    'min': 1,
                    'max': 12,
                },
            }
        },
        'multilateral_method_options': {
            'type': 'dict',
            'schema': {
                'geks': {
                    'type': 'dict',
                    'schema': {
                        'index_method_pairings': {
                            'type': 'list',
                            'allowed': bilateral_index_methods
                        },
                        **multilateral_method_options
                    }
                },
                'geary_khamis': {
                    'type': 'dict',
                    'schema': {
                        'window': {
                            'type': 'integer',
                            'min': 3,
                        }
                    }
                }
            }
        }
    }
