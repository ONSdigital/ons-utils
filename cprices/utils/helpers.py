"""Miscellaneous helper functions."""
import itertools
from typing import Mapping, Any, List, Tuple, Dict

from flatten_dict import flatten, unflatten


def _list_convert(x: Any) -> List[Any]:
    """Return obj as a single item list if not already a list or tuple."""
    return [x] if not (isinstance(x, list) or isinstance(x, tuple)) else x


def invert_nested_keys(d: Mapping[Any, Any]) -> Dict[Any, Any]:
    """Invert the order of the keys in a nested dict."""
    return unflatten({k[::-1]: v for k, v in flatten(d).items()})


def get_key_value_pairs(d: Mapping[Any, Any]) -> List[Tuple[Any, Any]]:
    """Get the key value pairs from a dictionary as a list of tuples.

    If the value is a non-string sequence, then a tuple pair is created
    for each object in the sequence.
    """
    # Get the pairs for each key
    pairs = {
        itertools.product(_list_convert(k), _list_convert(v))
        for k, v in d.items()
    }
    return list(itertools.chain.from_iterable(pairs))
