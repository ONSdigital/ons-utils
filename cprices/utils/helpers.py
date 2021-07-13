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


def fill_keys(
    d: Mapping[Tuple[Any], Any],
    repeat: bool = False
) -> Dict[Tuple[Any], Any]:
    """Fill tuple keys of a dict so they are all the same length.

    Parameters
    ----------
    repeat : bool, default False
        If True then fills upper key values with the current highest.
        If False fills with None.
    """
    max_depth = max(len(k) for k in d.keys())
    new_d = {}
    for k, v in d.items():
        fill_value = k[0] if repeat else None
        while len(k) != max_depth:
            k = (fill_value,) + k

        new_d.update({k: v})

    return new_d


def fill_tuples(tuples, repeat: bool = False):
    """Fill tuples so they are all the same length.

    Parameters
    ----------
    repeat : bool, default False
        If True then fills missing tuple values with the current highest.
        If False fills with None.
    """
    max_len = max(len(t) for t in tuples if isinstance(t, tuple))
    new_tups = []
    for tup in tuples:
        if not isinstance(tup, tuple):
            tup = (tup,)

        fill_value = tup[0] if repeat else None
        while len(tup) < max_len:
            tup = (fill_value,) + tup

        new_tups.append(tup)

    return new_tups
