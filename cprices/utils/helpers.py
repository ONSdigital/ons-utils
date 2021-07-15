"""Miscellaneous helper functions."""
from collections import abc
import itertools
from typing import Mapping, Any, List, Tuple, Dict, Sequence

from flatten_dict import flatten, unflatten


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
        itertools.product(list_convert(k), list_convert(v))
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


def fill_tuples(
    tuples: Sequence[Any],
    repeat: bool = False,
    fill_method: str = 'bfill',
) -> Sequence[Tuple]:
    """Fill tuples so they are all the same length.

    Parameters
    ----------
    repeat : bool, default False
        If True then fills missing tuple values with the current value
        at the end of the sequence given by ``at``. If False fills with None.
    fill_method : {'bfill', 'ffill'}, str
        Whether to forward fill or backfill the tuple values.
    """
    max_len = max(len(t) for t in tuples if isinstance(t, tuple))
    new_tups = []
    for tup in tuples:
        tup = tuple_convert(tup)

        while len(tup) < max_len:
            if fill_method == 'bfill':
                tup = (tup[0] if repeat else None,) + tup
            else:   # 'end'
                tup += (tup[-1] if repeat else None,)

        new_tups.append(tup)

    return new_tups


def tuple_convert(obj: Any) -> Tuple[Any]:
    """Convert given object to tuple.

    Converts non-string sequences to tuple. Won't convert sets. Wraps
    strings and non-sequences as a single item tuple.
    """
    if isinstance(obj, abc.Sequence) and not isinstance(obj, str):
        return tuple(obj)
    else:
        return (obj,)


def list_convert(obj: Any) -> List[Any]:
    """Convert given object to tuple.

    Converts non-string sequences to list. Won't convert sets. Wraps
    strings and non-sequences as a single item list.
    """
    if isinstance(obj, abc.Sequence) and not isinstance(obj, str):
        return list(obj)
    else:
        return [obj]
