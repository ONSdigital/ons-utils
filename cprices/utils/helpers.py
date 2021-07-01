"""Miscellaneous helper functions."""
# Import Python libraries.
import logging
from typing import (
    Mapping,
    Iterator,
    Any,
    List,
)

# Import third party libraries.
from flatten_dict import flatten, unflatten
from humanfriendly import format_timespan


LOGGER = logging.getLogger()


def timer_args(name):
    """Initialise timer args as workaround for 'text' arg."""
    return {
        'name': name,
        'text': lambda secs: name + f": {format_timespan(secs)}",
        'logger': LOGGER.info,
    }


# This function is no longer used - can be kept if useful?
def find(key: str, dictionary: Mapping[str, Any]) -> Iterator[Any]:
    """
    Return all values in a nested dictionary that match key.

    Parameters
    ----------
    key : string
        The key to look for inside the dictionary.
    dictionary : python dictionary
        The dictionary to look for the user-specified key.

    Returns
    -------
    Python generator object - can be turned into a list.
    """
    for k, v in dictionary.items():
        if k == key:
            yield v
        elif isinstance(v, dict):
            for result in find(key, v):
                yield result
        elif isinstance(v, list):
            for d in v:
                for result in find(key, d):
                    yield result


def _list_convert(x: Any) -> List[Any]:
    """Return obj as a single item list if not already a list or tuple."""
    return [x] if not (isinstance(x, list) or isinstance(x, tuple)) else x


def invert_nested_keys(d: Mapping[Any, Any]) -> Mapping[Any, Any]:
    """Invert the order of the keys in a nested dict."""
    return unflatten({k[::-1]: v for k, v in flatten(d).items()})
