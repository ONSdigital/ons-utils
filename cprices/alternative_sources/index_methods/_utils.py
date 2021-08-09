"""Generic utility functions."""
from collections import abc
from typing import Any, List, Optional, Tuple


def is_non_string_sequence(obj: Any) -> bool:
    """Return True if obj is non-string sequence like list or tuple."""
    return isinstance(obj, abc.Sequence) and not isinstance(obj, str)


def list_convert(obj: Optional[Any]) -> Optional[List[Any]]:
    """Convert given object to list unless obj is None.

    Converts non-string sequences to list. Won't convert sets. Wraps
    strings and non-sequences as a single item list. Returns None
    if None received as argument.
    """
    if obj is None:
        return obj

    return list(obj) if is_non_string_sequence(obj) else [obj]


def tuple_convert(obj: Any) -> Tuple[Any]:
    """Convert given object to tuple.

    Converts non-string sequences to tuple. Won't convert sets. Wraps
    strings and non-sequences as a single item tuple.
    """
    return tuple(obj) if is_non_string_sequence(obj) else (obj,)
