"""Contains methods:
- find USED IN MAIN.PY

"""

def find(key, dictionary):
    """
    Returns all values in a dictionary (with nested dictionaries) that have 
    to the user-specified key.

    Parameters
    ----------
    key : string
        The key to look for inside the dictionary.

    dictionary : python dictionary
        The dictionary to look for the user-specified key.

    Returns
    -------
    Python generator object - cane be turned into a list.
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

