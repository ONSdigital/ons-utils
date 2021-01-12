"""Functions to chunk pandas DataFrames."""
from typing import List, Tuple, Iterator

import pandas as pd
from pandas._typing import Label


def chunk_up(
    df: pd.DataFrame,
    chunksize: int,
    keep_distinct: Label,
) -> Iterator[pd.DataFrame]:
    """
    Iterate through each chunk while constraining unique values.

    Parameters
    ----------
    df : DataFrame
        The dataframe to chunk up.
    chunksize : int
        The max size for the dataframe chunks.
    keep_distinct : str or column label
        The column label used to constrain unique values across chunks.

    Returns
    -------
    iterator of dataframes
        The chunk with constrained unique values.

    """
    # The df needs to be sorted before aligning.
    df_sorted = df.sort_values(keep_distinct)
    chunks = get_chunks(df_sorted, chunksize)

    # Delete to attempt to free their memory.
    del df, df_sorted

    first_chunk = chunks.pop(0)
    for chunk in chunks:

        chunk1, chunk2 = _align_chunks(first_chunk, chunk, keep_distinct)
        yield chunk1

        # chunk2 goes into the next loop as first_chunk and is then
        # yielded as chunk1 after the next call of align_chunks.
        first_chunk = chunk2

    # Final chunk2 is yielded.
    yield chunk2


def get_chunks(df: pd.DataFrame, chunksize: int) -> List[pd.DataFrame]:
    """Slice a DataFrame into chunks of a given size."""
    lower_bounds = list(range(0, len(df), chunksize))
    upper_bounds = lower_bounds + chunksize

    chunks = zip(lower_bounds, upper_bounds)

    return [df.iloc[slice(*chunk), :] for chunk in chunks]


def _align_chunks(
    chunk: pd.DataFrame,
    other: pd.DataFrame,
    keep_distinct: Label,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Constrains unique value sets for keep_distinct col across chunks.

    Parameters
    ----------
    chunk : DataFrame
        First chunk to align.
    other : DataFrame
        Other chunk to align.
    keep_distinct : str or column label
        The column label used to constrain unique values across chunks.

    Returns
    -------
    tuple of DataFrame
        The aligned chunks.

    """
    last_val = chunk[keep_distinct].iloc[-1]
    is_last_val = f'{keep_distinct} == @last_val'

    chunk = pd.concat([chunk, other.query(is_last_val)])
    other = other.query(f'not {is_last_val}')

    return chunk, other
