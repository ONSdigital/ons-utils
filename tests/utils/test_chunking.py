"""A set of unit tests for the chunking functions."""
import math

import pandas as pd
from pandas._testing import assert_frame_equal
import pytest

from cprices.utils import chunking
from tests.conftest import (create_dataframe)


@pytest.fixture
def df_to_chunk():
    """Return a df that can be chunked with a column to keep distinct."""
    return create_dataframe([
        ('index', 'class', 'animals'),
        (0, 'insect', 'bees'),
        (1, 'mammal', 'goats'),
        (2, 'reptile', 'lizards'),
        (3, 'mammal', 'yaks'),
        (4, 'reptile', 'turtles'),
        (5, 'fish', 'barbel'),
        (6, 'mammal', 'impalas'),
        (7, 'mammal', 'hedgehogs'),
        (8, 'bird', 'finches'),
        (9, 'reptile', 'bearded_dragons'),
        (10, 'reptile', 'adders'),
        (11, 'amphibians', 'toads'),
    ])


def test_get_chunks_df_divisible_by_chunksize(df_to_chunk):
    """Test get_chunks divides df into equal size chunks."""
    chunksize = 2
    chunks = chunking.get_chunks(df_to_chunk, chunksize)

    # Test the number of chunks is as expected.
    assert len(chunks) == len(df_to_chunk) / chunksize
    # Test length of chunks.
    assert all([len(chunk) == chunksize for chunk in chunks])

    # Tests concatenating chunks gets you back to original df.
    assert_frame_equal(pd.concat(chunks), df_to_chunk)


def test_get_chunks_df_not_divisible_by_chunksize(df_to_chunk):
    """Test get_chunks returns equal size chunks except the last.

    The df isn't perfectly divisible by the chunksize, so the length of
    the last chunk is the remainder of len(df) / chunksize.
    """
    chunksize = 5
    chunks = chunking.get_chunks(df_to_chunk, chunksize)

    # Test the number of chunks is as expected.
    assert len(chunks) == math.ceil(len(df_to_chunk) / chunksize)
    # Test length of chunks equals chunksize except last.
    assert all([len(chunk) == chunksize for chunk in chunks[:-1]])
    assert len(chunks[-1]) == len(df_to_chunk) % chunksize

    # Tests concatenating chunks gets you back to original df.
    assert_frame_equal(pd.concat(chunks), df_to_chunk)


def test_align_chunks_when_keep_distinct_value_in_both():
    """Test chunks align when a value in keep_distinct is in both chunks."""
    chunk = create_dataframe([
        ('index', 'class', 'animals'),
        (1, 'mammal', 'goats'),
        (3, 'mammal', 'yaks'),
        (6, 'mammal', 'impalas'),
    ])

    other = create_dataframe([
        ('index', 'class', 'animals'),
        (7, 'mammal', 'hedgehogs'),
        (2, 'reptile', 'lizards'),
        (4, 'reptile', 'turtles'),
    ])

    output1, output2 = chunking._align_chunks(
        chunk,
        other,
        keep_distinct='class',
    )

    assert_frame_equal(
        output1.reset_index(drop=True),
        create_dataframe([
            ('index', 'class', 'animals'),
            (1, 'mammal', 'goats'),
            (3, 'mammal', 'yaks'),
            (6, 'mammal', 'impalas'),
            (7, 'mammal', 'hedgehogs'),
        ]),
    )
    assert_frame_equal(
        output2.reset_index(drop=True),
        create_dataframe([
            ('index', 'class', 'animals'),
            (2, 'reptile', 'lizards'),
            (4, 'reptile', 'turtles'),
        ]),
    )


def test_align_chunks_input_equals_output_when_values_already_distinct():
    """
    Test input equals output when the values in 'class' for each chunk
    are already distinct.
    """
    chunk = create_dataframe([
        ('index', 'class', 'animals'),
        (8, 'bird', 'finches'),
        (5, 'fish', 'barbel'),
        (0, 'insect', 'bees'),
    ])

    other = create_dataframe([
        ('index', 'class', 'animals'),
        (1, 'mammal', 'goats'),
        (3, 'mammal', 'yaks'),
        (6, 'mammal', 'impalas'),
    ])

    output1, output2 = chunking._align_chunks(
        chunk,
        other,
        keep_distinct='class',
    )

    # Should be the same as input.
    assert_frame_equal(output1, chunk)
    assert_frame_equal(output2, other)


def test_chunk_up(df_to_chunk):
    """Test that chunk_up creates chunks with the values kept distinct."""
    # Chunks is an iterator.
    chunks = chunking.chunk_up(
        df_to_chunk,
        chunksize=3,
        keep_distinct='class',
    )

    assert_frame_equal(
        next(chunks).reset_index(drop=True),
        create_dataframe([
            ('index', 'class', 'animals'),
            (11, 'amphibians', 'toads'),
            (8, 'bird', 'finches'),
            (5, 'fish', 'barbel'),
        ]),
    )
    assert_frame_equal(
        next(chunks).reset_index(drop=True),
        create_dataframe([
            ('index', 'class', 'animals'),
            (0, 'insect', 'bees'),
            (1, 'mammal', 'goats'),
            (3, 'mammal', 'yaks'),
            (6, 'mammal', 'impalas'),
            (7, 'mammal', 'hedgehogs'),
        ]),
    )
    assert_frame_equal(
        next(chunks).reset_index(drop=True),
        create_dataframe([
            ('index', 'class', 'animals'),
            (2, 'reptile', 'lizards'),
            (4, 'reptile', 'turtles'),
            (9, 'reptile', 'bearded_dragons'),
            (10, 'reptile', 'adders'),
        ]),
    )
    # Check that the final one is empty, ignore dtypes.
    # It's empty because it contained only reptile rows, and the
    # previous chunk had a reptile row in it.
    assert_frame_equal(
        next(chunks),
        pd.DataFrame(columns=('index', 'class', 'animals')),
        check_dtype=False,
        check_index_type=False,
    )
