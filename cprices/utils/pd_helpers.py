""""""
import pandas as pd
from flatten_dict import flatten


def nested_dict_to_df(
    d: dict,
    columns: list = None,
    level_names: list = None,
) -> pd.DataFrame:
    """Flattens a nested dict and converts to a DataFrame with MultiIndex."""
    flat_d = flatten(d)
    df = pd.DataFrame.from_dict(flat_d, orient='index', columns=columns)
    
    # Level of nesting may vary, this standardises the length
    idx_filled = fill_tuple_nones(df.index)
    
    new_idx = pd.MultiIndex.from_tuples(idx_filled, names=level_names)
    return df.set_index(new_idx)


def fill_tuple_nones(tuples):
    """Given a list of tuples of varying length, fills tuples to the
    length of the longest tuple with None values.
    """
    max_tuple_length = max([len(x) for x in tuples])
    extra_nones_needed = lambda x, max_len: (None,) * (max_len - len(x))
    
    return [x + extra_nones_needed(x, max_tuple_length) for x in tuples]


class Stacker():
    """Provides methods to stack and unstack a tidy DataFrame."""
    def __init__(
        self,
        value_cols: list,
        index_cols: list,
        transpose: bool = False,
    ):
        """
        value_cols: those unaffected by the stacking operation
        index_cols: the cols to keep as the key axis
        transpose: whether to transpose the resulting dataframe
            default is the index is set as columns
        """
        self.value_cols = value_cols
        self.index_cols = index_cols
        self.transpose = transpose

    def unstack(self, df):
        """Sets all but value_cols as index, then unstacks index_cols."""
        # Save the column order for the stacking
        self.all_cols = df.columns

        set_cols = [col for col in df.columns if col not in self.value_cols]

        df = df.set_index(set_cols)
        unstacked_df = df.unstack(self.index_cols)

        if self.transpose:
            unstacked_df = unstack_df.T

        return unstacked_df

    def stack(self, df):
        """Stacks index_cols back to the index and resets index with
        same column order.
        """
        if self.transpose:
            df = df.T

        stacked_df = df.stack(self.index_cols)
        return stacked_df.reset_index()[self.all_cols]

      
def convert_level_to_datetime(df, level, axis=0):
    """Converts the given level of a MultiIndex to DateTime."""
    # Get a new list of levels with those defined by level converted
    # to datetime
    new_levels = [
        pd.to_datetime(df.axes[axis].levels[i]) if name == level
        else df.axes[axis].levels[i]
        for i, name in enumerate(df.axes[axis].names)
    ]
    
    # Create a new MultiIndex from the levels and set as axis
    new_idx = df.axes[axis].set_levels(new_levels)
    return df.set_axis(new_idx, axis=axis)


class MultiIndexSlicer:
    """Provides a method to return a MultIndex slice on the levels given
    in the instance.
    """
    def __init__(self, df, levels, axis=0):
        """
        levels: the MultiIndex levels to buidl a slice generator for
        axis: The axis for the MultiIndex to slice on
        """
        self.levels = levels
        self.df = df
        self.axis = axis
        
    def get_slicer(self, *args):
        """Returns a MultiIndex slice for the given args."""
        
        if len(args) != len(self.levels):
            return ValueError(
                f"len args must be same as len self.levels: {len(self.levels)}"
            )
        
        args = iter(args)
        
        return tuple([
          next(args) if name in self.levels
          else slice(None)
          for name in self.df.axes[self.axis].names
        ])
        
      
def get_index_level_values(df, levels, axis=0):
    """Returns each combination of level values for given levels."""
    return list(
        df.axes[axis].to_frame()[levels]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
