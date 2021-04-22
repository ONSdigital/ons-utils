"""Miscellaneous helper functions."""
from functools import reduce
from typing import Dict, Mapping, Iterator, Any, List

import pandas as pd

from pyspark.sql import (
    DataFrame as SparkDF,
    functions as F,
    SparkSession,
)


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


def union_dfs_from_all_scenarios(
    spark: SparkSession,
    dfs: Mapping[str, Mapping[str, SparkDF]]
) -> Dict[str, SparkDF]:
    """Combine dictionary of spark dataframes into one spark dataframe.

    Unions the corresponding dataframes from all scenarios so in the output
    dictionary, each stage has one dataframe. Before doing that, a scenario
    column is added to each dataframe to distinguish between scenarios.

    Any pandas dataframes are converted to spark also.

    Parameters
    ----------
    spark: spark session

    dfs : nested dictionary of spark dataframes
        Every key in the dfs dictionary holds the dataframes for the stages of
        the scenario run.

    Returns
    -------
    dfs : dictionary of spark dataframes
        Each key holds the unioned dataframe across all scenarios for a
        particular stage.
    """
    # ADD SCENARIO COLUMN TO ALL DATAFRAMES
    for scenario in dfs:
        # scenarios have names: scenario_x
        scenario_name = ''.join(scenario.split('_')[1:])

        for df_key, df in dfs[scenario].items():
            # if the dataframe is in pandas we need it in spark for unioning
            if isinstance(df, pd.DataFrame):
                df = pd_to_pyspark_df(spark, df)

            dfs[scenario][df_key] = (
                df
                .withColumn(
                    'scenario',
                    F.lit(scenario_name)
                )
            )

    # UNION DATAFRAMES (IF THERE ARE MORE THAN 1 SCENARIOS)
    if len(dfs) > 1:
        # Collate unique dataframe names within scenarios
        names = set(
            val for dfs_vals in dfs.values()
            for val in dfs_vals.keys()
        )

        dfs_unioned = {}
        for name in names:
            dfs_to_union = list(find(name, dfs))
            dfs_unioned[name] = reduce(SparkDF.unionByName, dfs_to_union)
            dfs_unioned[name].cache().count()

        dfs = dfs_unioned

    else:
        dfs = dfs[scenario]

    return dfs


def pd_to_pyspark_df(
    spark,
    df: pd.DataFrame,
    num_partitions: int = 1,
) -> SparkDF:
    """Convert pandas dataframe to spark with specified partitions."""
    return spark.createDataFrame(df).coalesce(num_partitions)


def map_column_names(df: SparkDF, mapper: Mapping[str, str]) -> SparkDF:
    """Map column names to the given values in the mapper.

    If the column name is not in the mapper the name doesn't change.
    """
    # def _(df):
    cols = [
        F.col(col_name).alias(mapper.get(col_name, col_name))
        for col_name in df.columns
    ]
    return df.select(*cols)
    # return _


def _list_convert(x: Any) -> List[Any]:
    """Return obj as a single item list if not already a list or tuple."""
    return [x] if not (isinstance(x, list) or isinstance(x, tuple)) else x
