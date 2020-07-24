"""Contains methods:
- find
    used in union_dfs_from_all_scenarios
- union_dfs_from_all_scenarios
    used in main.py
"""
# import pyspark libraries
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# import python libraries
from functools import reduce


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


def union_dfs_from_all_scenarios(dfs):
    """
    Unions the corresponding dataframes from all scenarios so in the output
    dictionary, each stage has one dataframe. Before doing that, a scenario
    column is added to each dataframe to distinguish between scenarios.

    Parameters
    ----------
    dfs : nested dictionary of spark dataframes
        Every key in the dfs dictionary holds the dataframes for the stages of
        the scenario run.

    scenario : string
        The name of the scenario, i.e. scenario_1, scenario_2, etc

    Returns
    -------
    dfs : dictionary of spark dataframes
        Each key holds the unioned dataframe across all scenarios for a
        particular stage.
    """
    # ADD SCENARIO COLUMN TO ALL DATAFRAMES

    for scenario in dfs:
        # scenarios have names: scenario_x, extract number
        scenario_no = scenario.split('_')[1]
        for df in dfs[scenario]:
            dfs[scenario][df] = (
                dfs[scenario][df]
                .withColumn(
                    'scenario',
                    F.lit(scenario_no)
                )
            )

    # UNION DATAFRAMES (IF THERE ARE MORE THAN 1 SCENARIOS)

    # name of first scenario
    scenario = list(dfs.keys())[0]

    if len(dfs) > 1:
        # names of dataframes - they are the same for each scenario
        # they can be collected from any scenario in dfs
        names = dfs[scenario].keys()

        dfs_unioned = {}
        for name in names:
            dfs_to_union = list(find(name, dfs))
            dfs_unioned[name] = reduce(DataFrame.unionByName, dfs_to_union)
            dfs_unioned[name].cache().count()

        dfs = dfs_unioned

    else:
        dfs = dfs[scenario]

    return dfs
