from typing import List, Any

import nfl_data_py as nfl
import numpy as np


KEY = "raw/nfl"


def get_nfl_data(year: int) -> List[List[Any]]:
     data = nfl.import_pbp_data([year])
     # data = pl.from_pandas(nfl.import_pbp_data([year]))
     print(f"Season dataset shape:\t{data.shape}")
     return data


def replace_degree_symbol(val):
    if val is None:
        return val
    if isinstance(val, float):
        if np.isnan(val):
            return val
    return val.replace('\xb0', '')


def clean_data(df: "pandas.DataFrame") -> List[List[Any]]:
    """
    Remove the degree symbol from the weather column
    \xb0 represents degree. For example:
        Rain and mid 70s Temp: 73° F, Humidity: 79%...
    """
    df["weather"] = df["weather"].apply(replace_degree_symbol)
    return df


def get_filename(bucket, year: int):
    return f"s3://{bucket}/{KEY}/pbp-{year}.csv"