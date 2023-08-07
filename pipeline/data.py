from typing import Any, List, Optional

import nfl_data_py as nfl
import numpy as np


KEY = "raw/nfl"


def get_nfl_data(year: int) -> List[List[Any]]:
    data = nfl.import_pbp_data([year])
    print(f"Season dataset shape:\t{data.shape}")
    return data


def replace_degree_symbol(val):
    if val is None:
        return val
    if isinstance(val, float):
        if np.isnan(val):
            return val
    return val.replace("\xb0", "")


def clean_data(df: List[List[Any]]) -> List[List[Any]]:
    """
    Drop columns we dont need
    Remove the degree symbol from the weather column
    \xb0 represents degree. For example:
        Rain and mid 70s Temp: 73° F, Humidity: 79%...
    """
    # Too sparse or messy to be worth casting types, etc
    drop_me = [
        'tackle_with_assist_1_player_id',
        'tackle_with_assist_1_player_name',
        'tackle_with_assist_1_team',
        'tackle_with_assist_2_player_id',
        'tackle_with_assist_2_player_name',
        'tackle_with_assist_2_team',
        "assist_tackle_3_player_id",
        "assist_tackle_3_player_name",
        "assist_tackle_3_team",
        "assist_tackle_4_player_id",
        "assist_tackle_4_player_name",
        "assist_tackle_4_team"
    ]
    df = df.drop(columns=drop_me)
    df["weather"] = df["weather"].apply(replace_degree_symbol)
    df['tackle_with_assist'] = df['tackle_with_assist'].astype(bool)
    return df


def get_filename(bucket, year: int, ext: Optional[str] = "csv"):
    return f"s3://{bucket}/{KEY}/pbp-{year}.{ext}"
