import pandas as pd 
import numpy as np 

from dfilabs.utils.utils_date import days



def group_missing_intervals(missing_minutes, max_gap=pd.Timedelta('1min')):
    if len(missing_minutes) == 0:
        return []

    missing_minutes = missing_minutes.sort_values()
    intervals = []
    start = missing_minutes[0]
    prev = missing_minutes[0]

    for t in missing_minutes[1:]:
        if t - prev > max_gap:
            intervals.append((start, prev))
            start = t
        prev = t

    intervals.append((start, prev))
    return intervals

def scan_errors(df):
    num_days = days(df)
    print(f"Data spans across {num_days} unique days.")
    # match the volumes
    volume_diff = abs(df.buy_volume + df.sell_volume - df.volume)
    if not (volume_diff < 1000).all():
        indexes = df[volume_diff > 100].index
        print("There are some values where buy and sell volumes don't match the total volume at the dates: ", indexes)
    # check missing data
    if df.isna().sum().sum() > 0:
        nan_columns = df.columns[df.isna().any()].tolist()
        print("There are NaN values in the columns: ", nan_columns)
        nan_indexes = df[df.isna().any(axis=1)].index
        print("There are NaN values at the dates: ", nan_indexes)

    # check if all minutes are present in the index
    start_time = df.index.min() 
    end_time = df.index.max()
    expected_index = pd.date_range(start=start_time, end=end_time, freq='min')
    missing_minutes = expected_index.difference(df.index)
    if missing_minutes.size > 0:
        print(f"Missing data detected — total: {len(missing_minutes)} minutes.")
        intervals = group_missing_intervals(missing_minutes)
        for start, end in intervals:
            print(f"  Missing interval: {start} → {end}")

    # check for duplicate timestamps
    if df.index.duplicated().any():
        duplicate_indexes = df.index[df.index.duplicated()].unique()
        print(f"Duplicate timestamps detected: {len(duplicate_indexes)} unique duplicates.")
        # Consider printing some examples if needed, e.g.:
        # print("Examples of duplicate timestamps:", duplicate_indexes[:5].tolist())

    # check if high>low
    if (df.low > df.high).sum() > 0:
        indexes = df[df["low"] > df["high"]].index
        print("There are some values where low is greater than high at the dates: ", indexes)

    # check if prices are positive
    features = list(df.columns)
    if (df[features]<0).sum().sum() > 0:
        indexes = df[df[features] < 0].index
        print("There are some negative prices/volumes at the dates: ", indexes)

