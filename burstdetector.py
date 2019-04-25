import numpy as np
import pandas as pd


def detect_bursts(timestamps, window, threshold):
    timestamps = timestamps.dropna().sort_values().reset_index(drop=True)
    bursts_start = timestamps.shift(threshold - 1)

    is_end_of_burst = pd.concat(((timestamps - bursts_start) < window,
                                 pd.Series([False] * (threshold - 1))))

    last_burst_index = ((is_end_of_burst * np.arange(len(is_end_of_burst)))
                        .replace(0, np.nan)
                        .fillna(method='pad'))

    since_last_burst = (pd.Series(np.arange(len(last_burst_index)))
                        - last_burst_index.reset_index(drop=True))
    in_burst = (since_last_burst.shift(-(threshold - 1))[:len(timestamps)]
                < threshold)

    # https://stackoverflow.com/questions/40802800
    new_burst = in_burst & (in_burst.shift() != True)
    burst_ids = new_burst[in_burst].cumsum()
    timestamp_groups = timestamps[in_burst].groupby(burst_ids)

    if not len(timestamp_groups):
        # Return empty dataframe with correct types
        return pd.DataFrame({
            'start': pd.Series([], dtype='datetime64[ns]'),
            'end': pd.Series([], dtype='datetime64[ns]'),
            'duration': pd.Series([], dtype=str),
            'number_of_events': pd.Series([], dtype=int),
        })

    retval = pd.DataFrame({
        'start': timestamp_groups.first(),
        'end': timestamp_groups.last(),
        'duration': (timestamp_groups.last() -
                     timestamp_groups.first()).astype(str),
        'number_of_events': timestamp_groups.size(),
    })
    retval.reset_index(drop=True, inplace=True)
    return retval


def render(table, params, *, input_columns):
    date_column_name = params['date_column_name']
    interval_length = params['interval_length']
    interval_unit = params['interval_unit']
    threshold = params['trigger_threshold']

    if not date_column_name:
        return table

    if input_columns[date_column_name].type != 'datetime':
        # TODO make JSON force column type. Depends on
        # https://www.pivotaltracker.com/story/show/161234499
        return 'Input column must be datetime. Please convert it.'

    timedelta_units = ['seconds', 'minutes', 'hours', 'days', 'weeks']
    timedelta_unit = timedelta_units[interval_unit]
    window = pd.Timedelta(**{timedelta_unit: interval_length})

    return detect_bursts(table[date_column_name], window, threshold)
