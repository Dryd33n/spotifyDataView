from typing import Type
import pandas as pd
import numpy as np
import doctest

TimeStamp = tuple[int,  # 1 Year in the format (YYY)
                  int,  # 2 Month of the year (MM)
                  int,  # 3 Day of the month (DD)
                  int,  # 4 Hour of the day in 24 hour (HH)
                  int]  # 5 Minute of the hour (MM)

SongData = tuple[TimeStamp,  # 1 Timestamp of song listen
                 str,        # 2 Platform listened on
                 int,        # 3 Milliseconds played
                 str,        # 4 Country played in
                 str,        # 5 Track Name
                 str,        # 6 Track Album
                 str,        # 7 Track Artist
                 str,        # 8 Track URI
                 str,        # 9 Reason for song start
                 str,        # 10 Reason for song end
                 bool,       # 11 Shuffle
                 bool,       # 12 Skipped
                 bool]       # 13 Incognito Mode


def import_streaming_history(filename: str) -> None:
    data = pd.read_json('data/' + filename)
    formatted_data = pd.DataFrame()

    for row in data.itertuples():
        row_data = pd.Series()

        row_data.add(process_time_stamp(row[1]))  # TIMESTAMP
        row_data.add(str(row[3]))                 # PLATFORM
        row_data.add(int(row[4]))                 # MS_PLAYED
        row_data.add(str(row[5]))                 # COUNTRY
        row_data.add(str(row[8]))                 # TRACK_NAME
        row_data.add(str(row[10]))                # TRACK_ALBUM
        row_data.add(str(row[9]))                 # TRACK_ARTIST
        row_data.add(str(row[11]))                # TRACK_URI
        row_data.add(str(row[15]))                # REASON_START
        row_data.add(str(row[16]))                # REASON_END
        row_data.add(bool(row[17]))               # SHUFFLE
        row_data.add(bool(row[18]))               # SKIPPED
        row_data.add(bool(row[21]))               # INCOGNITO_MODE

        formatted_data = formatted_data.append()

    print(formatted_data)


def process_time_stamp(time_string: str) -> Type[tuple]:
    """

    Examples:
    >>> process_time_stamp('2022-11-30T02:37:55Z')
    (2022, 11, 30, 2, 37)

    :param time_string:
    :return:
    """
    time_string = time_string[:-1]
    time_string = time_string.replace('T', '-')
    time_string = time_string.replace(':', '-')
    time_string = time_string.split('-')

    year = int(time_string[0])
    month = int(time_string[1])
    day = int(time_string[2])
    hour = int(time_string[3])
    minute = int(time_string[4])

    return tuple[year, month, day, hour, minute]
